/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.EventExecutionAttempt;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ControlledMessageHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

@NonNullByDefault
public class Dispatcher {
    public static final Dispatcher NULL_DISPATCHER = new Dispatcher(DispatcherConfig.NULL_DISPATCHER_CFG,
                                                                    ReActorSystem.NO_REACTOR_SYSTEM);
    /* Default dispatcher. Used by system internals */
    public static final String DEFAULT_DISPATCHER_NAME = "ReactorSystemDispatcher";
    public static final int DEFAULT_DISPATCHER_BATCH_SIZE = 10;
    public static final int DEFAULT_DISPATCHER_THREAD_NUM = 2;
    private static final int MESSAGE_MSG_TYPE = 1;
    private static final String UNCAUGHT_EXCEPTION_IN_DISPATCHER = "Uncaught exception in thread [%s] : ";
    private static final String REACTIONS_EXECUTION_ERROR = "Error for ReActor {} processing " +
                                                            "message type {} with seq num {} and value {} ";
    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);
    private final DispatcherConfig dispatcherConfig;
    private final BlockingQueue<ReActorContext> slowpathQueue = new LinkedBlockingQueue<>();
    @Nullable
    private ExecutorService dispatcherLifeCyclePool;
    @Nullable
    private ExecutorService[] dispatcherPool;
    private final RingBuffer[] scheduledQueues;
    private final AtomicLong nextDispatchIdx = new AtomicLong(0);
    private final ReActorSystem reActorSystem;

    public Dispatcher(DispatcherConfig config, ReActorSystem reActorSystem) {
        this.reActorSystem = reActorSystem;
        int ringBufferSize = RingBufferDescriptor.TRAILER_LENGTH + BitUtil.findNextPositivePowerOfTwo(Long.BYTES * reActorSystem.getSystemConfig().getMaximumReActorsNum());

        this.dispatcherConfig = config;

        this.scheduledQueues =  Stream.iterate(new ManyToOneRingBuffer(new UnsafeBuffer(
                                                   ByteBuffer.allocateDirect(ringBufferSize))),
                                              scheduleQueue -> new ManyToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(ringBufferSize))))
                                     .limit(BitUtil.findNextPositivePowerOfTwo(getDispatcherConfig().getDispatcherThreadsNum()))
                                     .toArray(ManyToOneRingBuffer[]::new);

    }

    public String getName() { return dispatcherConfig.getDispatcherName(); }

    public void initDispatcher(ReActorRef devNull, boolean isExecutionRecorded,
                               Function<ReActorContext, Optional<CompletionStage<Void>>> reActorUnregister) {
        ThreadFactory dispatcherFactory = new ThreadFactoryBuilder()
                .setNameFormat("ReActed-Dispatcher-Thread-" + getName() + "-%d")
                .setUncaughtExceptionHandler((thread, error) -> LOGGER.error(String.format(UNCAUGHT_EXCEPTION_IN_DISPATCHER,
                                                                                           thread.getName()), error))
                .build();
        ThreadFactory lifecycleFactory = new ThreadFactoryBuilder()
                .setNameFormat("ReActed-Dispatcher-LifeCycle-Thread-" + getName() + "-%d")
                .setUncaughtExceptionHandler((thread, error) -> LOGGER.error(String.format(UNCAUGHT_EXCEPTION_IN_DISPATCHER,
                                                                                           thread.getName()), error))
                .build();
        this.dispatcherPool = Stream.iterate(Executors.newFixedThreadPool(1,dispatcherFactory),
                                             executorService -> Executors.newFixedThreadPool(1, dispatcherFactory))
                                    .limit(getDispatcherConfig().getDispatcherThreadsNum())
                                    .toArray(ExecutorService[]::new);
        var lifecyclePoolSize =  Integer.max(2, getDispatcherConfig().getDispatcherThreadsNum() >> 2);

        this.dispatcherLifeCyclePool = Executors.newFixedThreadPool(lifecyclePoolSize, lifecycleFactory);

        for(var currentDispatcherThread = 0;
            currentDispatcherThread < getDispatcherConfig().getDispatcherThreadsNum(); currentDispatcherThread++) {

            ExecutorService dispatcherThread = dispatcherPool[currentDispatcherThread];
            RingBuffer threadLocalSchedulingQueue = scheduledQueues[currentDispatcherThread];
            dispatcherThread.submit(() -> Try.ofRunnable(() -> dispatcherLoop(threadLocalSchedulingQueue,
                                                                              dispatcherConfig.getBatchSize(),
                                                                              dispatcherLifeCyclePool,
                                                                              isExecutionRecorded,
                                                                              reActorSystem, devNull,
                                                                              reActorUnregister))
                                             .ifError(error -> LOGGER.error("Error running dispatcher: ", error)));
        }
    }

    public void stopDispatcher() {
        Arrays.stream(Objects.requireNonNull(dispatcherPool)).forEachOrdered(ExecutorService::shutdownNow);
        getDispatcherLifeCyclePool().shutdown();
    }

    public DispatcherConfig getDispatcherConfig() {
        return dispatcherConfig;
    }

    public boolean dispatch(ReActorContext reActor) {
        if (reActor.acquireScheduling()) {
            int failures = 0;
            int claimIdx = -1;
            RingBuffer selectedRing = null;
            while(claimIdx < 1) {
                selectedRing = scheduledQueues[(int) (nextDispatchIdx.getAndIncrement() & (scheduledQueues.length - 1))];
                claimIdx = selectedRing.tryClaim(MESSAGE_MSG_TYPE, Long.BYTES);
                if (claimIdx < 1 && (getDispatcherConfig().getDispatcherThreadsNum() < 2 || failures++ > 100)) {
                    if (!slowpathQueue.offer(reActor)) {
                        LOGGER.error("CRITIC! Unable to activate slowpath mode for {} . Reactor may be stale!", reActor.getSelf()
                                                                                                                      .getReActorId());
                        reActor.releaseScheduling();
                        return false;
                    } else {
                        return true;
                    }
                }
            }
            var atomicBuffer = selectedRing.buffer();
            atomicBuffer.putLong(claimIdx, reActor.getReActorSchedulationId());
            selectedRing.commit(claimIdx);
        }
        return true;
    }
    private ExecutorService getDispatcherLifeCyclePool() {
        return Objects.requireNonNull(dispatcherLifeCyclePool);
    }

    private void dispatcherLoop(RingBuffer scheduledList, int dispatcherBatchSize,
                                ExecutorService dispatcherLifeCyclePool, boolean isExecutionRecorded,
                                ReActorSystem reActorSystem, ReActorRef devNull,
                                Function<ReActorContext, Optional<CompletionStage<Void>>> reActorUnregister) {
        var processedForDispatcher = new AtomicLong(0);
        EventExecutionAttempt recyledMessage = new EventExecutionAttempt();
        IdleStrategy ringBufferConsumerPauser = new BackoffIdleStrategy(100_000_000L,
                                                                        100L,
                                                                        BackoffIdleStrategy.DEFAULT_MIN_PARK_PERIOD_NS,
                                                                        BackoffIdleStrategy.DEFAULT_MAX_PARK_PERIOD_NS);

        MessageHandler ringBufferMessageProcessor = ((msgTypeId, buffer, index, length) -> {
            if (msgTypeId == MESSAGE_MSG_TYPE) {
                ReActorContext ctx = reActorSystem.getReActorCtx(buffer.getLong(index));
                if (ctx != null) {
                    processedForDispatcher.setPlain(processedForDispatcher.getPlain() +
                                                    onMessage(ctx, dispatcherBatchSize, dispatcherLifeCyclePool,
                                                              isExecutionRecorded, devNull, reActorUnregister,
                                                              recyledMessage));
                }
            }
        });
        while (!Thread.currentThread().isInterrupted()) {
            int ringRecordsProcessed = scheduledList.read(ringBufferMessageProcessor);
            if (!slowpathQueue.isEmpty()) {
                LOGGER.warn("Slowpath mode has been enabled with size " + slowpathQueue.size());
            }
            while (!slowpathQueue.isEmpty()) {
                ReActorContext slowPathActor = slowpathQueue.poll();
                if (!slowPathActor.releaseScheduling() || !dispatch(slowPathActor)) {
                    LOGGER.error("CRITIC! Slow path actor not scheduled!? {}", slowPathActor.getSelf().getReActorId());
                }
            }
            ringBufferConsumerPauser.idle(ringRecordsProcessed);
        }
        LOGGER.info("Dispatcher Thread {} is terminating. Processed: {}", Thread.currentThread().getName(),
                    processedForDispatcher);
    }
    public int onMessage(ReActorContext scheduledReActor, int dispatcherBatchSize,
                         ExecutorService dispatcherLifeCyclePool, boolean isExecutionRecorded, ReActorRef devNull,
                         Function<ReActorContext, Optional<CompletionStage<Void>>> reActorUnregister,
                         EventExecutionAttempt recyledMessage) {
        //memory acquire
        scheduledReActor.acquireCoherence();
        int processed = 0;
        for (; processed < dispatcherBatchSize &&
                             !scheduledReActor.getMbox().isEmpty() &&
                             !scheduledReActor.isStop(); processed++) {
            var newEvent = scheduledReActor.getMbox().getNextMessage();

            /*
              Register the execution attempt within the local driver log. In this way regardless of the
              tell order of the messages, we will always have a strictly ordered execution order per
              reactor because a reactor is scheduled on exactly one dispatcher when it has messages to
              process

              --- NOTE ----:

              This is the core of the cold replay engine: ReActed can replicate the state of a single
              reactor replicating the same very messages in the same order of when they were executed
              during the recorded execution. From ReActed perspective, replicating the state of a
              reactor system is replicating the state of the contained reactors using the strictly
              sequential execution attempts in the execution log
            */
            if (isExecutionRecorded &&
                devNull.tell(scheduledReActor.getSelf(),
                             recyledMessage.errorIfInvalid()
                                           .setReActorId(scheduledReActor.getSelf().getReActorId())
                                           .setMessageSeqNum(newEvent.getSequenceNumber())
                                           .setExecutionSeqNum(scheduledReActor.getNextMsgExecutionId())).isNotSent()) {
                LOGGER.error("CRITIC! Unable to send an Execution Attempt for message {} Replay will NOT be possible",
                             newEvent);
            }

            executeReactionForMessage(scheduledReActor, newEvent);
        }
        //memory release
        scheduledReActor.releaseCoherence();
        //now this reactor can be scheduled by some other thread if required
        if (!scheduledReActor.releaseScheduling()) {
            LOGGER.error("CRITIC! Failed to release scheduling!?");
        }
        if (scheduledReActor.isStop()) {
            dispatcherLifeCyclePool.execute(() -> reActorUnregister.apply(scheduledReActor));
        } else if (!scheduledReActor.getMbox().isEmpty()) {
            //If there are other messages to be processed, request another schedulation fo the dispatcher
            dispatch(scheduledReActor);
        }
        return processed;
    }

    private void executeReactionForMessage(ReActorContext scheduledReActor, Message newEvent) {
        try {
            scheduledReActor.reAct(newEvent);
        } catch (Exception anyExc) {
            scheduledReActor.logError(REACTIONS_EXECUTION_ERROR,
                                      scheduledReActor.getSelf().getReActorId(),
                                      newEvent.getPayload().getClass(),
                                      newEvent.getSequenceNumber(), newEvent.toString(), anyExc);
            scheduledReActor.stop();
        }
    }
}
