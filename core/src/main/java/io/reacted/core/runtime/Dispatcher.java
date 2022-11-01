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
import org.agrona.concurrent.BackoffIdleStrategy;
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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private static final String UNCAUGHT_EXCEPTION_IN_DISPATCHER = "Uncaught exception in thread [%s] : ";
    private static final String REACTIONS_EXECUTION_ERROR = "Error for ReActor {} processing " +
                                                            "message type {} with seq num {} and value {} ";
    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);
    private final DispatcherConfig dispatcherConfig;
    @Nullable
    private ExecutorService dispatcherLifeCyclePool;
    @Nullable
    private ExecutorService[] dispatcherPool;
    private final RingBuffer[] scheduledQueues;
    private final AtomicLong nextDispatchIdx = new AtomicLong(0);
    private final ReActorSystem reActorSystem;

    public Dispatcher(DispatcherConfig config, ReActorSystem reActorSystem) {
        this.reActorSystem = reActorSystem;
        int ringBufferSize = RingBufferDescriptor.TRAILER_LENGTH +
                             BitUtil.findNextPositivePowerOfTwo(
                                 ReActorId.NO_REACTOR_ID.getRawIdSize() * reActorSystem.getSystemConfig()
                                                                                       .getMaximumReActorsNum());
        this.dispatcherConfig = config;

        this.scheduledQueues =  Stream.iterate(new ManyToOneRingBuffer(new UnsafeBuffer(
                                                   ByteBuffer.allocateDirect(ringBufferSize))),
                                              scheduleQueue -> new ManyToOneRingBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(ringBufferSize))))
                                     .limit(getDispatcherConfig().getDispatcherThreadsNum())
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
        var lifecyclePoolSize = Integer.max(2, getDispatcherConfig().getDispatcherThreadsNum() >> 2);

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
            var rb = scheduledQueues[(int) (nextDispatchIdx.getAndIncrement() % scheduledQueues.length)];
            return rb.write(1,reActor.getSchedulationIdBuffer(),0,Long.BYTES);
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
        var processed = 0L;
        IdleStrategy ringBufferConsumerPauser = new BackoffIdleStrategy();

        MessageHandler ringBufferMessageProcessor = ((msgTypeId, buffer, index, length) ->
            onMessage(buffer, index, dispatcherBatchSize, dispatcherLifeCyclePool,
                      isExecutionRecorded, reActorSystem, devNull, reActorUnregister));
        var processedInRound = 0;
        while (!Thread.currentThread().isInterrupted()) {
            processedInRound = scheduledList.read(ringBufferMessageProcessor);
            ringBufferConsumerPauser.idle(processedInRound);
            processed += processedInRound;
        }
        LOGGER.debug("Dispatcher Thread {} is terminating. Processed: {}", Thread.currentThread().getName(), processed);
    }
    public void onMessage(MutableDirectBuffer buffer, int index, int dispatcherBatchSize,
                          ExecutorService dispatcherLifeCyclePool,
                          boolean isExecutionRecorded, ReActorSystem reActorSystem, ReActorRef devNull,
                          Function<ReActorContext, Optional<CompletionStage<Void>>> reActorUnregister) {
        ReActorContext scheduledReActor = reActorSystem.getReActorCtx(buffer.getLong(index, ByteOrder.BIG_ENDIAN));

        if (scheduledReActor == null) {
            return;
        }
        //memory acquire
        scheduledReActor.acquireCoherence();

        for (var msgNum = 0; msgNum < dispatcherBatchSize &&
                             !scheduledReActor.getMbox().isEmpty() &&
                             !scheduledReActor.isStop(); msgNum++) {
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
                devNull.route(scheduledReActor.getSelf(),
                              new EventExecutionAttempt(scheduledReActor.getSelf().getReActorId(),
                                                        scheduledReActor.getNextMsgExecutionId(),
                                                        newEvent.getSequenceNumber())).isNotSent()) {
                LOGGER.error("CRITIC! Unable to send an Execution Attempt for message {} Replay will NOT be possible",
                             newEvent);
            }

            executeReactionForMessage(scheduledReActor, newEvent);
        }
        //memory release
        scheduledReActor.releaseCoherence();
        //now this reactor can be scheduled by some other thread if required
        scheduledReActor.releaseScheduling();
        if (scheduledReActor.isStop()) {
            dispatcherLifeCyclePool.submit(() -> reActorUnregister.apply(scheduledReActor));
        } else if (!scheduledReActor.getMbox().isEmpty()) {
            //If there are other messages to be processed, request another schedulation fo the dispatcher
             dispatch(scheduledReActor);
        }
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
