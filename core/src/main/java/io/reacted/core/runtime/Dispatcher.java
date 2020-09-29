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
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@NonNullByDefault
public class Dispatcher {
    private static final String UNCAUGHT_EXCEPTION_IN_DISPATCHER = "Uncaught exception in thread [%s] : ";
    private static final String REACTIONS_EXECUTION_ERROR = "Error for ReActor {} processing " +
                                                            "message type {} with seq num {} and value {} ";
    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);
    private final DispatcherConfig dispatcherConfig;
    @Nullable
    private ExecutorService dispatcherLifeCyclePool;
    @Nullable
    private ExecutorService[] dispatcherPool;
    private final BlockingDeque<ReActorContext>[] scheduledQueues;
    private final AtomicLong nextDispatchIdx = new AtomicLong(0);

    @SuppressWarnings("unchecked")
    public Dispatcher(DispatcherConfig config) {
        this.dispatcherConfig = config;
        this.scheduledQueues = Stream.iterate(new LinkedBlockingDeque<ReActorContext>(),
                                              scheduleQueue -> new LinkedBlockingDeque<>())
                                     .limit(getDispatcherConfig().getDispatcherThreadsNum())
                                     .toArray(LinkedBlockingDeque[]::new);
    }

    public String getName() { return dispatcherConfig.getDispatcherName(); }

    public void initDispatcher(ReActorRef devNull, boolean isExecutionRecorded) {
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
        int lifecyclePoolSize = Integer.max(1, getDispatcherConfig().getDispatcherThreadsNum() >> 2);

        this.dispatcherLifeCyclePool = Executors.newFixedThreadPool(lifecyclePoolSize,lifecycleFactory);

        for(int currentDispatcher = 0;
            currentDispatcher < getDispatcherConfig().getDispatcherThreadsNum(); currentDispatcher++) {

            ExecutorService dispatcherThread = dispatcherPool[currentDispatcher];
            BlockingDeque<ReActorContext> threadLocalSchedulingQueue = scheduledQueues[currentDispatcher];
            dispatcherThread.submit(() -> Try.ofRunnable(() -> dispatcherLoop(threadLocalSchedulingQueue,
                                                                              dispatcherConfig.getBatchSize(),
                                                                              dispatcherLifeCyclePool,
                                                                              isExecutionRecorded, devNull))
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

    public void dispatch(ReActorContext reActor) {
        if (reActor.acquireScheduling()) {
            //TODO ringbuffer
            scheduledQueues[(int) (nextDispatchIdx.getAndIncrement() % scheduledQueues.length)].addLast(reActor);
        }
    }

    private ExecutorService getDispatcherLifeCyclePool() {
        return Objects.requireNonNull(dispatcherLifeCyclePool);
    }

    private void dispatcherLoop(BlockingDeque<ReActorContext> scheduledList, int dispatcherBatchSize,
                                ExecutorService dispatcherLifeCyclePool, boolean isExecutionRecorded,
                                ReActorRef devNull) {
        int processed = 0;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                //TODO a lot of time is wasted awakening/putting the thread to sleep, a more performant approach
                //should be used
                ReActorContext scheduledReActor = scheduledList.takeFirst();
                //memory acquire
                scheduledReActor.acquireCoherence();

                for (int msgNum = 0; msgNum < dispatcherBatchSize &&
                                     !scheduledReActor.getMbox().isEmpty() &&
                                     !scheduledReActor.isStop(); msgNum++) {
                    Message newEvent = scheduledReActor.getMbox().getNextMessage();

                    if (isExecutionRecorded) {
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
                          sequentials execution attempts in the execution log
                        */
                        devNull.tell(scheduledReActor.getSelf(),
                                     new EventExecutionAttempt(scheduledReActor.getSelf().getReActorId(),
                                                               scheduledReActor.getNextMsgExecutionId(),
                                                               newEvent.getSequenceNumber()));
                    }
                    try {
                        scheduledReActor.reAct(newEvent);
                    } catch (Throwable anyExc) {
                        scheduledReActor.getReActorSystem()
                                        .logError(REACTIONS_EXECUTION_ERROR, anyExc, scheduledReActor.getSelf()
                                                                                                     .getReActorId(),
                                                  newEvent.getPayload()
                                                                                                                              .getClass(), newEvent.getSequenceNumber(), newEvent.toString());
                        scheduledReActor.stop();
                    }
                    processed++;
                }
                //memory release
                scheduledReActor.releaseCoherence();
                //now this reactor can be scheduled by some other thread if required
                scheduledReActor.releaseScheduling();
                if (scheduledReActor.isStop()) {
                    dispatcherLifeCyclePool.submit(() -> scheduledReActor.getReActorSystem()
                                                                         .getLoopback().stop(scheduledReActor.getSelf()
                                                                                                             .getReActorId()));
                } else if (!scheduledReActor.getMbox().isEmpty()) {
                    //If there are other messages to be processed, request another schedulation fo the dispatcher
                    dispatch(scheduledReActor);
                }
            }
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
        LOGGER.debug("Dispatcher Thread {} is terminating. Processed: {}", Thread.currentThread().getName(), processed);
    }
}
