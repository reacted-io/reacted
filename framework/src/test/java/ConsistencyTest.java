/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.FastUnboundedMbox;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class ConsistencyTest {
    private static final int CYCLES = 9_999_999;
    public static void main(String[] args)  {


        String worker_dispatcher = "CruncherThread-1"; int worker_dispatcher_threads = 2;

        ReActorSystem crunchingSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                             .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                  .setBatchSize(CYCLES / worker_dispatcher_threads)
                                                                                                                  .setDispatcherName(worker_dispatcher)
                                                                                                                  .setDispatcherThreadsNum(worker_dispatcher_threads)
                                                                                                                  .build())
                                                                             .setExpectedReActorsNum(20)
                                                                             .setReactorSystemName("MessageCrunchingSystem")
                                                                             .build()).initReActorSystem();

        CrunchingWorker workerBody = new CrunchingWorker(worker_dispatcher, "Cruncher");
        ReActorRef worker = crunchingSystem.spawn(workerBody).orElseSneakyThrow();

        Instant start = Instant.now();

        BenchmarkingUtils.initAndWaitForMessageProducersToCompleteWithDedicatedExecutors(BenchmarkingUtils.constantWindowMessageSender(CYCLES/3, worker, Duration.ofNanos(10000)),
                                                                                         BenchmarkingUtils.constantWindowMessageSender(CYCLES/3, worker, Duration.ofNanos(500000)),
                                                                                         BenchmarkingUtils.constantWindowMessageSender(CYCLES/3, worker, Duration.ofNanos(5000)));

        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));

        if (worker.tell(ReActorStop.STOP).isNotDelivered()) {
            System.err.println("CRITIC! Unable to deliver stop!?");
            System.exit(3);
        }

        crunchingSystem.getReActorCtx(worker.getReActorId())
                       .getHierarchyTermination()
                       .toCompletableFuture()
                       .join();

        System.err.println("Events received: " + workerBody.getCounter());

        crunchingSystem.shutDown();
    }

    private static class CrunchingWorker implements ReActor {
        private long counter = 0L;
        private final ReActorConfig cfg;
        private final ReActions reActions;
        private volatile int marker;

        private CrunchingWorker(String dispatcher, String name) {
            this.cfg = ReActorConfig.newBuilder()
                                    .setMailBoxProvider(ctx -> new FastUnboundedMbox())
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onPayload)
                                      .reAct(ReActorStop.class, this::onStop)
                                      .build();
        }

        private synchronized long getCounter() { return counter; }

        private void onStop(ReActorContext reActorContext, ReActorStop stopCrunching) {
            reActorContext.stop();
        }
        private void onPayload(ReActorContext ctx, Long payLoad) {
            if (marker != 0) {
                System.err.println("CRITIC!");
                System.exit(1);
            }
            marker = 1;
            counter++;
            BenchmarkingUtils.nanoSleep(100);
            if (marker != 1) {
                System.err.println("CRITIC!");
                System.exit(2);
            }
            marker = 0;
        }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return cfg; }

        @Nonnull
        @Override
        public ReActions getReActions() { return reActions; }

    }
}
