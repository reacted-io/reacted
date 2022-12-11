package io.reacted.examples.benchmarking;/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.FastUnboundedMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class MessageTsunami {
    private static final int CYCLES = 99999;
    public static void main(String[] args) throws InterruptedException {


        String dispatcher_1 = "CruncherThread-1"; int threads_1 = 4;
        String dispatcher_2 = "CruncherThread-2"; int threads_2 = 1;
        int workersNum = 3;

        ReActorSystem crunchingSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                             .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                  .setBatchSize(100_000_000)
                                                                                                                  .setDispatcherName(dispatcher_1)
                                                                                                                  .setDispatcherThreadsNum(threads_1)
                                                                                                                  .build())
                                                                             .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                  .setBatchSize(100_000_000)
                                                                                                                  .setDispatcherName(dispatcher_2)
                                                                                                                  .setDispatcherThreadsNum(threads_2)
                                                                                                                  .build())
                                                                             .setExpectedReActorsNum(50)
                                                                             .setReactorSystemName("MessageCrunchingSystem")
                                                                             .build()).initReActorSystem();

        BenchmarkingUtils.initStatisticsCollectorProcessor(crunchingSystem, workersNum);

        ReActorRef cruncher_service = crunchingSystem.spawnService(ServiceConfig.newBuilder()
                                                                                .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                                             .setBackpressuringThreshold(30000)
                                                                                                                             //.setRealMbox(new FastUnboundedMbox())
                                                                                                                             .setRealMailboxOwner(ctx)
                                                                                                                             .build())
                                                                                .setRouteeProvider(() -> new CrunchingWorker(dispatcher_2, "worker", CYCLES))
                                                                                .setDispatcherName(dispatcher_1)
                                                                                .setRouteesNum(workersNum)
                                                                                .setReActorName("CruncherService")
                                                                                .build()).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(2);

        Instant start = Instant.now();
        BenchmarkingUtils.initAndWaitForMessageProducersToCompleteWithDedicatedExecutors(BenchmarkingUtils.backpressureAwareMessageSender(CYCLES/3, cruncher_service),
                                                                                         BenchmarkingUtils.backpressureAwareMessageSender(CYCLES/3, cruncher_service),
                                                                                         BenchmarkingUtils.backpressureAwareMessageSender(CYCLES/3, cruncher_service));

        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));


        TimeUnit.SECONDS.sleep(5);

        //cruncher_service.tell("BANANA");

        TimeUnit.SECONDS.sleep(5);

        IntStream.range(0, workersNum)
                 .forEach(iter -> cruncher_service.tell(new BenchmarkingUtils.StopCrunching()));
        //crunchingSystem.shutDown();
    }

    private static class CrunchingWorker implements ReActor {
        private final AtomicInteger counter = new AtomicInteger(0);
        private final ReActorConfig cfg;
        private final ReActions reActions;
        private int counted = 0;
        private long[] latencies;
        private volatile int marker;

        private CrunchingWorker(String dispatcher, String name, int iterations) {
            this.cfg = ReActorConfig.newBuilder()
                                    .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                 .setBackpressuringThreshold(30000)
                                                                                 .setRealMbox(new FastUnboundedMbox())
                                                                                 .setRealMailboxOwner(ctx)
                                                                                 .setNonDelayable(BenchmarkingUtils.DiagnosticRequest.class,
                                                                                                  ReActorInit.class)
                                                                                 .build())
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(BenchmarkingUtils.DiagnosticRequest.class),
                                                           TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(BenchmarkingUtils.StopCrunching.class))
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onPayload)
                                      .reAct(BenchmarkingUtils.StopCrunching.class, this::onCrunchStop)
                                      .reAct(BenchmarkingUtils.DiagnosticRequest.class, this::onDiagnosticRequest)
                                      .build();
            this.latencies = new long[iterations + iterations / 3];
        }

        private void onDiagnosticRequest(ReActorContext reActorContext, BenchmarkingUtils.DiagnosticRequest payloadT) {
            reActorContext.getReActorSystem()
                          .broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF,
                                                       new BenchmarkingUtils.RPISnapshot(getCounted()));
        }

        public synchronized long[] getLatencies() {
            return Arrays.copyOf(latencies, counter.get());
        }
        private int getCounted() {
            int _cnt = counter.getAcquire();
            int cnt =  _cnt - counted;
            this.counted = _cnt;
            return cnt;
        }
        private void onCrunchStop(ReActorContext reActorContext, BenchmarkingUtils.StopCrunching stopCrunching) {
            reActorContext.getReActorSystem().getSystemSink()
                          .publish(new BenchmarkingUtils.LatenciesSnapshot(getLatencies()));
            reActorContext.getMbox().request(1);
            reActorContext.stop();
        }
        private void onPayload(ReActorContext ctx, Long payLoad) {
            if (marker != 0) {
                System.err.println("CRITIC!");
            }
            marker = 1;
            int pos = counter.getPlain();
            latencies[pos] = System.nanoTime() - payLoad;
            counter.setPlain(pos + 1);
            ctx.getMbox().request(1);
            if (marker != 1) {
                System.err.println("CRITIC!");
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
