/*
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
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTsunami {
    private static final int CYCLES = 33_333_333;
    public static void main(String[] args) throws InterruptedException {


        String dispatcher_1 = "CruncherThread-1"; int threads_1 = 1;
        String dispatcher_2 = "CruncherThread-2"; int threads_2 = 4;
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
                                                                             .setExpectedReActorsNum(100)
                                                                             .setReactorSystemName("MessageCrunchingSystem")
                                                                             .build()).initReActorSystem();

        StatisticsCollector.initStatisticsCollectorProcessor(crunchingSystem, workersNum);

        ReActorRef cruncher_service = crunchingSystem.spawnService(ServiceConfig.newBuilder()
                                                                                .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                                             .setBackpressuringThreshold(30000)
                                                                                                                             .setRealMbox(new FastUnboundedMbox())
                                                                                                                             .setRealMailboxOwner(ctx)
                                                                                                                             .build())
                                                                                .setRouteeProvider(() -> new CrunchingWorker(dispatcher_2, "worker", CYCLES))
                                                                                .setDispatcherName(dispatcher_1)
                                                                                .setRouteesNum(workersNum)
                                                                                .setReActorName("CruncherService")
                                                                                .build()).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(2);


        ExecutorService exec_1 = Executors.newSingleThreadExecutor();
        ExecutorService exec_2 = Executors.newSingleThreadExecutor();
        ExecutorService exec_3 = Executors.newSingleThreadExecutor();

        Instant start = Instant.now();
        List.of(exec_1.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))),
                exec_2.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))),
                exec_3.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))))
               .forEach(fut -> Try.of(() -> fut.get())
                                  .ifError(Throwable::printStackTrace));
        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));
        TimeUnit.SECONDS.sleep(1);
        StatisticsCollector.requestsLatenciesFromWorkers(crunchingSystem);

        //crunchingSystem.shutDown();
    }

    private static void runTest(Instant start, ReActorRef cruncher_1) throws InterruptedException {
        long baseNanosDelay = 1_000_000;
        long delay = baseNanosDelay;
        for(int msg = 0; msg < CYCLES; msg++) {
            DeliveryStatus status = cruncher_1.tell(System.nanoTime());
            if (status.isNotDelivered()) {
                System.err.println("FAILED DELIVERY? ");
                System.exit(3);
            }
            if (status.isBackpressureRequired()) {
                TimeUnit.NANOSECONDS.sleep(delay);
                delay = delay + (delay / 3);
            } else {
                delay = Math.max( (delay / 3) << 1, baseNanosDelay);
            }
        }
        System.err.println("Sent in " + ChronoUnit.SECONDS.between(start, Instant.now()));
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
                                                                                 .setNonDelayable(StatisticsCollector.DiagnosticRequest.class,
                                                                                                  ReActorInit.class)
                                                                                 .build())
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(StatisticsCollector.DiagnosticRequest.class),
                                                           TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(StatisticsCollector.StopCrunching.class))
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onPayload)
                                      .reAct(StatisticsCollector.StopCrunching.class, this::onCrunchStop)
                                      .reAct(StatisticsCollector.DiagnosticRequest.class, this::onDiagnosticRequest)
                                      .build();
            this.latencies = new long[iterations * 2];
        }

        private void onDiagnosticRequest(ReActorContext reActorContext, StatisticsCollector.DiagnosticRequest payloadT) {
            reActorContext.getReActorSystem()
                          .broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF, new StatisticsCollector.RPISnapshot(getCounted()));
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
        private void onCrunchStop(ReActorContext reActorContext, StatisticsCollector.StopCrunching stopCrunching) {
            reActorContext.getReActorSystem().getSystemSink()
                          .publish(new StatisticsCollector.LatenciesSnapshot(getLatencies()));
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
