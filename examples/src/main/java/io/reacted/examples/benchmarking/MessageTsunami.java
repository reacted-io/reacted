/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.benchmarking;

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.FastUnboundedMbox;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.LoadBalancingPolicies;
import io.reacted.core.services.LoadBalancingPolicy;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.operators.map.MapOperatorConfig;
import io.reacted.flow.operators.reduce.ReduceOperatorConfig;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MessageTsunami {
    private static final int CYCLES = 17_000_000;
    public static void main(String[] args) throws InterruptedException {


        String dispatcher_1 = "CruncherThread-1"; int threads_1 = 1;
        String dispatcher_2 = "CruncherThread-2"; int threads_2 = 4;
        String dispatcher_3 = "CruncherThread-3"; int threads_3 = 1;

        ReActorSystem messageCruncher = new ReActorSystem(ReActorSystemConfig.newBuilder()
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
                                                                             .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                  .setBatchSize(100_000_000)
                                                                                                                  .setDispatcherName(dispatcher_3)
                                                                                                                  .setDispatcherThreadsNum(threads_3)
                                                                                                                  .build())
                                                                             .setExpectedReActorsNum(100)
                                                                             .setReactorSystemName("MessageCruncher")
                                                                             .setSystemMonitorRefreshInterval(Duration.ofHours(1))
                                                                             .build()).initReActorSystem();

        var collector =
        ReActedGraph.newBuilder()
                    .setReActorName("Test Statistics Collector")
                    .setDispatcherName(dispatcher_3)
                    .addOperator(ReduceOperatorConfig.newBuilder()
                                                     .setReActorName("LatenciesCollector")
                                                     .setReductionRules(Map.of(LatenciesSnapshot.class, 3L))
                                                     .setReducer(payloadsByType -> {
                                                         List<LatenciesSnapshot> rps = (List<LatenciesSnapshot>)payloadsByType.get(LatenciesSnapshot.class);
                                                         return getLatenciesForPercentiles(rps.stream()
                                                                                              .map(snap -> snap.latencies)
                                                                                              .map(l -> (LongStream)Arrays.stream(l))
                                                                                              .flatMapToLong(la -> la)
                                                                                              .sorted()
                                                                                              .toArray());
                                                     })
                                                    .setOutputOperators("Printer")
                                                    .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(LatenciesSnapshot.class))
                                                    .build())
                    .addOperator(ReduceOperatorConfig.newBuilder()
                                                     .setReActorName("RPSCollector")
                                                     .setReductionRules(Map.of(RPSSnapshot.class, 3L))
                                                     .setReducer(payloadsByType -> {
                                                         List<RPSSnapshot> rps = (List<RPSSnapshot>)payloadsByType.get(RPSSnapshot.class);
                                                         return List.of(String.format("Processed: %d %d %d -> %d%n",
                                                                                      rps.get(0).rps, rps.get(1).rps, rps.get(2).rps,
                                                                                      rps.stream()
                                                                                         .mapToInt(v -> v.rps)
                                                                                         .sum()));
                                                     })
                                                     .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(RPSSnapshot.class))
                                                     .setOutputOperators("Printer")
                                                     .build())
                    .addOperator(MapOperatorConfig.newBuilder()
                                                  .setReActorName("Printer")
                                                  .setConsumer(System.err::println)
                                                  .build())
                    .build();
        collector.run(messageCruncher)
                 .toCompletableFuture()
                 .join()
                 .orElseSneakyThrow();

        ReActorRef cruncher_service = messageCruncher.spawnService(ServiceConfig.newBuilder()
                                                                                .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                                             .setBackpressuringThreshold(30000)
                                                                                                                             //.setRealMbox(new FastUnboundedMbox())
                                                                                                                             .setRealMbox(new UnboundedMbox())
                                                                                                                             .setRealMailboxOwner(ctx)
                                                                                                                             .build())
                                                                                .setRouteeProvider(() -> new Cruncher(dispatcher_2, "-worker", CYCLES))
                                                                                .setDispatcherName(dispatcher_1)
                                                                                .setRouteesNum(3)
                                                                                .setReActorName("CruncherService")
                                                                                .build()).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(2);

        var diagnosticPrinter =
        messageCruncher.getSystemSchedulingService()
                       .scheduleAtFixedRate(() -> {
                           messageCruncher.broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF,
                                                                       new DiagnosticRequest());
                       }, 1, 1, TimeUnit.SECONDS);

        ExecutorService exec_1 = Executors.newSingleThreadExecutor();
        ExecutorService exec_2 = Executors.newSingleThreadExecutor();
        ExecutorService exec_3 = Executors.newSingleThreadExecutor();

        Instant start = Instant.now();
        List.of(exec_1.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))),
                exec_2.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))),
                exec_3.submit(UnChecked.runnable(() -> runTest(start, cruncher_service))))
               .forEach(fut -> Try.of(() -> fut.get())
                                  .ifError(Throwable::printStackTrace));
        cruncher_service.tell(new StopCrunching());
        cruncher_service.tell(new StopCrunching());
        cruncher_service.tell(new StopCrunching());
        //cruncher_service.tell(new StopCrunching());

        //diagnosticPrinter.cancel(false);

        //TimeUnit.SECONDS.sleep(1);
        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));

        //messageCruncher.shutDown();
    }

    private static List<String> getLatenciesForPercentiles(long[] latencies) {
        List<Double> percentiles = List.of(70d, 75d, 80d, 85d, 90d, 95d, 99d, 99.9d, 99.99d, 99.9999d, 100d);
        return percentiles.stream()
                   .map(percentile -> String.format("Msgs: %d Percentile %f Latency: %s",
                                                    latencies.length, percentile,
                                                    getLatencyForPercentile(latencies, percentile)))
                          .collect(Collectors.toList());
    }
    private static Duration getLatencyForPercentile(long[] latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.length) - 1;
        return Duration.ofNanos(latencies[index]);
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
                delay = delay << 1;
            } else {
                delay = Math.max( (delay / 3) << 1, baseNanosDelay);
            }
        }
        System.err.println("Sent in " + ChronoUnit.SECONDS.between(start, Instant.now()));
    }

    private record PayLoad() implements Serializable { }
    private record StopCrunching() implements Serializable { }

    private static class Cruncher implements ReActor {
        private final AtomicInteger counter = new AtomicInteger(0);
        private final ReActorConfig cfg;
        private final ReActions reActions;
        private int counted = 0;
        private long[] latencies;

        private Cruncher(String dispatcher, String name, int iterations) {
            this.cfg = ReActorConfig.newBuilder()
                                    .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                 .setBackpressuringThreshold(30000)
                                                                                 .setRealMbox(new FastUnboundedMbox())
                                                                                 .setRealMailboxOwner(ctx)
                                                                                 .build()
                                                                                 .addNonDelayableTypes(DiagnosticRequest.class))
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(DiagnosticRequest.class))
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onPayload)
                                      .reAct(StopCrunching.class, this::onCrunchStop)
                                      .reAct(DiagnosticRequest.class, this::onDiagnosticRequest)
                                      .build();
            this.latencies = new long[iterations * 2];
        }

        private void onDiagnosticRequest(ReActorContext reActorContext, DiagnosticRequest payloadT) {
            reActorContext.getReActorSystem()
                          .broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF, new RPSSnapshot(getCounted()));
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
        private void onCrunchStop(ReActorContext reActorContext, StopCrunching stopCrunching) {
            reActorContext.getReActorSystem().getSystemSink()
                          .publish(new LatenciesSnapshot(getLatencies()));
            reActorContext.getMbox().request(1);
            reActorContext.stop();
        }
        private void onPayload(ReActorContext ctx, Long payLoad) {
            int pos = counter.getPlain();
            latencies[pos] = System.nanoTime() - payLoad;
            counter.setPlain(pos + 1);
            ctx.getMbox().request(1);
        }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return cfg; }

        @Nonnull
        @Override
        public ReActions getReActions() { return reActions; }

    }

    private static final class DiagnosticRequest implements Serializable { }

    private static final class RPSSnapshot implements Serializable {
        private final int rps;
        RPSSnapshot(int rps) { this.rps = rps; }
    }
    private static final class LatenciesSnapshot implements Serializable {
        private final long[] latencies;
        private LatenciesSnapshot(long[] latencies) {
            this.latencies = latencies;
        }
    }
}
