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
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.FastUnboundedMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTsunami {
    private static final int CYCLES = 100_000_000;
    public static void main(String[] args) {


        String dispatcher_1 = "CruncherThread-1";
        int threads_1 = 2;
        Cruncher body_1 = new Cruncher(dispatcher_1, "Cruncher-1", CYCLES);

        String dispatcher_2 = "CruncherThread-2";
        int threads_2 = 1;
        Cruncher body_2 = new Cruncher(dispatcher_2, "Cruncher-2", CYCLES);

        String dispatcher_3 = "CruncherThread-3";
        int threads_3 = 1;
        Cruncher body_3 = new Cruncher(dispatcher_3, "Cruncher-3", CYCLES);


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


        ReActorRef cruncher_1 = messageCruncher.spawn(body_1)
                                               .orElseSneakyThrow();
        ReActorRef cruncher_2 = messageCruncher.spawn(body_2)
                                               .orElseSneakyThrow();
        ReActorRef cruncher_3 = messageCruncher.spawn(body_3)
                                               .orElseSneakyThrow();
        Instant start = Instant.now();
        var diagnosticPrinter =
        messageCruncher.getSystemSchedulingService()
                       .scheduleAtFixedRate(() -> {
                           int b1 = body_1.getCounted();
                           int b2 = body_2.getCounted();
                           int b3 = body_3.getCounted();
                           System.err.println("Processed: " + b1 + " " + b2 + " " + b3 + " -> " + (b1 + b2 + b3));
                       }, 1, 1, TimeUnit.SECONDS);

        ExecutorService exec_1 = Executors.newSingleThreadExecutor();
        ExecutorService exec_2 = Executors.newSingleThreadExecutor();
        ExecutorService exec_3 = Executors.newSingleThreadExecutor();
        ExecutorService exec_4 = Executors.newSingleThreadExecutor();

        exec_1.execute(() -> runTest(start, cruncher_1));
        exec_2.execute(() -> runTest(start, cruncher_2));
        exec_3.execute(() -> runTest(start, cruncher_3));
        //exec_4.execute(() -> runTest(start, cruncher_1));

        Optional.ofNullable(messageCruncher.getReActorCtx(cruncher_1.getReActorId()))
                .map(ReActorContext::getHierarchyTermination)
                .map(CompletionStage::toCompletableFuture)
                .ifPresent(CompletableFuture::join);

        Optional.ofNullable(messageCruncher.getReActorCtx(cruncher_2.getReActorId()))
                .map(ReActorContext::getHierarchyTermination)
                .map(CompletionStage::toCompletableFuture)
                .ifPresent(CompletableFuture::join);
        Optional.ofNullable(messageCruncher.getReActorCtx(cruncher_3.getReActorId()))
                .map(ReActorContext::getHierarchyTermination)
                .map(CompletionStage::toCompletableFuture)
                .ifPresent(CompletableFuture::join);

        diagnosticPrinter.cancel(false);

        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));

        for (Cruncher body : List.of(body_1, body_2, body_3)) {
            long[] sortedLatencies = body.getLatencies();
            Arrays.sort(sortedLatencies);

            List<Double> percentiles = List.of(70d, 75d, 80d, 85d, 90d, 95d, 99d, 99.9d, 99.99d, 99.9999d, 100d);
            percentiles.forEach(percentile -> System.out.printf("Msgs: %d Percentile %f Latency: %s%n", sortedLatencies.length, percentile, getLatencyForPercentile(sortedLatencies, percentile)));
        }
        messageCruncher.shutDown();
    }

    private static Duration getLatencyForPercentile(long[] latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.length) - 1;
        return Duration.ofNanos(latencies[index]);
    }

    private static void runTest(Instant start, ReActorRef cruncher_1) {
        for(int msg = 0; msg < CYCLES; msg++) {
            cruncher_1.tell(System.nanoTime());
        }
        cruncher_1.tell(new StopCrunching());
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
                    .setMailBoxProvider(ctx -> new FastUnboundedMbox())
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onPayload)
                                      .reAct(StopCrunching.class, this::onCrunchStop)
                                      .build();
            this.latencies = new long[iterations];
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
            reActorContext.stop();
        }
        private void onPayload(ReActorContext ctx, Long payLoad) {
            int pos = counter.getPlain();
            latencies[pos] = System.nanoTime() - payLoad;
            counter.setPlain(pos + 1);
        }


        @Nonnull
        @Override
        public ReActorConfig getConfig() { return cfg; }

        @Nonnull
        @Override
        public ReActions getReActions() { return reActions; }

    }
}
