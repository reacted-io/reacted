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
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class ReactionTime {
    private static final String CQ_DIR = "/Volumes/RAM Disk";
    public static void main(String[] args) throws InterruptedException {
        ReActorSystem benchmarkSystem;
        benchmarkSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                               .setReactorSystemName(ReactionTime.class.getSimpleName())
/*
                                                    .setLocalDriver(new CQLocalDriver(CQLocalDriverConfig.newBuilder()
                                                                                                         .setChannelName("Q")
                                                                                                         .setChronicleFilesDir(CQ_DIR)
                                                                                                         .build()))
*/
                                                               .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                    .setBatchSize(1_000_000_000)
                                                                                                    .setDispatcherName("Lonely")
                                                                                                    .setDispatcherThreadsNum(1)
                                                                                                    .build())
                                                               .build()).initReActorSystem();
        int iterations = 5_000_000;
        MessageGrabber latencyGrabberBody = new MessageGrabber(iterations);
        ReActorRef latencyGrabber = benchmarkSystem.spawn(latencyGrabberBody.getReActions(),
                                                 ReActorConfig.newBuilder()
                                                              .setMailBoxProvider((ctx) -> new FastUnboundedMbox())
                                                              //.setMailBoxProvider((ctx) -> new BoundedMbox(iterations))
                                                              //.setMailBoxProvider((ctx) -> new FastUnboundedMbox())
                                                              //.setMailBoxProvider((ctx) -> new TypeCoalescingMailbox())
                                                              //.setMailBoxProvider((ctx) -> new FastBoundedBasicMbox(30))
                                                              //.setMailBoxProvider((ctx) -> new BoundedBasicMbox(3000))
                                                              .setReActorName("Interceptor")
                                                              .setDispatcherName("Lonely")
                                                              //.setTypedSubscriptions(TypedSubscription.LOCAL.forType(Long.class))
                                                              .build()).orElseSneakyThrow();
        //Warmup
        //IntStream.range(0, 100_000)
        //         .forEach(val -> benchmarkSystem.getSystemSink().tell(ReActorRef.NO_REACTOR_REF, ""));
        //TimeUnit.SECONDS.sleep(2);

        long pauseWindowDuration = Duration.ofNanos(10000).toNanos();
        long start = System.nanoTime();
        long end;
        long elapsed = 0;
        for(long cycle = 0; cycle < iterations; cycle++) {

            while (elapsed < pauseWindowDuration) {
                end = System.nanoTime();
                elapsed = end - start;
            }

            elapsed = 0;
            start = System.nanoTime();
            latencyGrabber.tell(start);
            //benchmarkSystem.getSystemSink().publish(start);
        }
        TimeUnit.SECONDS.sleep(5);
        latencyGrabberBody.stop().toCompletableFuture().join();
        long[] sortedLatencies = latencyGrabberBody.getLatencies();
        Arrays.sort(sortedLatencies);

        List<Double> percentiles = List.of(70d, 75d, 80d, 85d, 90d, 95d, 99d, 99.9d, 99.99d, 99.9999d, 100d);
        percentiles.forEach(percentile -> System.out.printf("Msgs: %d Percentile %f Latency: %s%n",
                                                            sortedLatencies.length, percentile,
                                                            getLatencyForPercentile(sortedLatencies, percentile)));
        benchmarkSystem.shutDown();
    }

    private static Duration getLatencyForPercentile(long[] latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.length) - 1;
        return Duration.ofNanos(latencies[index]);
    }

    private record LatenciesRequest() implements Serializable {}
    private record LatenciesReply(long[] latencies) implements Serializable {}
    private static class MessageGrabber implements ReActiveEntity {
        private ReActorContext ctx;
        private final long[] latencies;
        private int cycles = 0;

        private MessageGrabber(int iterations) {
            this.latencies = new long[iterations];
        }
        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(Long.class, this::onNanoTime)
                            .reAct(LatenciesRequest.class,
                                   (ctx, request) -> ctx.reply(new LatenciesReply(getLatencies())))
                            .reAct(ReActorInit.class, (ctx, init) -> this.ctx = ctx)
                            .build();
        }

        public long[] getLatencies() {
            return Arrays.copyOf(latencies, cycles);
        }

        public synchronized CompletionStage<Void> stop() { return ctx.stop(); }
        private void onNanoTime(ReActorContext reActorContext, long nanotime) {
            latencies[cycles++] = System.nanoTime() - nanotime;
        }
    }
}
