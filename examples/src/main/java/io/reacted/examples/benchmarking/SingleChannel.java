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
import io.reacted.core.mailboxes.FastBoundedMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;

public class SingleChannel {
    public static void main(String[] args) {
        final int cycles = 1_00_000_000;
        ReActorSystem singleChanelSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                                .setExpectedReActorsNum(256)
                                                                                .setReactorSystemName(SingleChannel.class.getSimpleName())
                                                                                .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                     .setDispatcherName(SingleChannel.class.getSimpleName())
                                                                                                                     .setBatchSize(cycles)
                                                                                                                     .setDispatcherThreadsNum(1)
                                                                                                                     .build())
                                                                                .build()).initReActorSystem();
        Cycler cyclerBody = new Cycler(cycles);

        singleChanelSystem.getReActorCtx(singleChanelSystem.spawn(cyclerBody,
                                                                  ReActorConfig.newBuilder()
                                                                               .setMailBoxProvider(ctx -> new FastBoundedMbox(4))
                                                                               .setDispatcherName(SingleChannel.class.getSimpleName())
                                                                               .setReActorName(Cycler.class.getSimpleName())
                                                                               .build())
                                                           .orElseSneakyThrow()
                                                           .getReActorId())
                          .getHierarchyTermination()
                          .toCompletableFuture()
                          .join();
        BenchmarkingUtils.computeLatenciesOutput(cyclerBody.getLatencies())
                         .forEach(System.out::println);
        singleChanelSystem.shutDown();
    }

    private final static class Cycler implements ReActiveEntity {

        private final long[] latencies;
        private final ReActions reActions;
        private int idx;
        private Cycler(int cycles) {
            this.latencies = new long[cycles];
            this.reActions = ReActions.newBuilder()
                                      .reAct(Long.class, this::onLong)
                                      .reAct(ReActorInit.class, (ctx, init) -> ctx.selfTell(System.nanoTime()))
                                      .build();
        }

        private synchronized long[] getLatencies() { return latencies; }
        private void onLong(ReActorContext ctx, Long latency) {
            latencies[idx++] = System.nanoTime() - latency;
            if (idx < latencies.length) {
                ctx.selfTell(System.nanoTime());
            } else {
                ctx.stop();
            }
        }
        @Nonnull
        @Override
        public ReActions getReActions() { return reActions; }
    }
}
