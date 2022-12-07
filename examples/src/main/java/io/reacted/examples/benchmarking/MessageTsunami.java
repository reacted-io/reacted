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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTsunami {
    public static void main(String[] args) {
        String dispatcher_1 = "CruncherThread-1";
        String dispatcher_2 = "CruncherThread-2";
        String dispatcher_3 = "CruncherThread-3";
        ReActorSystem messageCruncher = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                  .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                               .setBatchSize(100_000_000)
                                                                                               .setDispatcherName(dispatcher_1)
                                                                                               .setDispatcherThreadsNum(1)
                                                                                                       .build())
                                                                 .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                      .setBatchSize(100_000_000)
                                                                                                      .setDispatcherName(dispatcher_2)
                                                                                                      .setDispatcherThreadsNum(1)
                                                                                                      .build())
                                                                             .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                                  .setBatchSize(100_000_000)
                                                                                                                  .setDispatcherName(dispatcher_3)
                                                                                                                  .setDispatcherThreadsNum(1)
                                                                                                                  .build())
                                                                  .setExpectedReActorsNum(100)
                                                                  .setReactorSystemName("MessageCruncher")
                                                                  .setSystemMonitorRefreshInterval(Duration.ofHours(1))
                                                                             .build())
                .initReActorSystem();
        Cruncher body_1 = new Cruncher(dispatcher_1, "Cruncher-1");
        Cruncher body_2 = new Cruncher(dispatcher_2, "Cruncher-2");
        Cruncher body_3 = new Cruncher(dispatcher_3, "Cruncher-3");
        ReActorRef cruncher_1 = messageCruncher.spawn(body_1).orElseSneakyThrow();
        ReActorRef cruncher_2 = messageCruncher.spawn(body_2).orElseSneakyThrow();
        ReActorRef cruncher_3 = messageCruncher.spawn(body_3).orElseSneakyThrow();
        Instant start = Instant.now();
        PayLoad payLoad = new PayLoad();
        messageCruncher.getSystemSchedulingService()
                       .scheduleAtFixedRate(() -> {
                               int b1 = body_1.getCounted();
                               int b2 = body_2.getCounted();
                               int b3 = body_3.getCounted();
                               System.err.println("Processed: " + b1 + " " + b2 + " " + b3 + " -> " + (b1 + b2 + b3));
                           },
                                            1, 1, TimeUnit.SECONDS);
        ExecutorService exec_1 = Executors.newSingleThreadExecutor();
        ExecutorService exec_2 = Executors.newSingleThreadExecutor();
        ExecutorService exec_3 = Executors.newSingleThreadExecutor();

        exec_1.execute(() -> runTest(start, cruncher_1, payLoad));
        exec_2.execute(() -> runTest(start, cruncher_2, payLoad));
        exec_3.execute(() -> runTest(start, cruncher_3, payLoad));

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

        System.err.println("Completed in " + ChronoUnit.SECONDS.between(start, Instant.now()));
    }

    private static void runTest(Instant start, ReActorRef cruncher_1, PayLoad payLoad) {
        for(int msg = 0; msg < 100_000_000; msg++) {
            cruncher_1.tell(payLoad);
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

        private Cruncher(String dispatcher, String name) {
            this.cfg = ReActorConfig.newBuilder()
                    .setMailBoxProvider(ctx -> new FastUnboundedMbox())
                                    .setReActorName(name)
                                    .setDispatcherName(dispatcher)
                                    .build();
            this.reActions = ReActions.newBuilder()
                                      .reAct(PayLoad.class, this::onPayload)
                                      .reAct(StopCrunching.class, this::onCrunchStop)
                                      .build();
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
        private void onPayload(ReActorContext ctx, PayLoad payLoad) {
            counter.setPlain(counter.getPlain() + 1);
        }


        @Nonnull
        @Override
        public ReActorConfig getConfig() { return cfg; }

        @Nonnull
        @Override
        public ReActions getReActions() { return reActions; }

    }
}
