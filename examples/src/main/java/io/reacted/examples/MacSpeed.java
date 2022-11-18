/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples;

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

public class MacSpeed {
  private static final int REQUESTS_PER_ACTOR = 1_00_000;
  public static void main(String[] args) throws InterruptedException {
    var instances = 4;
    var batchDispatcher = DispatcherConfig.newBuilder()
                                          .setBatchSize(10000)
                                          .setDispatcherName("Batch")
                                          .setDispatcherThreadsNum(instances)
                                          .build();
    ReActorSystem speedTest = new ReActorSystem(ReActorSystemConfig.newBuilder()
/*
                                                    .setLocalDriver(new CQLocalDriver(CQDriverConfig.newBuilder()
                                                                                                    .setChronicleFilesDir("/tmp/")
                                                                                                    //.setChronicleFilesDir("/Volumes/RAMDisk/")
                                                                                                    .setTopicName("SpeedTest")
                                                                                                    .setChannelName("SpeedTestChan")
                                                                                                    .build()))
*/
                                                    .setReactorSystemName(MacSpeed.class.getSimpleName())
                                                    .addDispatcherConfig(batchDispatcher)
                                                    .build()).initReActorSystem();
    /*
    var reactorService = speedTest.spawnService(ServiceConfig.newBuilder()

                                                    .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                        .setBufferSize(10000)
                                                        .setBackpressureTimeout(BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT)
                                                        .setRealMailboxOwner(ctx)
                                                        .setRequestOnStartup(10000)
                                                        .build())
                                                    .setDispatcherName("Batch")
                                                    .setReActorName("Receiver")
                                                    .setRouteeProvider(Received::new)
                                                    .setRouteesNum(instances)
                                                    .build()).orElseSneakyThrow();

     */
    List<Received> reactors = IntStream.range(0, 10_000)
                                       .mapToObj(Received::new)
                                       .toList();

    var executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> System.err.println("Msgs: " + reactors.stream()
                                                                             .mapToLong(Received::getRequests)
                                                                             .sum()),
                                 0, 1, TimeUnit.SECONDS);
    Function<Received, Runnable> sender = rec -> () -> {
      var req = new Request();
      var reactorService = speedTest.spawn(rec).orElseSneakyThrow();
      long amount = 1;
      for (int cycle = 0; cycle < REQUESTS_PER_ACTOR; cycle++) {

        if (reactorService.tell(req) == DeliveryStatus.BACKPRESSURE_REQUIRED) {
          var namount = amount;
          Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(namount))
              .orElseSneakyThrow();

          amount <<= 1;
        } else {
          amount = 10;
        }
      }
    };
    List<ExecutorService> executors = IntStream.range(0, 8)
        .mapToObj(cycle -> Executors.newSingleThreadScheduledExecutor())
        .collect(Collectors.toList());

    List<Runnable> publishers = reactors.stream()
                                        .map(sender)
                                        .toList();
    for (int publisher = 0; publisher < publishers.size(); publisher++) {
      executors.get(publisher % executors.size()).submit(publishers.get(publisher));
    }

    System.out.println("Done");
    TimeUnit.SECONDS.sleep(100);
    speedTest.shutDown();
    executors.forEach(ExecutorService::shutdown);
    executor.shutdown();
    System.out.println("Terminated");
    TimeUnit.SECONDS.sleep(1000);
  }

  private static final class Received implements ReActor {
    private final ReActorConfig.Builder config = ReActorConfig.newBuilder()
        .setDispatcherName("Batch")

        .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                     .setRequestOnStartup(10000)
                                                     .setBackpressuringThreshold(10001)
                                                     .setRealMailboxOwner(ctx)
                                                     .build());
    private final ReActions reactions = ReActions.newBuilder()
        .reAct(Request.class, this::onRequest)
        .build();
    private long requests = 0L;
    private Received(int cycle) {
      config.setReActorName(Received.class.getSimpleName() + " " + Instant.now() + System.nanoTime() + " " + cycle);
    }
    @Nonnull
    @Override
    public ReActorConfig getConfig() { return config.build(); }

    @Nonnull
    @Override
    public ReActions getReActions() { return reactions; }

    private void onRequest(ReActorContext ctx, Request req) {
      ctx.getMbox().request(1);
      requests++;
      if (requests == REQUESTS_PER_ACTOR) {
        ctx.stop();
      }
    }
    private long getRequests() { long requests = this.requests;
    this.requests = 0;
    return requests; }
  }

  private record Request() implements Serializable { }
//  private record Request() implements Serializable { }
}
