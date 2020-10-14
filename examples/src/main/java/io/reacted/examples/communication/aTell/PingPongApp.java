/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.aTell;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorServiceConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.ReActorService;
import io.reacted.drivers.channels.grpc.GrpcDriver;
import io.reacted.drivers.channels.grpc.GrpcDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverCfg;
import io.reacted.examples.ExampleUtils;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class PingPongApp {
    public static void main(String[] args) throws InterruptedException {
        Properties zooKeeperProps = new Properties();
        zooKeeperProps.put(ZooKeeperDriver.ZK_CONNECTION_STRING, "localhost:2181");
        var clientSystemCfg = ExampleUtils.getDefaultReActorSystemCfg("Client",
                                                                   SystemLocalDrivers.DIRECT_COMMUNICATION,
                                                                   List.of(new ZooKeeperDriver(ZooKeeperDriverCfg.newBuilder()
                                                                                                                 .setTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class))
                                                                                                                 .setReActorName("ZooKeeperDriver")
                                                                                                                 .setServiceRegistryProperties(zooKeeperProps)
                                                                                                                 .build())),
                                                                   List.of(new GrpcDriver(GrpcDriverConfig.newBuilder()
                                                                                                          .setHostName("localhost")
                                                                                                          .setPort(12345)
                                                                                                          .setChannelRequiresDeliveryAck(true)
                                                                                                          .setChannelName("TestChannel")
                                                                                                          .build())));
        var serverSystemCfg = ExampleUtils.getDefaultReActorSystemCfg("Server",
                                                                   //SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/server"),
                                                                   SystemLocalDrivers.DIRECT_COMMUNICATION,
                                                                   List.of(new ZooKeeperDriver(ZooKeeperDriverCfg.newBuilder()
                                                                                                                 .setTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class))
                                                                                                                 .setReActorName("ZooKeeperDriver")
                                                                                                                 .setServiceRegistryProperties(zooKeeperProps)
                                                                                                                 .build())),
                                                                   List.of(new GrpcDriver(GrpcDriverConfig.newBuilder()
                                                                                                          .setHostName("localhost")
                                                                                                          .setPort(54321)
                                                                                                          .setChannelRequiresDeliveryAck(true)
                                                                                                          .setChannelName("TestChannel")
                                                                                                          .build())));
        var clientSystem = new ReActorSystem(clientSystemCfg).initReActorSystem();
        var serverSystem = new ReActorSystem(serverSystemCfg).initReActorSystem();

        TimeUnit.SECONDS.sleep(10);

        var serverReActor = serverSystem.spawnService(ReActorServiceConfig.newBuilder()
                                                                          .setRouteeProvider(ServerReActor::new)
                                                                          .setLoadBalancingPolicy(ReActorService.LoadBalancingPolicy.ROUND_ROBIN)
                                                                          .setReActorName("ServerService")
                                                                          .setRouteesNum(1)
                                                                          .build()).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(10);
        var remoteService = clientSystem.serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                           .setServiceName("ServerService")
                                                                                           .build())
                                        .toCompletableFuture().join()
                                        .get().getServiceGates()
                                        .iterator().next();

        var clientReActor = clientSystem.spawn(new ClientReActor(remoteService)).orElseSneakyThrow();

        //The reactors are executing now
        TimeUnit.SECONDS.sleep(3600);
        remoteService.aTell("Banana").thenAccept(ds -> ds.filter(DeliveryStatus::isDelivered)
                                                         .ifSuccessOrElse(success -> System.out.println("Delivered also banana"),
                                                                          Throwable::printStackTrace));
        TimeUnit.SECONDS.sleep(5);
        //The game is finished, shut down
        clientSystem.shutDown();
        serverSystem.shutDown();
    }

    private static class ServerReActor implements ReActor {
        @Nonnull
        @Override
        public ReActions getReActions() { return ReActions.NO_REACTIONS; }

        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return ReActorConfig.newBuilder()
                                .setReActorName(ServerReActor.class.getSimpleName())
                                .build();
        }
    }

    private static class ClientReActor implements ReActor {
        private final ReActorRef serverReference;
        public ClientReActor(ReActorRef serverReference) {
            this.serverReference = Objects.requireNonNull(serverReference);
        }
        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return ReActorConfig.newBuilder()
                                .setReActorName(ClientReActor.class.getSimpleName())
                                .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, (ctx, init) -> onInit(ctx))
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext raCtx) {
            long start = System.nanoTime();
            AsyncUtils.asyncLoop(noval -> this.serverReference.aTell("Not received"),
                                 Try.of(() -> DeliveryStatus.DELIVERED),
                                 (Try<DeliveryStatus>) null, 1_000_000L)
                      .thenAccept(status -> System.err.printf("Async loop finished. Time %s Thread %s%n",
                                                              Duration.ofNanos(System.nanoTime() - start)
                                                                      .toString(),
                                                              Thread.currentThread().getName()));
            long end = System.nanoTime();
            System.out.println("Finished storm: " + Duration.ofNanos(end - start).toString());
        }
    }
}
