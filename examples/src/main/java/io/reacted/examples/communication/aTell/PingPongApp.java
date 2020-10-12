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
import io.reacted.drivers.serviceregistries.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.ZooKeeperDriverCfg;
import io.reacted.examples.ExampleUtils;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

        TimeUnit.SECONDS.sleep(5);

        var serverReActor = serverSystem.spawnService(ReActorServiceConfig.newBuilder()
                                                                          .setRouteeProvider(ServerReActor::new)
                                                                          .setSelectionPolicy(ReActorService.LoadBalancingPolicy.ROUND_ROBIN)
                                                                          .setReActorName("ServerService")
                                                                          .setRouteesNum(1)
                                                                          .build()).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(5);

        var clientReActor = clientSystem.spawn(new ClientReActor(clientSystem.serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                                           .setServiceName("ServerService")
                                                                                                                                           .build())
                                                                                        .toCompletableFuture()
                                                                                        .join()
                                                                                        .get()
                                                                                        .getServiceGates()
                                                                                        .iterator()
                                                                                        .next())).orElseSneakyThrow();

        //The reactors are executing now
        TimeUnit.SECONDS.sleep(10000);
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
            System.out.println("Initing");
            var requests = IntStream.range(0, 1_000_000)
                     .mapToObj(idx -> this.serverReference.aTell("Not received"))
                     .collect(Collectors.toUnmodifiableList());
            requests.stream()
                    .reduce((a, b) -> a.thenCompose(c -> {
                        c.ifError(Throwable::printStackTrace);
                        return b; }))
                    .ifPresent(lastStep -> lastStep.thenAcceptAsync(val -> raCtx.logInfo("Finished!")));
        }
    }
}
