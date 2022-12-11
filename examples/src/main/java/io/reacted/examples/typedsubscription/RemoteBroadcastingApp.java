/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.typedsubscription;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.drivers.channels.grpc.GrpcDriver;
import io.reacted.drivers.channels.grpc.GrpcDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class RemoteBroadcastingApp {
    public static void main(String[] args) throws InterruptedException {
        ZooKeeperDriverConfig registryCfg = ZooKeeperDriverConfig.newBuilder()
                                                                 .setReActorName("ZooKeeperDriver")
                                                                 .build();

        GrpcDriverConfig producerDriverCfg = GrpcDriverConfig.newBuilder()
                                                             .setHostName("localhost")
                                                             .setPort(12345)
                                                             .setChannelName("BroadcastChannel")
                                                             .build();

        ReActorSystem producerSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                            .setReactorSystemName("Producer")
                                                                            .addServiceRegistryDriver(new ZooKeeperDriver(registryCfg))
                                                                            .addRemotingDriver(new GrpcDriver(producerDriverCfg))
                                                                            .build()).initReActorSystem();

        GrpcDriverConfig subscriberDriverCfg = GrpcDriverConfig.newBuilder()
                                                               .setHostName("localhost")
                                                               .setPort(54321)
                                                               .setChannelName("BroadcastChannel")
                                                               .build();

        ReActorSystem subscriberSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                            .setReactorSystemName("Subscriber")
                                                                            .addServiceRegistryDriver(new ZooKeeperDriver(registryCfg))
                                                                            .addRemotingDriver(new GrpcDriver(subscriberDriverCfg))
                                                                            .build()).initReActorSystem();

        subscriberSystem.spawn(ReActions.newBuilder()
                                        .reAct(Update.class,
                                               (ctx, update) -> ctx.logInfo("Received {}",
                                                                                update.getClass().getSimpleName()))
                                        .build(),
                               ReActorConfig.newBuilder()
                                            .setReActorName("UpdateListener")
                                            .setTypedSubscriptions(TypedSubscription.FULL.forType(Update.class))
                                            .build());

        TimeUnit.SECONDS.sleep(10);
        producerSystem.broadcastToRemoteSubscribers(new Update());
        TimeUnit.SECONDS.sleep(1);
        producerSystem.shutDown();
        subscriberSystem.shutDown();
    }

    private static final class Update implements Serializable { }
}
