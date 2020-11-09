/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.quickstart;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.kafka.KafkaDriver;
import io.reacted.drivers.channels.kafka.KafkaDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;

@NonNullByDefault
public class QuickstartSubscriber {
    public static void main(String[] args) {
        var kafkaDriverConfig = KafkaDriverConfig.newBuilder()
                                                 .setChannelName("KafkaQuickstartChannel")
                                                 .setTopic("ReactedTopic")
                                                 .setGroupId("QuickstartGroupSubscriber")
                                                 .setMaxPollRecords(1)
                                                 .setBootstrapEndpoint("localhost:9092")
                                                 .build();
        var zooKeeperDriverConfig = ZooKeeperDriverConfig.newBuilder()
                                                         .setReActorName("LocalhostCluster")
                                                         .build();
        var showOffSubscriberSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                           .addRemotingDriver(new KafkaDriver(kafkaDriverConfig))
                                                                           .setReactorSystemName("ShowOffSubscriberSystemName")
                                                                           .addServiceRegistryDriver(new ZooKeeperDriver(zooKeeperDriverConfig))
                                                                           .build()).initReActorSystem();
        showOffSubscriberSystem.spawnService(ServiceConfig.newBuilder()
                                                          .setRouteeProvider(GreetingsRequestSubscriber::new)
                                                          .setReActorName("DataCaptureService")
                                                          .setRouteesNum(4)
                                                          .setTypedSubscriptions(TypedSubscription.FULL.forType(GreeterService.GreetingsRequest.class))
                                                          .build())
                               .peekFailure(Throwable::printStackTrace)
                               .ifError(error -> showOffSubscriberSystem.shutDown());
    }

    private static class GreetingsRequestSubscriber implements ReActor {
        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return  ReActorConfig.newBuilder()
                                 .setReActorName("Worker")
                                 .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(GreeterService.GreetingsRequest.class,
                                   (raCtx, greetingsRequest) ->
                                           raCtx.logInfo("{} intercepted {}",
                                                         raCtx.getSelf().getReActorId().getReActorName(),
                                                         greetingsRequest))
                            .build();
        }
    }
}
