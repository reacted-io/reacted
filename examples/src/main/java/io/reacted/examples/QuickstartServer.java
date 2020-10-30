/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ServiceConfig;
import io.reacted.drivers.channels.kafka.KafkaDriver;
import io.reacted.drivers.channels.kafka.KafkaDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;

import javax.annotation.Nonnull;
import java.io.Serializable;

public class QuickstartServer {
    public static void main(String[] args) {
        var zooKeeperDriverConfig = ZooKeeperDriverConfig.newBuilder()
                                                         .setReActorName("LocalhostCluster")
                                                         .build();
        var kafkaDriverConfig = KafkaDriverConfig.newBuilder()
                                                 .setChannelName("KafkaQuickstartChannel")
                                                 .setTopic("ReactedTopic")
                                                 .setGroupId("QuickstartGroup")
                                                 .setMaxPollRecords(1)
                                                 .setBootstrapEndpoint("localhost:9092")
                                                 .build();
        ReActorSystem showOffSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                           .addServiceRegistryDriver(new ZooKeeperDriver(zooKeeperDriverConfig))
                                                                           .addRemotingDriver(new KafkaDriver(kafkaDriverConfig))
                                                                           .setReactorSystemName("ShowOffReActorSystenName")
                                                                           .build()).initReActorSystem();
        try {
            String serviceName = "Greetings";
            showOffSystem.spawnService(ServiceConfig.newBuilder()
                                                    .setReActorName(serviceName)
                                                    .setRouteesNum(2)
                                                    .setRouteeProvider(Greeter::new)
                                                    .setIsRemoteService(true)
                                                    .build())
                         .orElseSneakyThrow();

            var searchFilter = BasicServiceDiscoverySearchFilter.newBuilder()
                                                                .setServiceName(serviceName)
                                                                .build();
            var serviceDiscoveryReply = showOffSystem.serviceDiscovery(searchFilter)
                                                     .toCompletableFuture()
                                                     .join()
                                                     .orElseSneakyThrow();

            if (serviceDiscoveryReply.getServiceGates().isEmpty()) {
                showOffSystem.logInfo("No services found, exiting");
            } else {
                var serviceGate = serviceDiscoveryReply.getServiceGates().iterator().next();
                serviceGate.ask(new GreetingsRequest(), String.class, "Request to service")
                           .toCompletableFuture()
                           .join()
                           .ifSuccessOrElse(showOffSystem::logInfo, Throwable::printStackTrace);
            }
        } catch (Exception anyException) {
            anyException.printStackTrace();
        }
        showOffSystem.shutDown();
    }

    private static final class Greeter implements ReActor {

        @Nonnull
        @Override
        public ReActorConfig getConfig() {
            return ReActorConfig.newBuilder()
                                .setReActorName("Worker")
                                .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class,
                                   ((reActorContext, init) -> reActorContext.logInfo("A reactor was born")))
                            .reAct(GreetingsRequest.class,
                                   ((raCtx, greetingsRequest) -> raCtx.reply("Hello from " +
                                                                             Greeter.class.getSimpleName())))
                            .build();
        }
    }
    private static final class GreetingsRequest implements Serializable { }
}