/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.quickstart;

import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.kafka.KafkaDriver;
import io.reacted.drivers.channels.kafka.KafkaDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class QuickstartClient {
    public static void main(String[] args) {
        var zooKeeperDriverConfig = ZooKeeperDriverConfig.newBuilder()
                                                         .setReActorName("LocalhostCluster")
                                                         .build();
        var kafkaDriverConfig = KafkaDriverConfig.newBuilder()
                                                 .setChannelName("KafkaQuickstartChannel")
                                                 .setTopic("ReactedTopic")
                                                 .setGroupId("QuickstartGroupClient")
                                                 .setMaxPollRecords(1)
                                                 .setBootstrapEndpoint("localhost:9092")
                                                 .build();
        var showOffClientSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       .setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/client"))
                                                                       .addServiceRegistryDriver(new ZooKeeperDriver(zooKeeperDriverConfig))
                                                                       .addRemotingDriver(new KafkaDriver(kafkaDriverConfig))
                                                                       .setReactorSystemName("ShowOffClientReActorSystemName")
                                                                       .build()).initReActorSystem();
        String serviceName = "Greetings";
        var searchFilter = BasicServiceDiscoverySearchFilter.newBuilder()
                                                            .setServiceName(serviceName)
                                                            .build();
        var serviceDiscoveryReply = showOffClientSystem.serviceDiscovery(searchFilter)
                                                       .toCompletableFuture()
                                                       .join()
                                                       .orElseSneakyThrow();

        if (serviceDiscoveryReply.getServiceGates().isEmpty()) {
            showOffClientSystem.logInfo("No services found, exiting");
        } else {
            var serviceGate = serviceDiscoveryReply.getServiceGates().iterator().next();
            serviceGate.ask(new GreeterService.GreetingsRequest(), String.class, "Request to service")
                       .toCompletableFuture()
                       .join()
                       .ifSuccessOrElse(showOffClientSystem::logInfo, Throwable::printStackTrace);
        }
        showOffClientSystem.shutDown();
    }
}
