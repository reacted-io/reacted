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
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.drivers.channels.kafka.KafkaDriver;
import io.reacted.drivers.channels.kafka.KafkaDriverConfig;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;
import io.reacted.patterns.NonNullByDefault;
import java.io.FileNotFoundException;
import java.time.Duration;

@NonNullByDefault
public class QuickstartServer {
    public static void main(String[] args) throws FileNotFoundException {
        var zooKeeperDriverConfig = ZooKeeperDriverConfig.newBuilder()
                                                         .setReActorName("LocalhostCluster")
                                                         .build();
        var kafkaDriverConfig = KafkaDriverConfig.newBuilder()
                                                 .setChannelName("KafkaQuickstartChannel")
                                                 .setTopic("ReactedTopic")
                                                 .setGroupId("QuickstartGroupServer")
                                                 .setMaxPollRecords(1)
                                                 .setBootstrapEndpoint("localhost:9092")
                                                 .build();
        var showOffServerSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       .setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/server"))
                                                                       .addServiceRegistryDriver(new ZooKeeperDriver(zooKeeperDriverConfig))
                                                                       .addRemotingDriver(new KafkaDriver(kafkaDriverConfig))
                                                                       .setReactorSystemName("ShowOffServerReActorSystemName")
                                                                       .setSystemMonitorRefreshInterval(Duration.ofMinutes(1))
                                                                       .build()).initReActorSystem();
        try {
            String serviceName = "Greetings";
            showOffServerSystem.spawnService(ServiceConfig.newBuilder()
                                                          .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                       .setRealMailboxOwner(ctx)
                                                                                                       .build())
                                                          .setReActorName(serviceName)
                                                          .setRouteesNum(2)
                                                          .setRouteeProvider(GreeterService::new)
                                                          .setIsRemoteService(true)
                                                          .build())
                               .orElseSneakyThrow();
        } catch (Exception anyException) {
            anyException.printStackTrace();
            showOffServerSystem.shutDown();
        }
    }
}
