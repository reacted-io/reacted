/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import com.google.common.base.Strings;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactorsystem.ReActorService;
import io.reacted.core.reactorsystem.ReActorServiceConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.grpc.GrpcDriver;
import io.reacted.drivers.serviceregistries.ZooKeeperDriver;
import io.reacted.examples.ExampleUtils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ServicePublicationApp {
    public static void main(String[] args) throws InterruptedException {
        String zookeeperConnectionString = args.length == 0 || Strings.isNullOrEmpty(args[0])
                                           ? "localhost:2181" : args[0];
        Properties serviceRegistryProperties = new Properties();
        //The two reactor system will discover each other automatically using this service registry
        serviceRegistryProperties.setProperty(ZooKeeperDriver.ZK_CONNECTION_STRING, zookeeperConnectionString);
        var serverReActorSystem = "SERVER_REACTORSYSTEM";
        var serverGatePort = 12345;
        var clientReActorSystem = "CLIENT_REACTORSYSTEM";
        var clientGatePort = 54321;
        var serverSystemCfg = ExampleUtils.getDefaultReActorSystemCfg(serverReActorSystem,
                                                                      SystemLocalDrivers.DIRECT_COMMUNICATION,
                                                                      List.of(new ZooKeeperDriver(serviceRegistryProperties)),
                                                                      List.of(new GrpcDriver(ExampleUtils.getGrpcDriverCfg(serverGatePort))));

        var clientSystemCfg = ExampleUtils.getDefaultReActorSystemCfg(clientReActorSystem,
                                                                      SystemLocalDrivers.DIRECT_COMMUNICATION,
                                                                      List.of(new ZooKeeperDriver(serviceRegistryProperties)),
                                                                      List.of(new GrpcDriver(ExampleUtils.getGrpcDriverCfg(clientGatePort))));

        var server = new ReActorSystem(serverSystemCfg).initReActorSystem();
        var client = new ReActorSystem(clientSystemCfg).initReActorSystem();

        var serviceName = "ClockService";
        //For simplicity let's use the default dispatcher. A new one could and should
        //be used for load partitioning
        var serviceDisparcherName = ReActorSystem.DEFAULT_DISPATCHER_NAME;
        //Now let's publish a service in server reactor system
        var serviceCfg = ReActorServiceConfig.newBuilder()
                                             .setReActorName(serviceName)
                                             //Two workers for service will be created for load balancing reasons
                                             .setRouteesNum(2)
                                             //For every new request select a different worker instance
                                             .setSelectionPolicy(ReActorService.LoadBalancingPolicy.ROUND_ROBIN)
                                             .setDispatcherName(serviceDisparcherName)
                                             //Let's assume that we do not need any form of backpressure
                                             .setMailBoxProvider(BasicMbox::new)
                                             //We do not need to listen for ServiceDiscoveryRequests, we have the
                                             //Service Registry now
                                             .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                                             .setRouteeProvider(() -> new ClockReActor(serviceDisparcherName))
                                             .build();

        //Create a service. It will be published automatically on the service registry
        server.spawnService(serviceCfg).orElseSneakyThrow();
        //Give some time for the service propagation
        TimeUnit.SECONDS.sleep(2);
        //Create a reactor in CLIENT reactor system that will query the service exported in SERVER
        //All the communication between the two reactor systems will be done using a GRPC channel
        client.spawnReActor(new TimeReActor(serviceName, "1")).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(2);
        server.shutDown();
    }
}
