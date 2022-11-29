/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.services.LoadBalancingPolicies;
import io.reacted.drivers.channels.grpc.GrpcDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriver;
import io.reacted.drivers.serviceregistries.zookeeper.ZooKeeperDriverConfig;
import io.reacted.examples.ExampleUtils;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ServicePublicationApp {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        Properties serviceRegistryProperties = new Properties();
        //The two reactor system will discover each other automatically using this service registry
        var serverReActorSystem = "SERVER_REACTORSYSTEM";
        var serverGatePort = 12345;
        var clientReActorSystem = "CLIENT_REACTORSYSTEM";
        var clientGatePort = 54321;
        var serverSystemCfg = ExampleUtils.getDefaultReActorSystemCfg(serverReActorSystem,
                                                                      SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/server"),
                                                                      List.of(new ZooKeeperDriver(ZooKeeperDriverConfig.newBuilder()
                                                                                                                       .setServiceRegistryProperties(serviceRegistryProperties)
                                                                                                                       .setReActorName("ZooKeeperDriver")
                                                                                                                       .build())),
                                                                      List.of(new GrpcDriver(ExampleUtils.getGrpcDriverCfg(serverGatePort))));

        var clientSystemCfg = ExampleUtils.getDefaultReActorSystemCfg(clientReActorSystem,
                                                                      SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver("/tmp/client"),
                                                                      List.of(new ZooKeeperDriver(ZooKeeperDriverConfig.newBuilder()
                                                                                                                       .setServiceRegistryProperties(serviceRegistryProperties)
                                                                                                                       .setReActorName("ZooKeeperDriver")
                                                                                                                       .build())),
                                                                      List.of(new GrpcDriver(ExampleUtils.getGrpcDriverCfg(clientGatePort))));

        var server = new ReActorSystem(serverSystemCfg).initReActorSystem();
        var client = new ReActorSystem(clientSystemCfg).initReActorSystem();

        var serviceName = "ClockService";
        //For simplicity let's use the default dispatcher. A new one could and should
        //be used for load partitioning
        var serviceDispatcherName = Dispatcher.DEFAULT_DISPATCHER_NAME;
        //Now let's publish a service in server reactor system
        var serviceCfg = ServiceConfig.newBuilder()
                                      .setReActorName(serviceName)
                                      //Two workers for service will be created for load balancing reasons
                                      .setRouteesNum(2)
                                      //For every new request select a different worker instance
                                      .setLoadBalancingPolicy(LoadBalancingPolicies.ROUND_ROBIN)
                                      .setDispatcherName(serviceDispatcherName)
                                      //Let's assume that we do not need any form of backpressure
                                      .setMailBoxProvider(ctx -> new UnboundedMbox())
                                      //We do not need to listen for ServiceDiscoveryRequests, we have the
                                      //Service Registry now
                                      .setRouteeProvider(serviceConfig -> new ClockReActor(serviceName))
                                      .setIsRemoteService(true)
                                      .build();

        //Create a service. It will be published automatically on the service registry
        server.spawnService(serviceCfg).orElseSneakyThrow();
        //Give some time for the service propagation
        TimeUnit.SECONDS.sleep(10);
        //Create a reactor in CLIENT reactor system that will query the service exported in SERVER
        //All the communication between the two reactor systems will be done using a GRPC channel
        client.spawn(new TimeReActor(serviceName)).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(10);
        System.out.println("Shutting down...");
        server.shutDown();
        client.shutDown();
    }
}
