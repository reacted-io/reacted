/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend;

import com.mongodb.reactivestreams.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ServiceConfig;
import io.reacted.core.services.Service;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.examples.webappbackend.db.DatabaseService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class Backend {
    public static final String DB_SERVICE_NAME = "StorageGate";

    public static void main(String[] args) throws IOException {
        var chronicleDriverConfig = CQDriverConfig.newBuilder()
                                                  .setChronicleFilesDir("/tmp/replayable_server")
                                                  .setChannelName("LocalChannel")
                                                  .setTopicName("TestSession")
                                                  .setChannelRequiresDeliveryAck(true)
                                                  .build();

        ReActorSystem backendSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                           .setLocalDriver(new CQLocalDriver(chronicleDriverConfig))
                                                                           .setReactorSystemName("BackendSystem")
                                                                           .setRecordExecution(true)
                                                                           .build()).initReActorSystem();

        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);

        backendSystem.spawnService(ServiceConfig.newBuilder()
                                                .setRouteesNum(2)
                                                .setTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class))
                                                .setReActorName(DB_SERVICE_NAME)
                                                .setLoadBalancingPolicy(Service.LoadBalancingPolicy.LOWEST_LOAD)
                                                .setRouteeProvider(() -> new DatabaseService(MongoClients.create()))
                                                .build());
        backendSystem.spawn(new ServerGate(server, Executors.newSingleThreadExecutor(),
                                           Executors.newSingleThreadExecutor()));
    }
}