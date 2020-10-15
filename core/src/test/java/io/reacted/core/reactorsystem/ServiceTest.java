/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.CoreConstants;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.services.Service;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceTest {
    private static ReActorSystem reActorSystem;
    private static ServiceConfig serviceConfig;

    @BeforeEach
    void setUp() {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                                                                     .setMsgFanOutPoolSize(2)
                                                                     .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                                     .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                          .setDispatcherName("TestDispatcher")
                                                                                                          .setBatchSize(1_000)
                                                                                                          .setDispatcherThreadsNum(1)
                                                                                                          .build())
                                                                     .build();
        reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();

        serviceConfig = ServiceConfig.newBuilder()
                                     .setLoadBalancingPolicy(Service.LoadBalancingPolicy.ROUND_ROBIN)
                                     .setMailBoxProvider(ctx -> new BasicMbox())
                                     .setReActorName("TestRouter")
                                     .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                     .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                     .setRouteesNum(1)
                                     .setRouteeProvider(() -> new MagicTestReActor(2, true,
                                                                                                 "RouterChild"))
                                     .build();
    }

    @AfterEach
    void tearDown() {
        reActorSystem.shutDown();
    }

    @Test
    void reactorSystemSpawnServiceIsSuccessful() {
        Assertions.assertTrue(reActorSystem.spawnService(serviceConfig).isSuccess());
    }

    @Test
    void reactorSystemCanSpawnService() {
        ReActorRef router = reActorSystem.spawnService(serviceConfig).orElseSneakyThrow();
        Assertions.assertEquals(router, reActorSystem.getReActor(router.getReActorId())
                                                     .map(ReActorContext::getSelf)
                                                     .orElse(ReActorRef.NO_REACTOR_REF));
    }
}