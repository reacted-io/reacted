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
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class ReActorServiceTest {
    private static ReActorSystem reActorSystem;
    private static ReActorServiceConfig reActorServiceConfig;

    @BeforeEach
    void setUp() {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.RE_ACTED_ACTOR_SYSTEM)
                                                                     .setAskTimeoutsCleanupInterval(Duration.ofSeconds(10))
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

        reActorServiceConfig = ReActorServiceConfig.newBuilder()
                                                   .setSelectionPolicy(ReActorService.LoadBalancingPolicy.ROUND_ROBIN)
                                                   .setMailBoxProvider(BasicMbox::new)
                                                   .setReActorName("TestRouter")
                                                   .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                                   .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
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
        Assertions.assertTrue(reActorSystem.spawnService(reActorServiceConfig).isSuccess());
    }

    @Test
    void reactorSystemCanSpawnService() {
        ReActorRef router = reActorSystem.spawnService(reActorServiceConfig).orElseSneakyThrow();
        Assertions.assertEquals(router, reActorSystem.getReActor(router.getReActorId()).get().getSelf());
    }
}