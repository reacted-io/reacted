/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. 
 */

package io.reacted.core.drivers.system;

import io.reacted.core.CoreConstants;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.drivers.ChannelDriverCfg;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

class LoopbackDriverTest {
    static String testDispatcher = "TestDispatcher";
    static ReActorSystem reActorSystem;
    static LoopbackDriver<? extends ChannelDriverCfg<?, ?>> loopbackDriver;
    static ReActorRef destReActorRef;
    static Message message;

    @BeforeEach
    void prepareEnvironment() throws Exception {
        // Prepare & init ReActorSystem
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                                                                     .setMsgFanOutPoolSize(1)
                                                                     .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                                     .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                          .setDispatcherName(testDispatcher)
                                                                                                          .setBatchSize(1_000)
                                                                                                          .setDispatcherThreadsNum(1)
                                                                                                          .build())
                                                                     .build();
        reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();

        loopbackDriver = new LoopbackDriver(reActorSystem, SystemLocalDrivers.DIRECT_COMMUNICATION);
        loopbackDriver.initDriverLoop(reActorSystem);

        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName("ReActorName")
                                                   .setDispatcherName(testDispatcher)
                                                   .setTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(Message.class))
                                                   .build();

        destReActorRef = reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig))
                                      .orElseSneakyThrow();
        message = new Message(ReActorRef.NO_REACTOR_REF, destReActorRef, 0, reActorSystem.getLocalReActorSystemId(),
                              AckingPolicy.NONE, "payload");
    }

    @AfterEach
    public void cleanup() { reActorSystem.shutDown(); }

    @Test
    void loopbackDriverCanTellMessageToReactor() throws ExecutionException, InterruptedException {
        Assertions.assertTrue(loopbackDriver.tell(ReActorRef.NO_REACTOR_REF, destReActorRef, AckingPolicy.NONE, message)
                                            .toCompletableFuture()
                                            .get()
                                            .isSuccess());
        // as we have a dispatcher the message was dispatched & it cannot be found in destination mbox
        Awaitility.await().until(() -> MagicTestReActor.RECEIVED.sum() == 1);
    }

    @Test
    void loopbackDriverChannelIdIsTheOneSetInLocalDriver() {
        Assertions.assertEquals(SystemLocalDrivers.DIRECT_COMMUNICATION.getChannelId(), loopbackDriver.getChannelId());
    }
}