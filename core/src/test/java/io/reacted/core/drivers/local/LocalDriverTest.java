/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.CoreConstants;
import io.reacted.core.ReactorHelper;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SniffSubscription;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

class LocalDriverTest {
    static ReActorSystem reActorSystem;
    static BasicMbox basicMbox;
    static Message originalMsg;
    static LocalDriver localDriver;
    static ReActorContext reActorContext;

    @BeforeAll
    static void prepareLocalDriver() throws Exception {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.RE_ACTED_ACTOR_SYSTEM)
                                                                     .setMsgFanOutPoolSize(1)
                                                                     .setRecordExecution(false)
                                                                     .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                                     .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                          .setDispatcherName("Dispatcher")
                                                                                                          .setBatchSize(1_000)
                                                                                                          .setDispatcherThreadsNum(1)
                                                                                                          .build())
                                                                     .build();
        reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();

        basicMbox = new BasicMbox();
        localDriver = SystemLocalDrivers.DIRECT_COMMUNICATION;
        localDriver.initDriverLoop(reActorSystem);

        SniffSubscription subscribedTypes = SubscriptionPolicy.LOCAL.forType(Message.class);
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName(CoreConstants.REACTOR_NAME)
                                                   .setDispatcherName("Dispatcher")
                                                   .setMailBoxProvider(ctx -> new BasicMbox())
                                                   .setTypedSniffSubscriptions(subscribedTypes)
                                                   .build();

        ReActorRef reActorRef = reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig))
                                             .orElseSneakyThrow();

        reActorSystem.registerReActorSystemDriver(localDriver);


        reActorContext = ReActorContext.newBuilder()
                                       .setMbox(ctx -> basicMbox)
                                       .setReactorRef(reActorRef)
                                       .setReActorSystem(reActorSystem)
                                       .setParentActor(ReActorRef.NO_REACTOR_REF)
                                       .setInterceptRules(subscribedTypes)
                .setDispatcher(mock(Dispatcher.class))
                .setReActions(mock(ReActions.class))
                .build();

        originalMsg = new Message(ReActorRef.NO_REACTOR_REF, reActorRef, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                AckingPolicy.NONE, CoreConstants.DE_SERIALIZATION_SUCCESSFUL);
    }

    @Test
    void localDriverDeliversMessagesInMailBox() {
        Assertions.assertTrue(localDriver.sendMessage(reActorContext, originalMsg).isSuccess());

        Assertions.assertEquals(1, basicMbox.getMsgNum());
        Assertions.assertEquals(originalMsg, basicMbox.getNextMessage());
        Assertions.assertEquals(0, basicMbox.getMsgNum());
    }

    @Test
    void localDriverForwardsMessageToLocalActor() {
        Assertions.assertTrue(LocalDriver.forwardMessageToLocalActor(reActorContext, originalMsg)
                                         .toCompletableFuture().join().isSuccess());
        Assertions.assertEquals(originalMsg, basicMbox.getNextMessage());
    }

    @Test
    void localDriverCanSendMessage() {
        Assertions.assertTrue(localDriver.sendMessage(reActorContext, originalMsg).isSuccess());
        Assertions.assertEquals(originalMsg, basicMbox.getNextMessage());
    }
}
