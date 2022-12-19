/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import static org.mockito.Mockito.mock;

import io.reacted.core.CoreConstants;
import io.reacted.core.ReactorHelper;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LocalDriverTest {
    static ReActorSystem reActorSystem;
    static UnboundedMbox unboundedMbox;
    static Message originalMsg;
    static LocalDriver localDriver;
    static ReActorContext reActorContext;

    @BeforeAll
    static void prepareLocalDriver() throws Exception {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
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

        unboundedMbox = new UnboundedMbox();
        localDriver = SystemLocalDrivers.DIRECT_COMMUNICATION;
        localDriver.initDriverLoop(reActorSystem);

        TypedSubscription subscribedTypes = TypedSubscription.LOCAL.forType(String.class);
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName(CoreConstants.REACTOR_NAME)
                                                   .setDispatcherName("Dispatcher")
                                                   .setMailBoxProvider(ctx -> new UnboundedMbox())
                                                   .setTypedSubscriptions(subscribedTypes)
                                                   .build();

        ReActorRef reActorRef = reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig))
                                             .orElseSneakyThrow();

        reActorSystem.registerReActorSystemDriver(localDriver);


        reActorContext = ReActorContext.newBuilder()
                                       .setMbox(ctx -> unboundedMbox)
                                       .setReactorRef(reActorRef)
                                       .setReActorSystem(reActorSystem)
                                       .setParentActor(ReActorRef.NO_REACTOR_REF)
                                       .setSubscriptions(subscribedTypes)
                .setDispatcher(mock(Dispatcher.class))
                .setReActions(mock(ReActions.class))
                .build();

        originalMsg = Message.of(ReActorRef.NO_REACTOR_REF, reActorRef, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                                 AckingPolicy.NONE, CoreConstants.DE_SERIALIZATION_SUCCESSFUL);
    }

    @Test
    void localDriverDeliversMessagesInMailBox() {
        Assertions.assertTrue(localDriver.sendMessage(originalMsg.getSender(), reActorContext,
                                                      originalMsg.getDestination(),
                                                      originalMsg.getSequenceNumber(),
                                                      originalMsg.getCreatingReactorSystemId(),
                                                      originalMsg.getAckingPolicy(),
                                                      originalMsg.getPayload()).isDelivered());

        Assertions.assertEquals(1, unboundedMbox.getMsgNum());
        Assertions.assertEquals(originalMsg, unboundedMbox.getNextMessage());
        Assertions.assertEquals(0, unboundedMbox.getMsgNum());
    }

    @Test
    void localDriverForwardsMessageToLocalActor() {
        Assertions.assertTrue(LocalDriver.syncForwardMessageToLocalActor(originalMsg.getSender(), reActorContext,
                                                                         originalMsg.getDestination(),
                                                                         originalMsg.getSequenceNumber(),
                                                                         originalMsg.getCreatingReactorSystemId(),
                                                                         originalMsg.getAckingPolicy(),
                                                                         originalMsg.getPayload())
                                         .isDelivered());
        Assertions.assertEquals(originalMsg, unboundedMbox.getNextMessage());
    }

    @Test
    void localDriverCanSendMessage() {
        Assertions.assertTrue(localDriver.sendMessage(originalMsg.getSender(), reActorContext,
                                                      originalMsg.getDestination(),
                                                      originalMsg.getSequenceNumber(),
                                                      originalMsg.getCreatingReactorSystemId(),
                                                      originalMsg.getAckingPolicy(),
                                                      originalMsg.getPayload()).isDelivered());
        Assertions.assertEquals(originalMsg, unboundedMbox.getNextMessage());
    }
}
