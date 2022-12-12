/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import io.reacted.core.CoreConstants;
import io.reacted.core.ReactorHelper;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactorsystem.ReActorRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnboundedMboxTest {
    UnboundedMbox unboundedMbox;
    static Message originalMsg;
    static ReActorRef testMsgSrc;
    static ReActorRef testMsgDst;

    @BeforeAll
    static void prepareMessages() {
        testMsgSrc = ReactorHelper.generateReactor(CoreConstants.SOURCE);
        testMsgDst = ReactorHelper.generateReactor(CoreConstants.DESTINATION);

        originalMsg = Message.forParams(testMsgSrc, testMsgDst, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                                        AckingPolicy.NONE, CoreConstants.DE_SERIALIZATION_SUCCESSFUL);
    }

    @BeforeEach
    void prepareMailBox() {
        unboundedMbox = new UnboundedMbox();
    }

    @Test
    void messageGetsDeliveredInInbox() {
        unboundedMbox.deliver(originalMsg);
        Assertions.assertFalse(unboundedMbox.isEmpty());
    }

    @Test
    void messagesAreCorrectlyCountedInInbox() {
        unboundedMbox.deliver(originalMsg);
        Assertions.assertEquals(1, unboundedMbox.getMsgNum());
    }

    @Test
    void inboxWithOneMessageIsNotFull() {
        unboundedMbox.deliver(originalMsg);
        Assertions.assertFalse(unboundedMbox.isFull());
    }

    @Test
    void inboxWithOneMessageIsNotEmpty() {
        unboundedMbox.deliver(originalMsg);
        Assertions.assertFalse(unboundedMbox.isEmpty());
    }

    @Test
    void inboxWithNoMessagesIsEmpty() {
        Assertions.assertTrue(unboundedMbox.isEmpty());
    }

    @Test
    void getNextMessageReturnsSentMessage() {
        unboundedMbox.deliver(originalMsg);
        Assertions.assertEquals(
                testMsgDst.toString(), unboundedMbox.getNextMessage().getDestination().toString());
    }

    @Test
    void getNextMessageReturnsMessagesInTheOrderTheyWereSent() {
        unboundedMbox.deliver(originalMsg);

        ReActorRef testMsgSrc2 = ReactorHelper.generateReactor("source2");
        ReActorRef testMsgDst2 = ReactorHelper.generateReactor("destination2");

        Message originalMsg2 = Message.forParams(testMsgSrc2, testMsgDst2, 0x31337,
                                                 ReactorHelper.TEST_REACTOR_SYSTEM_ID, AckingPolicy.NONE, "De/Serialization Successful!");

        unboundedMbox.deliver(originalMsg2);
        Assertions.assertEquals(2, unboundedMbox.getMsgNum());

        Assertions.assertEquals(
                testMsgDst.toString(), unboundedMbox.getNextMessage().getDestination().toString());
        Assertions.assertEquals(
                testMsgDst2.toString(), unboundedMbox.getNextMessage().getDestination().toString());
    }
}
