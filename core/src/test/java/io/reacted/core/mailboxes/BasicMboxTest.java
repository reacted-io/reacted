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

class BasicMboxTest {
    BasicMbox basicMbox;
    static Message originalMsg;
    static ReActorRef testMsgSrc;
    static ReActorRef testMsgDst;

    @BeforeAll
    static void prepareMessages() {
        testMsgSrc = ReactorHelper.generateReactor(CoreConstants.SOURCE);
        testMsgDst = ReactorHelper.generateReactor(CoreConstants.DESTINATION);

        originalMsg = new Message(testMsgSrc, testMsgDst, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                                    AckingPolicy.NONE, CoreConstants.DE_SERIALIZATION_SUCCESSFUL);
    }

    @BeforeEach
    void prepareMailBox() {
        basicMbox = new BasicMbox();
    }

    @Test
    void messageGetsDeliveredInInbox() {
        basicMbox.deliver(originalMsg);
        Assertions.assertFalse(basicMbox.isEmpty());
    }

    @Test
    void messagesAreCorrectlyCountedInInbox() {
        basicMbox.deliver(originalMsg);
        Assertions.assertEquals(1, basicMbox.getMsgNum());
    }

    @Test
    void inboxWithOneMessageIsNotFull() {
        basicMbox.deliver(originalMsg);
        Assertions.assertFalse(basicMbox.isFull());
    }

    @Test
    void inboxWithOneMessageIsNotEmpty() {
        basicMbox.deliver(originalMsg);
        Assertions.assertFalse(basicMbox.isEmpty());
    }

    @Test
    void inboxWithNoMessagesIsEmpty() {
        Assertions.assertTrue(basicMbox.isEmpty());
    }

    @Test
    void getNextMessageReturnsSentMessage() {
        basicMbox.deliver(originalMsg);
        Assertions.assertEquals(
                testMsgDst.toString(), basicMbox.getNextMessage().getDestination().toString());
    }

    @Test
    void getNextMessageReturnsMessagesInTheOrderTheyWereSent() {
        basicMbox.deliver(originalMsg);

        ReActorRef testMsgSrc2 = ReactorHelper.generateReactor("source2");
        ReActorRef testMsgDst2 = ReactorHelper.generateReactor("destination2");

        Message originalMsg2 =
                new Message(testMsgSrc2, testMsgDst2, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                              AckingPolicy.NONE, "De/Serialization Successful!");

        basicMbox.deliver(originalMsg2);
        Assertions.assertEquals(2, basicMbox.getMsgNum());

        Assertions.assertEquals(
                testMsgDst.toString(), basicMbox.getNextMessage().getDestination().toString());
        Assertions.assertEquals(
                testMsgDst2.toString(), basicMbox.getNextMessage().getDestination().toString());
    }
}
