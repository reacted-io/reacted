/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import static io.reacted.core.CoreConstants.PRIORITY_2;

import io.reacted.core.CoreConstants;
import io.reacted.core.ReactorHelper;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactorsystem.ReActorRef;
import java.util.Comparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PriorityMailboxTest {

    PriorityMailbox priorityMailbox;
    static Message originalMsg;
    static ReActorRef testMsgSrc;
    static ReActorRef testMsgDst;

    @BeforeAll
    static void prepareMessages() {
        testMsgSrc = ReactorHelper.generateReactor(CoreConstants.SOURCE);
        testMsgDst = ReactorHelper.generateReactor(CoreConstants.DESTINATION);

        originalMsg = new Message(testMsgSrc, testMsgDst, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                AckingPolicy.NONE, "De/Serialization Successful!");
    }

    @BeforeEach
    void prepareMailBox() {
        priorityMailbox = new PriorityMailbox();
    }

    @Test
    void deliveredMessageEqualsProvidedMessage() {
        priorityMailbox.deliver(originalMsg);
        Assertions.assertEquals(originalMsg, priorityMailbox.getNextMessage());
    }

    @Test
    void messageWithHigherPriorityProvidedFirst() {
        PriorityMailbox priorityMailbox1 = new PriorityMailbox(PAYLOAD_COMPARATOR);

        Message message1 = createMessage(CoreConstants.LOW_PRIORITY);
        Message message2 = createMessage(CoreConstants.HIGH_PRIORITY);
        priorityMailbox1.deliver(message1);
        priorityMailbox1.deliver(message2);

        Assertions.assertEquals(message2, priorityMailbox1.getNextMessage());
    }

    @Test
    void messageWithHigherPriorityProvidedFirstWhenSentFirst() {
        PriorityMailbox priorityMailbox1 = new PriorityMailbox(PAYLOAD_COMPARATOR);

        Message message1 = createMessage(CoreConstants.HIGH_PRIORITY);
        Message message2 = createMessage(CoreConstants.LOW_PRIORITY);
        Message message3 = createMessage(CoreConstants.LOW_PRIORITY);
        priorityMailbox1.deliver(message1);
        priorityMailbox1.deliver(message2);
        priorityMailbox1.deliver(message3);

        Assertions.assertEquals(message1, priorityMailbox1.getNextMessage());
    }

    @Test
    void messageWithHigherPriorityProvidedFirstThenNormalPriorityApplied() {
        PriorityMailbox priorityMailbox1 = new PriorityMailbox(PAYLOAD_COMPARATOR);

        Message message1 = createMessage(CoreConstants.LOW_PRIORITY);
        Message message2 = createMessage(CoreConstants.LOW_PRIORITY);
        Message message3 = createMessage(CoreConstants.HIGH_PRIORITY);

        ReActorRef highPrioMsgSrc2 =
                ReactorHelper.generateReactor(CoreConstants.SOURCE + PRIORITY_2);
        ReActorRef highPrioMsgDest2 =
                ReactorHelper.generateReactor(CoreConstants.DESTINATION + PRIORITY_2);

        Message message4 =
                new Message(highPrioMsgSrc2, highPrioMsgDest2, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                        AckingPolicy.NONE, CoreConstants.HIGH_PRIORITY);

        priorityMailbox1.deliver(message1);
        priorityMailbox1.deliver(message2);
        priorityMailbox1.deliver(message3);
        priorityMailbox1.deliver(message4);

        Assertions.assertEquals(message3, priorityMailbox1.getNextMessage());
        Assertions.assertEquals(message4, priorityMailbox1.getNextMessage());
        Assertions.assertEquals(message1, priorityMailbox1.getNextMessage());
        Assertions.assertEquals(message2, priorityMailbox1.getNextMessage());
    }

    private Message createMessage(String payload) {
        return new Message(testMsgSrc, testMsgDst, 0x31337, ReactorHelper.TEST_REACTOR_SYSTEM_ID,
                AckingPolicy.NONE, payload);
    }

    private final static Comparator<? super Message> PAYLOAD_COMPARATOR =
            (Comparator<Message>)
                    (message1, message2) -> {
                        String payloadSize1 = message1.getPayload().toString();
                        String payloadSize2 = message2.getPayload().toString();

                        return payloadSize1.compareTo(payloadSize2);
                    };
}
