package io.reacted.core;

import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactorsystem.ReActorRef;

public class MessageHelper {
    public static Message getDefaultMessage() {
        return new Message(ReActorRef.NO_REACTOR_REF, ReActorRef.NO_REACTOR_REF, 0x31337,
                           ReactorHelper.TEST_REACTOR_SYSTEM_ID, AckingPolicy.NONE,
                           CoreConstants.DE_SERIALIZATION_SUCCESSFUL);
    }
}
