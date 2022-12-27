/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
@NonNullByDefault
public class TypeCoalescingMailbox implements MailBox {
    private final Map<Class<? extends ReActedMessage>, Message> latestByPayloadType;
    private final Set<Class<? extends ReActedMessage>> pendingUpdatedTypes;
    private final Deque<Class<? extends ReActedMessage>> lastAdded;

    public TypeCoalescingMailbox() {
        this.latestByPayloadType = new HashMap<>();
        this.pendingUpdatedTypes = ConcurrentHashMap.newKeySet(100);
        this.lastAdded = new LinkedBlockingDeque<>();
    }

    @Override
    public boolean isEmpty() { return lastAdded.isEmpty(); }

    @Override
    public boolean isFull() { return false; }

    @Override
    public long getMsgNum() { return lastAdded.size(); }

    @Override
    public long getMaxSize() { return Long.MAX_VALUE; }

    @Nonnull
    @Override
    public Message getNextMessage() {
        var payloadType = this.lastAdded.removeLast();
        Message nextMessage;
        synchronized (payloadType) {
            this.pendingUpdatedTypes.remove(payloadType);
            nextMessage = this.latestByPayloadType.remove(payloadType);
        }
        return nextMessage;
    }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        var payloadType = message.getPayload().getClass();
        synchronized (payloadType) {
            this.latestByPayloadType.put(payloadType, message);
            if (this.pendingUpdatedTypes.add(payloadType)) {
                this.lastAdded.addFirst(payloadType);
            }
        }
        return DeliveryStatus.DELIVERED;
    }
}
