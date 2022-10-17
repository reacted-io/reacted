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
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;

import javax.annotation.Nonnull;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@NonNullByDefault
public class BoundedBasicMbox implements MailBox {
    private final BlockingQueue<Message> inbox;
    private final int mailboxCapacity;

    public BoundedBasicMbox(int maxMsgs) {
        this.mailboxCapacity = ObjectUtils.requiredInRange(maxMsgs, 1, Integer.MAX_VALUE,
                                                           IllegalArgumentException::new);
        this.inbox = new ArrayBlockingQueue<>(mailboxCapacity);
    }

    @Override
    public boolean isEmpty() { return inbox.isEmpty(); }

    @Override
    public long getMsgNum() { return inbox.size(); }

    @Override
    public long getMaxSize() { return mailboxCapacity; }

    @Nonnull
    @Override
    public Message getNextMessage() { return inbox.remove(); }

    @Override
    public boolean isFull() { return inbox.remainingCapacity() == mailboxCapacity; }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        return inbox.offer(message) ? DeliveryStatus.DELIVERED : DeliveryStatus.NOT_DELIVERED;
    }
}
