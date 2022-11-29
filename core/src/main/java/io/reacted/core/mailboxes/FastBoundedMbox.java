/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
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
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

import javax.annotation.Nonnull;
import java.util.Objects;

@NonNullByDefault
public class FastBoundedMbox implements MailBox {
    private final ManyToManyConcurrentArrayQueue<Message> inbox;
    private final int mailboxCapacity;

    public FastBoundedMbox(int maxMsgs) {
        this.mailboxCapacity = ObjectUtils.requiredInRange(maxMsgs, 1, Integer.MAX_VALUE,
                                                           IllegalArgumentException::new);
        this.inbox = new ManyToManyConcurrentArrayQueue<>(mailboxCapacity);
    }

    @Override
    public boolean isEmpty() { return inbox.isEmpty() || inbox.peek() == null; }

    @Override
    public long getMsgNum() { return inbox.size(); }

    @Override
    public long getMaxSize() { return inbox.capacity(); }

    @Nonnull
    @Override
    public Message getNextMessage() { return Objects.requireNonNull(inbox.poll()); }

    @Override
    public boolean isFull() { return inbox.remainingCapacity() == mailboxCapacity; }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        return inbox.offer(message) ? DeliveryStatus.DELIVERED : DeliveryStatus.NOT_DELIVERED;
    }
}
