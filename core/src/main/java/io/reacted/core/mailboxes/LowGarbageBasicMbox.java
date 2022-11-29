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
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@NonNullByDefault
public class LowGarbageBasicMbox implements MailBox {
    private final ManyToOneConcurrentLinkedQueue<Message> inbox = new ManyToOneConcurrentLinkedQueue<>();
    @Override
    public boolean isEmpty() { return inbox.isEmpty() || inbox.peek() == null; }

    @Override
    public boolean isFull() { return false; }

    @Nonnull
    @Override
    public Message getNextMessage() { return Objects.requireNonNull(inbox.poll()); }

    @Override
    public long getMsgNum() { return inbox.size(); }

    @Override
    public long getMaxSize() { return Integer.MAX_VALUE; }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        return inbox.offer(message)
                ? DeliveryStatus.DELIVERED
                : DeliveryStatus.NOT_DELIVERED;
    }
}
