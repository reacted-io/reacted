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

import javax.annotation.Nonnull;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@NonNullByDefault
public class UnboundedMbox implements MailBox {
    private final BlockingDeque<Message> inbox = new LinkedBlockingDeque<>();
    @Override
    public boolean isEmpty() { return inbox.isEmpty(); }

    @Override
    public boolean isFull() { return false; }

    @Nonnull
    @Override
    public Message getNextMessage() { return inbox.removeFirst(); }

    @Override
    public long getMsgNum() { return inbox.size(); }

    @Override
    public long getMaxSize() { return Integer.MAX_VALUE; }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        return inbox.offerLast(message)
                ? DeliveryStatus.DELIVERED
                : DeliveryStatus.NOT_DELIVERED;
    }
}
