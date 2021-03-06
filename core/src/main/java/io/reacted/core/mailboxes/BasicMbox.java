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
import io.reacted.patterns.Try;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;

@NonNullByDefault
public class BasicMbox implements MailBox {

    private final Deque<Message> inbox;

    public BasicMbox() { this.inbox = new LinkedBlockingDeque<>(); }

    @Override
    public boolean isEmpty() { return inbox.isEmpty(); }

    @Override
    public boolean isFull() { return false; }

    @Override
    public Message getNextMessage() { return inbox.removeFirst(); }

    @Override
    public long getMsgNum() { return inbox.size(); }

    @Override
    public long getMaxSize() { return Integer.MAX_VALUE; }

    @Override
    public DeliveryStatus deliver(Message message) {
        return inbox.offerLast(message)
                ? DeliveryStatus.DELIVERED
                : DeliveryStatus.BACKPRESSURED;
    }

    @Override
    public CompletionStage<Try<DeliveryStatus>> asyncDeliver(Message message) {
        return CompletableFuture.completedFuture(Try.ofSuccess(deliver(message)));
    }
}
