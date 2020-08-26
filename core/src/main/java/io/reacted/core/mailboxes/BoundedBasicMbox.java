/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import io.reacted.core.config.ConfigUtils;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;

@NonNullByDefault
public class BoundedBasicMbox implements MailBox {
    private final LinkedBlockingDeque<Message> inbox;
    private final int mailboxCapacity;

    public BoundedBasicMbox(int maxMsgs) {
        this.mailboxCapacity = ConfigUtils.requiredInRange(maxMsgs, 1, Integer.MAX_VALUE,
                                                           IllegalArgumentException::new);
        this.inbox = new LinkedBlockingDeque<>(this.mailboxCapacity);
    }

    @Override
    public boolean isEmpty() { return this.inbox.isEmpty(); }

    @Override
    public long getMsgNum() { return this.inbox.size(); }

    @Override
    public long getMaxSize() { return this.mailboxCapacity; }

    @Nonnull
    @Override
    public Message getNextMessage() { return this.inbox.removeFirst(); }

    @Override
    public boolean isFull() {
        return this.inbox.remainingCapacity() == this.mailboxCapacity;
    }

    @Override
    public DeliveryStatus deliver(Message message) {
        return this.inbox.offerLast(message) ? DeliveryStatus.DELIVERED : DeliveryStatus.BACKPRESSURED;
    }

    @Override
    public CompletionStage<Try<DeliveryStatus>> asyncDeliver(Message message) {
        return CompletableFuture.completedFuture(Try.ofSuccess(deliver(message)));
    }
}
