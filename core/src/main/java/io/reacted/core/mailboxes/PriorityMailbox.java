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

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.PriorityBlockingQueue;
import javax.annotation.Nonnull;

@NonNullByDefault
public class PriorityMailbox implements MailBox {
    private final PriorityBlockingQueue<Message> mailBox;

    public PriorityMailbox() {
        this(Comparator.comparingLong(Message::getSequenceNumber));
    }

    public PriorityMailbox(Comparator<? super Message> msgComparator) {
        this.mailBox = new PriorityBlockingQueue<>(10, Objects.requireNonNull(msgComparator));
    }

    @Override
    public boolean isEmpty() { return mailBox.isEmpty(); }

    @Override
    public boolean isFull() { return false; }

    @Override
    public long getMsgNum() { return mailBox.size(); }

    @Override
    public long getMaxSize() { return Integer.MAX_VALUE; }

    @Nonnull
    @Override
    public Message getNextMessage() { return Objects.requireNonNull(mailBox.poll()); }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        return mailBox.add(message) ? DeliveryStatus.DELIVERED : DeliveryStatus.NOT_DELIVERED;
    }
}
