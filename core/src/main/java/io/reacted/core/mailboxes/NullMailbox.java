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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class NullMailbox implements MailBox {

    @Override
    public boolean isEmpty() { return true; }

    @Override
    public boolean isFull() { return false; }

    @Override
    public Message getNextMessage() { throw new UnsupportedOperationException(); }

    @Override
    public long getMsgNum() { return 0; }

    @Override
    public long getMaxSize() { return Long.MAX_VALUE; }

    @Override
    public DeliveryStatus deliver(Message message) { return DeliveryStatus.DELIVERED; }

    @Override
    public CompletionStage<Try<DeliveryStatus>> asyncDeliver(Message message) {
        return CompletableFuture.completedFuture(Try.ofSuccess(deliver(message)));
    }
}
