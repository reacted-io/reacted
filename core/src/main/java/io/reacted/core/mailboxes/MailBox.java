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
import io.reacted.patterns.Try;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletionStage;

@ParametersAreNonnullByDefault
public interface MailBox extends AutoCloseable {
    CompletionStage<DeliveryStatus> DELIVERED = CompletableFuture.completedStage(DeliveryStatus.DELIVERED);
    CompletionStage<DeliveryStatus> BACKPRESSURED = CompletableFuture.completedStage(DeliveryStatus.BACKPRESSURED);
    boolean isEmpty();

    boolean isFull();

    long getMsgNum();

    long getMaxSize();

    @Nonnull
    Message getNextMessage();

    @Nonnull
    DeliveryStatus deliver(Message message);

    @Nonnull
    default CompletionStage<DeliveryStatus> asyncDeliver(Message message) {
        return deliver(message).isDelivered()
               ? DELIVERED
               : BACKPRESSURED;
    }

    default void request(long messagesNum) { }
    @Override
    default void close() throws Exception { }
}
