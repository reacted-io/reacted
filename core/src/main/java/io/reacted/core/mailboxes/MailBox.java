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
public interface MailBox {
    boolean isEmpty();

    boolean isFull();

    long getMsgNum();

    long getMaxSize();

    @Nonnull
    Message getNextMessage();

    @Nonnull
    DeliveryStatus deliver(Message message);

    default void request(long messagesNum) { }
}
