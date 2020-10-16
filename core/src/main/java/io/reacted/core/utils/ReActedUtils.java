/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.utils;

import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

@NonNullByDefault
public final class ReActedUtils {
    private ReActedUtils() { /* No Implementation required */ }

    public static  <PayloadT extends Serializable> void rescheduleIf(BiConsumer<ReActorContext, PayloadT> realCall,
                                                                     Supplier<Boolean> shouldReschedule,
                                                                     Duration rescheduleInterval,
                                                                     ReActorContext raCtx, PayloadT message) {
        if (shouldReschedule.get()) {
            raCtx.rescheduleMessage(message, rescheduleInterval)
                 .ifError(error -> raCtx.logError("WARNING {} misbehaves. Error attempting a {} reschedulation. " +
                                                  "System remoting may become unreliable ",
                                                  realCall.toString(), message.getClass().getSimpleName(), error));
        } else {
            realCall.accept(raCtx, message);
        }
    }
}
