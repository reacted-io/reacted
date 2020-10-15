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
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.util.Objects;
import java.util.function.Consumer;

@NonNullByDefault
public final class ReActedUtils {
    private ReActedUtils() { /* No Implementation required */ }

    public static Try<DeliveryStatus> ifNotDelivered(Try<DeliveryStatus> deliveryAttempt,
                                                     Consumer<Throwable> ifNotDelivered) {
        return ifNotDelivered(deliveryAttempt).peekFailure(Objects.requireNonNull(ifNotDelivered));
    }

    public static Try<DeliveryStatus> ifNotDelivered(Try<DeliveryStatus> deliveryAttempt) {
        return Objects.requireNonNull(deliveryAttempt)
                      .filter(DeliveryStatus::isDelivered, DeliveryException::new);
    }
}
