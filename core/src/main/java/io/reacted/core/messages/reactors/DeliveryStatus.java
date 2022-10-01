/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

public enum DeliveryStatus {
    SENT,
    NOT_SENT,
    DELIVERED,
    NOT_DELIVERED,
    BACKPRESSURE_REQUIRED;

    public boolean isDelivered() { return this == DELIVERED || this == BACKPRESSURE_REQUIRED; }
    public boolean isNotSent() { return this == NOT_SENT; }
    public boolean isNotDelivered() { return this == NOT_SENT || this == NOT_DELIVERED; }

    public boolean isSent() { return this == SENT || isDelivered(); }

}
