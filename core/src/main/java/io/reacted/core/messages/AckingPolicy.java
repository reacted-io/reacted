/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages;

public enum AckingPolicy {
    /**
     * No ack will be sent back
     */
    NONE(false),
    /**
     * An ack containing the outcome of the delivery attempt will be sent back for each message
     * @see io.reacted.core.messages.reactors.DeliveryStatus
     * Any {@link io.reacted.core.reactorsystem.ReActorRef#tell} will not be considered as completed
     * before reaching the ack
     */
    ONE_TO_ONE(true);

    private final boolean isAckRequired;

    AckingPolicy(boolean isAckRequired) { this.isAckRequired = isAckRequired; }

    public boolean isAckRequired() { return isAckRequired; }
}
