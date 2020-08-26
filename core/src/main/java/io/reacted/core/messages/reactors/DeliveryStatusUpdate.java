/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class DeliveryStatusUpdate implements Serializable {

    private final long msgSeqNum;
    private final DeliveryStatus deliveryStatus;

    public DeliveryStatusUpdate(long msgSeqNum, DeliveryStatus deliveryStatus) {
        this.msgSeqNum = msgSeqNum;
        this.deliveryStatus = deliveryStatus;
    }

    public long getMsgSeqNum() { return msgSeqNum; }

    public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeliveryStatusUpdate that = (DeliveryStatusUpdate) o;
        return getMsgSeqNum() == that.getMsgSeqNum() &&
                getDeliveryStatus() == that.getDeliveryStatus();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMsgSeqNum(), getDeliveryStatus());
    }

}
