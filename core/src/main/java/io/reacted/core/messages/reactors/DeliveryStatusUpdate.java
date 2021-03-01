/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import com.google.common.base.Objects;
import io.reacted.core.config.ChannelId;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;

@Immutable
@NonNullByDefault
public class DeliveryStatusUpdate implements Serializable {

    private final long msgSeqNum;
    private final DeliveryStatus deliveryStatus;
    private final ChannelId sourceChannelId;
    public DeliveryStatusUpdate(long msgSeqNum, DeliveryStatus deliveryStatus,
                                ChannelId sourceChannelId) {
        this.msgSeqNum = msgSeqNum;
        this.deliveryStatus = deliveryStatus;
        this.sourceChannelId = sourceChannelId;
    }

    public long getMsgSeqNum() { return msgSeqNum; }

    public DeliveryStatus getDeliveryStatus() { return deliveryStatus; }

    public ChannelId getSourceChannelId() { return sourceChannelId; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeliveryStatusUpdate that = (DeliveryStatusUpdate) o;
        return getMsgSeqNum() == that.getMsgSeqNum() &&
               getDeliveryStatus() == that.getDeliveryStatus() &&
               Objects.equal(getSourceChannelId(), that.getSourceChannelId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getMsgSeqNum(), getDeliveryStatus(), getSourceChannelId());
    }

}
