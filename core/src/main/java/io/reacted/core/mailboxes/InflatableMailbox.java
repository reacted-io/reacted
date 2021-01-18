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
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.util.concurrent.atomic.AtomicLong;

@NonNullByDefault
public class InflatableMailbox extends BasicMbox {
    private final AtomicLong mailboxCapacity;

    public InflatableMailbox() { this(0L); }

    public InflatableMailbox(long mailboxCapacity) {
        this.mailboxCapacity = new AtomicLong(ObjectUtils.requiredInRange(mailboxCapacity, 0L, Long.MAX_VALUE,
                                                                          IllegalArgumentException::new));
    }

    public long adjustCapacity(int capacityDelta) {
        return mailboxCapacity.addAndGet(capacityDelta);
    }

    public InflatableMailbox setCapacity(long newCapacity) {
        mailboxCapacity.set(newCapacity);
        return this;
    }

    @Override
    public boolean isFull() { return super.getMsgNum() >= mailboxCapacity.get(); }

    @Override
    public long getMaxSize() { return mailboxCapacity.get(); }

    @Override
    public DeliveryStatus deliver(Message message) {
        if (isFull()) {
            return DeliveryStatus.BACKPRESSURED;
        }
        synchronized (mailboxCapacity) {
            return isFull() ? DeliveryStatus.BACKPRESSURED
                            : super.deliver(message);
        }
    }
}
