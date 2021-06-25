/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams.messages;

import java.io.Serializable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SubscriptionReply implements Serializable {
    private final boolean success;
    public SubscriptionReply(boolean isSuccess) {
        this.success = isSuccess;
    }
    public boolean isSuccess() { return success; }

    @Override
    public String toString() {
        return "SubscriptionReply{" +
               "success=" + success +
               '}';
    }
}
