/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import java.io.Serializable;

public class TypedSubscription {
    public static final TypedSubscription[] NO_SUBSCRIPTIONS = new TypedSubscription[]{};
    private final TypedSubscriptionPolicy typedSubscriptionPolicy;
    private final Class<? extends Serializable> payloadType;

    public TypedSubscription(TypedSubscriptionPolicy typedSubscriptionPolicy,
                             Class<? extends Serializable> sniffedPayloadType) {
        this.typedSubscriptionPolicy = typedSubscriptionPolicy;
        this.payloadType = sniffedPayloadType;
    }

    public TypedSubscriptionPolicy getSubscriptionPolicy() { return typedSubscriptionPolicy; }

    public Class<? extends Serializable> getPayloadType() { return payloadType; }
}