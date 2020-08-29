/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;

@NonNullByDefault
public enum SubscriptionPolicy {
    LOCAL, REMOTE;
    public boolean isLocal() { return this != REMOTE; }
    public boolean isRemote() { return this != LOCAL; }

    public SniffSubscription forType(Class<? extends Serializable> payloadType) {
        return new SniffSubscription(this, payloadType);
    }

    public static class SniffSubscription {
        public static final SniffSubscription[] NO_SUBSCRIPTIONS = new SniffSubscription[]{};
        private final SubscriptionPolicy subscriptionPolicy;
        private final Class<? extends Serializable> payloadType;
        private SniffSubscription(SubscriptionPolicy subscriptionPolicy,
                                  Class<? extends Serializable> sniffedPayloadType) {
            this.subscriptionPolicy = subscriptionPolicy;
            this.payloadType = sniffedPayloadType;
        }

        public SubscriptionPolicy getSubscriptionPolicy() { return subscriptionPolicy; }

        public Class<? extends Serializable> getPayloadType() { return payloadType; }
    }
}
