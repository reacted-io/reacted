/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.typedsubscriptions;

import com.google.common.base.Objects;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class TypedSubscription {
    public static final TypedSubscription[] NO_SUBSCRIPTIONS = new TypedSubscription[]{};
    public static final TypedSubscriptionPolicy LOCAL = TypedSubscriptionPolicy.LOCAL;
    public static final TypedSubscriptionPolicy FULL = TypedSubscriptionPolicy.FULL;

    private final TypedSubscriptionPolicy typedSubscriptionPolicy;
    private final Class<? extends ReActedMessage> payloadType;

    public TypedSubscription(TypedSubscriptionPolicy typedSubscriptionPolicy,
                             Class<? extends ReActedMessage> sniffedPayloadType) {
        this.typedSubscriptionPolicy = typedSubscriptionPolicy;
        this.payloadType = sniffedPayloadType;
    }

    public TypedSubscriptionPolicy getSubscriptionPolicy() { return typedSubscriptionPolicy; }

    public Class<? extends ReActedMessage> getPayloadType() { return payloadType; }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        TypedSubscription that = (TypedSubscription) o;
        return typedSubscriptionPolicy == that.typedSubscriptionPolicy &&
               Objects.equal(getPayloadType(), that.getPayloadType());
    }

    @Override
    public int hashCode() { return Objects.hashCode(typedSubscriptionPolicy, getPayloadType()); }

    public enum TypedSubscriptionPolicy {
        /** Messages will be intercepted if sent within the local ReactorSystem */
        LOCAL,
        /** Messages will be intercepted if received from the local or a remote ReActorSystem */
        FULL;
        public boolean isLocal() { return this != FULL; }
        public boolean isFull() { return this != LOCAL; }

        public TypedSubscription forType(Class<? extends ReActedMessage> payloadType) {
            return new TypedSubscription(this, payloadType);
        }
    }
}