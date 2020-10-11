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
public enum TypedSubscriptionPolicy {
    /** Messages will be intercepted if sent within the local ReactorSystem */
    LOCAL,
    /** Messages will be intercepted if received from a remote ReActorSystem */
    REMOTE;
    public boolean isLocal() { return this != REMOTE; }
    public boolean isRemote() { return this != LOCAL; }

    public TypedSubscription forType(Class<? extends Serializable> payloadType) {
        return new TypedSubscription(this, payloadType);
    }
}
