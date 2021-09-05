/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams.messages;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class SubscriptionRequest extends UnsubscriptionRequest {
    public SubscriptionRequest(ReActorRef subscriptionBackpressuringManager) {
        super(subscriptionBackpressuringManager);
    }

    @Override
    public String toString() {
        return "SubscriptionRequest{ " + super.toString() + " }";
    }
}
