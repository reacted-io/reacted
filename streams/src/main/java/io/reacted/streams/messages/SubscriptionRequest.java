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
import java.io.Serializable;

@NonNullByDefault
public class SubscriptionRequest implements Serializable {
    private final ReActorRef subscriptionBackpressuringManager;
    public SubscriptionRequest(ReActorRef subscriptionBackpressuringManager) {
        this.subscriptionBackpressuringManager = subscriptionBackpressuringManager;
    }
    public ReActorRef getSubscriptionBackpressuringManager() {
        return subscriptionBackpressuringManager;
    }

    @Override
    public String toString() {
        return "SubscriptionRequest{" +
               "subscriptionBackpressuringManager=" + subscriptionBackpressuringManager +
               '}';
    }
}
