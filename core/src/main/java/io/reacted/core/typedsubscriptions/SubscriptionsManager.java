/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.typedsubscriptions;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.typedsubscriptions.TypedSubscription.TypedSubscriptionPolicy;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;

@ParametersAreNonnullByDefault
public interface SubscriptionsManager {

  default void addSubscription(Class<? extends ReActedMessage> payloadType,
                               TypedSubscriptionPolicy subscriptionPolicy,
                               ReActorContext subscriber) { }

  default void removeSubscription(Class<? extends ReActedMessage> payloadType,
                                  TypedSubscriptionPolicy subscriptionPolicy,
                                  ReActorContext subscriber) { }

  default boolean hasFullSubscribers(Class<? extends ReActedMessage> payloadType) { return false; }

  @Nonnull
  default List<ReActorContext> getLocalSubscribers(Class<? extends ReActedMessage> payloadType) {
    return List.of();
  }
}
