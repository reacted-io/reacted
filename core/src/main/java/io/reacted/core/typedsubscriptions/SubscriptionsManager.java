/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.typedsubscriptions;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.typedsubscriptions.TypedSubscription.TypedSubscriptionPolicy;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface SubscriptionsManager {

  void addSubscription(Class<? extends Serializable> payloadType,
                       TypedSubscriptionPolicy subscriptionPolicy, ReActorContext subscriber);

  void removeSubscription(Class<? extends Serializable> payloadType,
                          TypedSubscriptionPolicy subscriptionPolicy,
                          ReActorContext subscriber);

  boolean hasFullSubscribers(Class<? extends Serializable> payloadType);

  @Nonnull
  List<ReActorContext> getLocalSubscribers(Class<? extends Serializable> payloadType);
}
