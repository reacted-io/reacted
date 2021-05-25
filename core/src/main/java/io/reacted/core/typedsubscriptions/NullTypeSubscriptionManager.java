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

public class NullTypeSubscriptionManager implements SubscriptionsManager {
  @Override
  public void addSubscription(@Nonnull Class<? extends Serializable> payloadType,
                              @Nonnull TypedSubscriptionPolicy subscriptionPolicy,
                              @Nonnull ReActorContext subscriber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSubscription(@Nonnull Class<? extends Serializable> payloadType,
                                 @Nonnull TypedSubscriptionPolicy subscriptionPolicy,
                                 @Nonnull ReActorContext subscriber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasFullSubscribers(@Nonnull Class<? extends Serializable> payloadType) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public List<ReActorContext> getLocalSubscribers(@Nonnull Class<? extends Serializable> payloadType) {
    throw new UnsupportedOperationException();
  }
}
