/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.typedsubscriptions;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@NonNullByDefault
public class TypedSubscriptionsManager implements SubscriptionsManager {
    private final Map<Class<? extends Serializable>, SubscriptionBucket> typeToSubscriber;
    public TypedSubscriptionsManager() {
        this.typeToSubscriber = new ConcurrentHashMap<>(5000, 0.5f);
    }

    @Override
    public void addSubscription(@Nonnull Class<? extends Serializable> payloadType,
                                @Nonnull TypedSubscription.TypedSubscriptionPolicy subscriptionPolicy,
                                @Nonnull ReActorContext subscriber) {
        var bucket = typeToSubscriber.computeIfAbsent(Objects.requireNonNull(payloadType),
                                                      type -> new SubscriptionBucket());
        bucket.addSubscriber(Objects.requireNonNull(subscriptionPolicy), Objects.requireNonNull(subscriber));
    }
    @Override
    public void removeSubscription(@Nonnull Class<? extends Serializable> payloadType,
                                   @Nonnull TypedSubscription.TypedSubscriptionPolicy subscriptionPolicy,
                                   @Nonnull ReActorContext subscriber) {
        var bucket = typeToSubscriber.get(Objects.requireNonNull(payloadType));
        if (bucket != null) {
            bucket.removeSubscriber(Objects.requireNonNull(subscriptionPolicy), Objects.requireNonNull(subscriber));
        }
    }
    @Override
    public boolean hasFullSubscribers(@Nonnull Class<? extends Serializable> payloadType) {
        var bucket = typeToSubscriber.get(payloadType);
        return bucket != null && bucket.hasFullSubscriptions();
    }
    @Nonnull
    @Override
    public List<ReActorContext> getLocalSubscribers(
        @Nonnull Class<? extends Serializable> payloadType) {
        var localBucket = typeToSubscriber.get(payloadType);
        return localBucket != null
               ? localBucket.subscribers
               : List.of();
    }
    public static TypedSubscription[] getNormalizedSubscriptions(TypedSubscription ...subscriptions) {
        var uniquePerPolicy = Arrays.stream(subscriptions).distinct()
                                    .collect(Collectors.groupingBy(TypedSubscription::getSubscriptionPolicy,
                                                                   Collectors.mapping(TypedSubscription::getPayloadType,
                                                                                      Collectors.toUnmodifiableSet())));
        return Stream.concat(uniquePerPolicy.getOrDefault(TypedSubscription.TypedSubscriptionPolicy.FULL,
                                                          Set.of()).stream()
                                            .map(TypedSubscription.FULL::forType),
                             uniquePerPolicy.getOrDefault(TypedSubscription.TypedSubscriptionPolicy.LOCAL,
                                                          Set.of()).stream()
                                            .filter(localSub -> !uniquePerPolicy.getOrDefault(TypedSubscription.TypedSubscriptionPolicy.FULL,
                                                                                              Set.of()).contains(localSub))
                                            .map(TypedSubscription.LOCAL::forType))
                     .toArray(TypedSubscription[]::new);
    }
    private static final class SubscriptionBucket {
        private final AtomicLong fullSubscriptions;
        private final List<ReActorContext> subscribers;
        private SubscriptionBucket() {
            this.fullSubscriptions = new AtomicLong();
            this.subscribers = new CopyOnWriteArrayList<>();
        }
        private void addSubscriber(TypedSubscription.TypedSubscriptionPolicy subscriptionType,
                                   ReActorContext subscriber) {
            this.subscribers.add(subscriber);
            if (subscriptionType.isFull()) {
                fullSubscriptions.incrementAndGet();
            }
        }

        private void removeSubscriber(TypedSubscription.TypedSubscriptionPolicy subscriptionType,
                                      ReActorContext subscriber) {
            this.subscribers.remove(subscriber);
            if (subscriptionType.isFull()) {
                this.fullSubscriptions.decrementAndGet();
            }
        }
        private boolean hasFullSubscriptions() { return fullSubscriptions.get() != 0L; }
    }
}
