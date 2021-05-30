/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

@NonNullByDefault
public final class FilterOperator extends FlowOperator {
    private final Predicate<? super Serializable> filterPredicate;
    private FilterOperator(Predicate<? super Serializable> filterPredicate) {
        this.filterPredicate = Objects.requireNonNull(filterPredicate,
                                                      "FilterOperator predicate cannot be null");
    }

    public static CompletionStage<Try<ReActorRef>>
    of(ReActorSystem localReActorSystem, ReActorConfig operatorCfg,
       Predicate<? super Serializable> filter) {
        return CompletableFuture.completedStage(localReActorSystem.spawn(new FilterOperator(filter),
                                                                         operatorCfg));
    }
    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        return CompletableFuture.completedStage(filterPredicate.test(input) ? List.of(input) : List.of());
    }
}
