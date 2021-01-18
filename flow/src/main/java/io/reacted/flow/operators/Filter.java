/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

@NonNullByDefault
public final class Filter<InputT extends Serializable> extends PipelineStage {
    private final Predicate<InputT> filterPredicate;
    public Filter(Predicate<InputT> filterPredicate) {
        this.filterPredicate = Objects.requireNonNull(filterPredicate,
                                                      "Filter predicate cannot be null");
    }
    @Override
    protected Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx) {
        //noinspection unchecked
        return filterPredicate.test((InputT)input) ? List.of(input) : List.of();
    }
}
