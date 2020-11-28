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
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@NonNullByDefault
public class Reductions implements Reducer, Serializable {
    private final Map<Class<? extends Serializable>,
                      BiFunction<Serializable, ReActorContext,
                                 Collection<? extends Serializable>>> reductions;
    private Reductions(Builder builder) {
        this.reductions = builder.reductions.entrySet().stream()
                                            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                                                                                  Map.Entry::getValue));
    }

    @Override
    public Collection<? extends Serializable> reduce(ReActorContext raCtx, Serializable message) {
        return reductions.get(message.getClass()).apply(message, raCtx);
    }

    public static Builder newBuilder() { return new Builder(); }
    public static class Builder {
        private final Map<Class<? extends Serializable>,
                          BiFunction<Serializable, ReActorContext,
                                     Collection<? extends Serializable>>> reductions;
        private Builder() { this.reductions = new HashMap<>(); }

        public <InputT extends Serializable>
        Builder setReduction(Class<InputT> inputType, BiFunction<InputT, ReActorContext,
                                                                 Collection<? extends Serializable>> reducer) {
            this.reductions.put(inputType, (BiFunction<Serializable, ReActorContext, Collection<? extends Serializable>>) reducer);
            return this;
        }

        public Reductions build() { return new Reductions(this); }
    }
}
