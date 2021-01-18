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
public class Reductions implements Reducer {
    private final Map<Class<Serializable>,
                      BiFunction<Serializable, ReActorContext,
                                 Collection<Serializable>>> reductors;
    private Reductions(Builder builder) {
        this.reductors = builder.reductions.entrySet().stream()
                                           .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                                                                                 Map.Entry::getValue));
    }

    @Override
    public Collection<? extends Serializable> reduce(ReActorContext raCtx, Serializable message) {
        return reductors.get(message.getClass()).apply(message, raCtx);
    }

    public static Builder newBuilder() { return new Builder(); }
    public static class Builder {
        private final Map<Class<Serializable>,
                          BiFunction<Serializable, ReActorContext,
                                     Collection<Serializable>>> reductions;
        private Builder() { this.reductions = new HashMap<>(); }

        public Builder setReduction(Class<Serializable> inputType,
                             BiFunction<Serializable, ReActorContext,
                                        Collection<Serializable>> reducer) {
            this.reductions.put(inputType, reducer);
            return this;
        }

        public Reductions build() { return new Reductions(this); }
    }
}
