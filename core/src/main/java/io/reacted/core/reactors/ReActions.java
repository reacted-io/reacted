/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import com.google.common.collect.ImmutableMap;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

@NonNullByDefault
public class ReActions {
    public static final ReActions NO_REACTIONS = ReActions.newBuilder().build();

    private final Map<Class<? extends Serializable>,
                      BiConsumer<ReActorContext, ? extends Serializable>> behaviors;
    private final BiConsumer<ReActorContext, Serializable> defaultReaction;

    private ReActions(Builder builder) {
        this.behaviors = builder.callbacks.build();
        this.defaultReaction = Objects.requireNonNull(builder.anyType);
    }

    @SuppressWarnings("unchecked")
    public <PayloadT extends Serializable>
    BiConsumer<ReActorContext, PayloadT> getReAction(PayloadT payload) {
        return (BiConsumer<ReActorContext, PayloadT>) behaviors.getOrDefault(payload.getClass(), defaultReaction);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private final ImmutableMap.Builder<Class<? extends Serializable>,
                                           BiConsumer<ReActorContext, ? extends Serializable>> callbacks;
        private BiConsumer<ReActorContext, Serializable> anyType = ReActions::noReAction;

        private Builder() {
            this.callbacks = ImmutableMap.builder();
        }

        public Builder reAct(BiConsumer<ReActorContext, Serializable> defaultReaction) {
            this.anyType = defaultReaction;
            return this;
        }

        public <PayloadT extends Serializable>
        Builder reAct(Class<PayloadT> payloadType, BiConsumer<ReActorContext, PayloadT> behavior) {
            callbacks.put(Objects.requireNonNull(payloadType), Objects.requireNonNull(behavior));
            return this;
        }

        public ReActions build() {
            return new ReActions(this);
        }
    }

    @SuppressWarnings("EmptyMethod")
    public static <PayloadT extends Serializable> void noReAction(ReActorContext raCtx, PayloadT payload) { }
}
