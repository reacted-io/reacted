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
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

@NonNullByDefault
public class ReActions {
    public static final ReActions NO_REACTIONS = ReActions.newBuilder().build();

    private final Map<Class<? extends ReActedMessage>,
                      BiConsumer<ReActorContext, ? extends ReActedMessage>> behaviors;
    private final BiConsumer<ReActorContext, ReActedMessage> defaultReaction;

    private ReActions(Builder builder) {
        this.behaviors = builder.callbacks.build();
        this.defaultReaction = Objects.requireNonNull(builder.anyType,
                                                      "Default reaction cannot be null");
    }

    @SuppressWarnings("unchecked")
    public <PayloadT extends ReActedMessage>
    BiConsumer<ReActorContext, PayloadT> getReAction(PayloadT payload) {
        return (BiConsumer<ReActorContext, PayloadT>) behaviors.getOrDefault(payload.getClass(),
                                                                             defaultReaction);
    }

    public static Builder newBuilder() { return new Builder(); }
    private Map<Class<? extends ReActedMessage>, BiConsumer<ReActorContext, ? extends ReActedMessage>>
    getBehaviors() { return behaviors; }

    public static class Builder {
        private final ImmutableMap.Builder<Class<? extends ReActedMessage>,
                                           BiConsumer<ReActorContext, ? extends ReActedMessage>> callbacks;
        private BiConsumer<ReActorContext, ReActedMessage> anyType = ReActions::noReAction;

        private Builder() {
            this.callbacks = ImmutableMap.builder();
        }

        public final Builder reAct(BiConsumer<ReActorContext, ReActedMessage> defaultReaction) {
            this.anyType = defaultReaction;
            return this;
        }

        public final <PayloadT extends ReActedMessage>
        Builder reAct(Class<PayloadT> payloadType, BiConsumer<ReActorContext, PayloadT> behavior) {
            callbacks.put(Objects.requireNonNull(payloadType, "Message type cannot be null"),
                          Objects.requireNonNull(behavior, "Message callback cannot be null"));
            return this;
        }

        public final Builder from(ReActions reActions) {
            Objects.requireNonNull(reActions, "Source reactions cannot be null")
                   .getBehaviors()
                   .forEach(callbacks::put);
            return this;
        }

        public ReActions build() {
            return new ReActions(this);
        }
    }

    @SuppressWarnings("EmptyMethod")
    public static <PayloadT extends ReActedMessage>
    void noReAction(ReActorContext ctx, PayloadT payload) { /* No Reactions */ }
}
