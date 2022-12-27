/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.mailboxes.MailBox;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
import java.util.Set;

@NonNullByDefault
public class ReActorCtxConfig {
    private final Set<Class<? extends ReActedMessage>> subscribedTypes;
    private final Dispatcher dispatcher;
    private final MailBox mailBox;

    private ReActorCtxConfig(Builder actorCtxConfigBuilder) {
        this.subscribedTypes = Objects.requireNonNull(actorCtxConfigBuilder.subscribedTypes);
        this.mailBox = Objects.requireNonNull(actorCtxConfigBuilder.reactorMailbox);
        this.dispatcher = Objects.requireNonNull(actorCtxConfigBuilder.dispatcher);
    }

    Set<Class<? extends ReActedMessage>> getSubscribedTypes() { return subscribedTypes; }

    Dispatcher getDispatcher() { return dispatcher; }

    MailBox getMailBox() { return mailBox; }

    @SuppressWarnings("NotNullFieldNotInitialized")
    public static class Builder {
        private Set<Class<? extends ReActedMessage>> subscribedTypes;
        private MailBox reactorMailbox;
        private Dispatcher dispatcher;

        public final Builder setSubscribedTypes(Set<Class<? extends ReActedMessage>> subscribedTypes) {
            this.subscribedTypes = subscribedTypes;
            return this;
        }

        public final Builder setMailBox(MailBox reactorMailbox) {
            this.reactorMailbox = reactorMailbox;
            return this;
        }

        public final Builder setDispatcher(Dispatcher actorDispatcher) {
            this.dispatcher = actorDispatcher;
            return this;
        }

        public ReActorCtxConfig build() {
            return new ReActorCtxConfig(this);
        }
    }
}
