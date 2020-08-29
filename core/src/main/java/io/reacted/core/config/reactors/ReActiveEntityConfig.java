/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import com.google.common.base.Strings;
import io.reacted.core.mailboxes.MailBox;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

@NonNullByDefault
public abstract class ReActiveEntityConfig<BuiltT extends ReActiveEntityConfig<BuiltT, BuilderT>,
                                           BuilderT extends ReActiveEntityConfig.Builder<BuiltT, BuilderT>>
    implements Serializable {

    private final String dispatcherName;
    private final String reActorName;
    private final SubscriptionPolicy.SniffSubscription[] typedSniffSubscriptions;
    private final Supplier<MailBox> mailBoxProvider;
    private final ReActiveEntityType reActiveEntityType;

    protected ReActiveEntityConfig(Builder<BuiltT, BuilderT> builder) {
        if (Strings.isNullOrEmpty(builder.dispatcherName)) {
            throw new IllegalArgumentException("DispatcherName cannot be empty or null");
        }
        this.reActorName = Objects.requireNonNull(builder.reActorName);
        this.mailBoxProvider = Objects.requireNonNull(builder.mailBoxProvider);
        this.typedSniffSubscriptions = Objects.requireNonNull(builder.typedSniffSubscriptions).length == 0
                                       ? SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS
                                       : Arrays.copyOf(builder.typedSniffSubscriptions,
                                                       builder.typedSniffSubscriptions.length);
        this.dispatcherName = builder.dispatcherName;
        this.reActiveEntityType = builder.entityType;
    }

    public String getDispatcherName() {
        return dispatcherName;
    }

    public String getReActorName() {
        return reActorName;
    }

    public SubscriptionPolicy.SniffSubscription[] getTypedSniffSubscriptions() {
        return Arrays.copyOf(typedSniffSubscriptions, typedSniffSubscriptions.length);
    }

    public Supplier<MailBox> getMailBoxProvider() {
        return mailBoxProvider;
    }

    public ReActiveEntityType getReActiveEntityType() {
        return reActiveEntityType;
    }

    public BuilderT fillBuilder(BuilderT realBuilder) {
        return realBuilder.setDispatcherName(getDispatcherName())
                          .setMailBoxProvider(getMailBoxProvider())
                          .setReActorName(getReActorName())
                          .setTypedSniffSubscriptions(getTypedSniffSubscriptions())
                          .setEntityType(getReActiveEntityType());
    }

    abstract public Builder<BuiltT, BuilderT> toBuilder();

    public abstract static class Builder<BuiltT extends ReActiveEntityConfig<BuiltT, BuilderT>,
                                         BuilderT extends Builder<BuiltT, BuilderT>> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String dispatcherName;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String reActorName;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private SubscriptionPolicy.SniffSubscription[] typedSniffSubscriptions;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Supplier<MailBox> mailBoxProvider;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActiveEntityType entityType;

        protected Builder() {
        }

        abstract public BuiltT build();

        @SuppressWarnings("unchecked")
        public BuilderT getThis() {
            return (BuilderT) this;
        }

        /**
         * Every reactor is scheduled only on a single dispatcher. Here is set which one
         */
        public BuilderT setDispatcherName(String dispatcherName) {
            this.dispatcherName = dispatcherName;
            return getThis();
        }

        /**
         * Every reactor needs a name that has to be unique among its siblings
         */
        public BuilderT setReActorName(String reActorName) {
            this.reActorName = reActorName;
            return getThis();
        }

        /**
         * Messages for a reactor are delivered within its unique and only mailbox
         *
         * @param mailBoxProvider specify how to obtain a new mailbox. Used on reactor creation
         */
        public BuilderT setMailBoxProvider(Supplier<MailBox> mailBoxProvider) {
            this.mailBoxProvider = mailBoxProvider;
            return getThis();
        }

        /**
         * Reactors can subscribe to message types to receive a copy of of a message if it matches the subscription
         * criteria
         *
         * @param typedSniffSubscriptions Sniffing subscriptions
         */
        public BuilderT setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription... typedSniffSubscriptions) {
            this.typedSniffSubscriptions = typedSniffSubscriptions;
            return getThis();
        }

        protected BuilderT setEntityType(ReActiveEntityType reActiveEntityType) {
            this.entityType = reActiveEntityType;
            return getThis();
        }
    }

    public enum ReActiveEntityType { REACTOR, REACTORSERVICE }
}
