/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import com.google.common.base.Strings;
import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.mailboxes.MailBox;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

@NonNullByDefault
public abstract class ReActiveEntityConfig<BuilderT extends ReActiveEntityConfig.Builder<BuilderT, BuiltT>,
                                           BuiltT extends ReActiveEntityConfig<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {

    public static final Function<ReActorContext, MailBox> DEFAULT_MAILBOX_SUPPLIER = ctx -> new BasicMbox();
    public static final TypedSubscription[] DEFAULT_SNIFF_SUBSCRIPTIONS = TypedSubscription.NO_SUBSCRIPTIONS;
    private final String dispatcherName;
    private final String reActorName;
    private final TypedSubscription[] typedTypedSubscriptions;
    private final Function<ReActorContext, MailBox> mailBoxProvider;
    private final ReActiveEntityType reActiveEntityType;

    protected ReActiveEntityConfig(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        if (Strings.isNullOrEmpty(builder.dispatcherName)) {
            throw new IllegalArgumentException("DispatcherName cannot be empty or null");
        }
        this.reActorName = Objects.requireNonNull(builder.reActorName);
        this.mailBoxProvider = Objects.requireNonNull(builder.mailBoxProvider);
        this.typedTypedSubscriptions = Objects.requireNonNull(builder.typedTypedSubscriptions).length == 0
                                       ? TypedSubscription.NO_SUBSCRIPTIONS
                                       : Arrays.copyOf(builder.typedTypedSubscriptions,
                                                       builder.typedTypedSubscriptions.length);
        this.dispatcherName = builder.dispatcherName;
        this.reActiveEntityType = builder.entityType;
    }

    public final String getDispatcherName() {
        return dispatcherName;
    }

    public final String getReActorName() {
        return reActorName;
    }

    public final TypedSubscription[] getTypedSniffSubscriptions() {
        return Arrays.copyOf(typedTypedSubscriptions, typedTypedSubscriptions.length);
    }

    public final Function<ReActorContext, MailBox> getMailBoxProvider() {
        return mailBoxProvider;
    }

    public final ReActiveEntityType getReActiveEntityType() {
        return reActiveEntityType;
    }

    protected BuilderT fillBuilder(Builder<BuilderT, BuiltT> realBuilder) {
        return realBuilder.setDispatcherName(getDispatcherName())
                          .setMailBoxProvider(getMailBoxProvider())
                          .setReActorName(getReActorName())
                          .setTypedSubscriptions(getTypedSniffSubscriptions())
                          .setEntityType(getReActiveEntityType());
    }

    abstract public Builder<BuilderT, BuiltT> toBuilder();

    public abstract static class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        private String dispatcherName = ReActorSystem.DEFAULT_DISPATCHER_NAME;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String reActorName;
        private TypedSubscription[] typedTypedSubscriptions = DEFAULT_SNIFF_SUBSCRIPTIONS;
        private Function<ReActorContext, MailBox> mailBoxProvider = DEFAULT_MAILBOX_SUPPLIER;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActiveEntityType entityType;

        protected Builder() { }

        /**
         * Every reactor is scheduled only on a single dispatcher. Here is set which one
         * @param dispatcherName Name of the {@link io.reacted.core.runtime.Dispatcher} on which this reactor should run
         */
        public final BuilderT setDispatcherName(String dispatcherName) {
            this.dispatcherName = dispatcherName;
            return getThis();
        }

        /**
         * Every reactor needs a name that has to be unique among its siblings
         *
         * @param reActorName name of the reactor
         */
        public final BuilderT setReActorName(String reActorName) {
            this.reActorName = reActorName;
            return getThis();
        }

        /**
         * Messages for a reactor are delivered within its unique and only mailbox
         *
         * @param mailBoxProvider specify how to obtain a new mailbox. Used on reactor creation
         */
        public final BuilderT setMailBoxProvider(Function<ReActorContext, MailBox> mailBoxProvider) {
            this.mailBoxProvider = mailBoxProvider;
            return getThis();
        }

        /**
         * Reactors can subscribe to message types to receive a copy of of a message if it matches the subscription
         * criteria
         *
         * @param typedTypedSubscriptions Sniffing subscriptions
         */
        public final BuilderT setTypedSubscriptions(TypedSubscription... typedTypedSubscriptions) {
            this.typedTypedSubscriptions = typedTypedSubscriptions;
            return getThis();
        }

        protected final BuilderT setEntityType(ReActiveEntityType reActiveEntityType) {
            this.entityType = reActiveEntityType;
            return getThis();
        }
    }

    public enum ReActiveEntityType { REACTOR, REACTORSERVICE }
}
