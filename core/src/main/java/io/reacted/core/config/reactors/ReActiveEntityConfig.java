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
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.NonNullByDefault;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;

@NonNullByDefault
@Immutable
public abstract class ReActiveEntityConfig<BuilderT extends ReActiveEntityConfig.Builder<BuilderT, BuiltT>,
                                           BuiltT extends ReActiveEntityConfig<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {

    public static final Function<ReActorContext, MailBox> DEFAULT_MAILBOX_SUPPLIER = ctx -> new BasicMbox();
    public static final TypedSubscription[] DEFAULT_TYPED_SUBSCRIPTIONS = TypedSubscription.NO_SUBSCRIPTIONS;
    private final String dispatcherName;
    private final String reActorName;
    private final TypedSubscription[] typedSubscriptions;
    private final Function<ReActorContext, MailBox> mailBoxProvider;

    protected ReActiveEntityConfig(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        if (Strings.isNullOrEmpty(builder.dispatcherName)) {
            throw new IllegalArgumentException("DispatcherName cannot be empty or null");
        }
        this.reActorName = Objects.requireNonNull(builder.reActorName, "ReActor name is mandatory");
        this.mailBoxProvider = Objects.requireNonNull(builder.mailBoxProvider, "Mailbox provider is mandatory");
        this.typedSubscriptions = Objects.requireNonNull(builder.typedSubscriptions,
                                                         "Typed Subscriptions cannot be null").length == 0
                                       ? TypedSubscription.NO_SUBSCRIPTIONS
                                       : Arrays.copyOf(builder.typedSubscriptions,
                                                       builder.typedSubscriptions.length);
        this.dispatcherName = builder.dispatcherName;
    }

    public final String getDispatcherName() {
        return dispatcherName;
    }

    public final String getReActorName() {
        return reActorName;
    }

    public final TypedSubscription[] getTypedSubscriptions() {
        return Arrays.copyOf(typedSubscriptions, typedSubscriptions.length);
    }

    public final Function<ReActorContext, MailBox> getMailBoxProvider() {
        return mailBoxProvider;
    }

    public abstract static class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        protected String dispatcherName = Dispatcher.DEFAULT_DISPATCHER_NAME;
        @SuppressWarnings("NotNullFieldNotInitialized")
        protected String reActorName;
        protected TypedSubscription[] typedSubscriptions = DEFAULT_TYPED_SUBSCRIPTIONS;
        protected Function<ReActorContext, MailBox> mailBoxProvider = DEFAULT_MAILBOX_SUPPLIER;

        protected Builder() { }

        /**
         * Every reactor is scheduled only on a single dispatcher. Here is set which one
         * @param dispatcherName Name of the {@link io.reacted.core.runtime.Dispatcher} on which this reactor should run
         * @return this builder
         */
        public final BuilderT setDispatcherName(String dispatcherName) {
            this.dispatcherName = dispatcherName;
            return getThis();
        }

        /**
         * Every reactor needs a name that has to be unique among its siblings
         *
         * @param reActorName name of the reactor
         * @return this builder
         */
        public final BuilderT setReActorName(String reActorName) {
            this.reActorName = reActorName;
            return getThis();
        }

        /**
         * Messages for a reactor are delivered within its unique and only mailbox
         *
         * @param mailBoxProvider specify how to obtain a new mailbox. Used on reactor creation
         * @return this builder
         */
        public final BuilderT setMailBoxProvider(Function<ReActorContext, MailBox> mailBoxProvider) {
            this.mailBoxProvider = mailBoxProvider;
            return getThis();
        }

        /**
         * Reactors can subscribe to message types to receive a copy of of a message if it matches the subscription
         * criteria
         *
         * @param typedSubscriptions Typed subscriptions
         * @return this builder
         */
        public final BuilderT setTypedSubscriptions(TypedSubscription... typedSubscriptions) {
            this.typedSubscriptions = typedSubscriptions;
            return getThis();
        }
    }
}
