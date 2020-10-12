/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.utils.ConfigUtils;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

@NonNullByDefault
public abstract class ReActedDriverCfg<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                       BuiltT extends InheritableBuilder<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {
    public static final Duration NEVER_FAIL = Duration.ofSeconds(Long.MAX_VALUE);
    public static final String CHANNEL_ID_PROPERTY_NAME = "channelName";
    public static final String IS_DELIVERY_ACK_REQUIRED_BY_CHANNEL_PROPERTY_NAME = "deliveryAckRequiredByChannel";
    private final String channelName;
    private final boolean deliveryAckRequiredByChannel;
    private final Duration aTellAutomaticFailureTimeout;

    protected ReActedDriverCfg(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.channelName = Objects.requireNonNull(builder.channelName);
        this.deliveryAckRequiredByChannel = builder.deliveryAckRequiredByChannel;
        this.aTellAutomaticFailureTimeout = Objects.requireNonNull(builder.aTellFailureTimeout);
    }

    public Properties getProperties() {
        return ConfigUtils.toProperties(this, Set.of());
    }

    public String getChannelName() { return channelName; }

    public boolean isDeliveryAckRequiredByChannel() { return deliveryAckRequiredByChannel; }

    public Duration getAtellAutomaticFailureTimeout() { return aTellAutomaticFailureTimeout; }

    public static abstract class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String channelName;
        private Duration aTellFailureTimeout = NEVER_FAIL;
        private boolean deliveryAckRequiredByChannel;

        public final BuilderT setChannelName(String channelName) {
            this.channelName = channelName;
            return getThis();
        }

        /**
         *
         * @param isDeliveryAckRequiredByChannel specify if the {@link io.reacted.core.config.ChannelId} provided
         *                                       by the driver is not reliable enough and it may require an ack.
         *                                       If this is set to false, all the messages sent through {@link io.reacted.core.reactorsystem.ReActorRef#aTell}
         *                                       will behave like {@link io.reacted.core.reactorsystem.ReActorRef#tell}
         * @return this builder
         */
        public final BuilderT setChannelRequiresDeliveryAck(boolean isDeliveryAckRequiredByChannel) {
            this.deliveryAckRequiredByChannel = isDeliveryAckRequiredByChannel;
            return getThis();
        }

        /**
         * Specify after how much time a not acknowledged message from {@link io.reacted.core.reactorsystem.ReActorRef#aTell}
         * should be automatically marked as completed as a failure
         * @param aTellFailureTimeout the automatic failure timeout. Default {@link ReActedDriverCfg#NEVER_FAIL}
         * @return this builder
         */
        public final BuilderT setAckedTellAutomaticFailureAfterTimeout(Duration aTellFailureTimeout) {
            this.aTellFailureTimeout = aTellFailureTimeout;
            return getThis();
        }
    }
}
