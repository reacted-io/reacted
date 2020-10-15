/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

@NonNullByDefault
public abstract class ChannelDriverCfg<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                       BuiltT extends InheritableBuilder<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {
    public static final Duration NEVER_FAIL = Duration.ofNanos(Long.MAX_VALUE);
    public static final String CHANNEL_ID_PROPERTY_NAME = "channelName";
    private final String channelName;
    private final boolean deliveryAckRequiredByChannel;
    private final Duration aTellAutomaticFailureTimeout;

    protected ChannelDriverCfg(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.channelName = Objects.requireNonNull(builder.channelName);
        this.deliveryAckRequiredByChannel = builder.deliveryAckRequiredByChannel;
        this.aTellAutomaticFailureTimeout = ObjectUtils.requiredCondition(Objects.requireNonNull(builder.aTellFailureTimeout),
                                                                          interval -> interval.compareTo(Duration.ZERO) > 0 &&
                                                                                      interval.compareTo(Duration.ofNanos(Long.MAX_VALUE)) <= 0,
                                                                          () -> new IllegalArgumentException("Invalid timeout!"));
    }

    public Properties getProperties() {
        var props = getChannelProperties();
        props.setProperty(ChannelDriverCfg.CHANNEL_ID_PROPERTY_NAME, getChannelName());
        return props;
    }

    public String getChannelName() { return channelName; }

    public boolean isDeliveryAckRequiredByChannel() { return deliveryAckRequiredByChannel; }

    public Duration getAtellAutomaticFailureTimeout() { return aTellAutomaticFailureTimeout; }

    public Properties getChannelProperties() { return new Properties(); }

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
         * @param aTellFailureTimeout the automatic failure timeout. Default {@link ChannelDriverCfg#NEVER_FAIL}
         *                            Max Value: {@link Long#MAX_VALUE} nanosecs
         * @return this builder
         */
        public final BuilderT setAckedTellAutomaticFailureAfterTimeout(Duration aTellFailureTimeout) {
            this.aTellFailureTimeout = aTellFailureTimeout;
            return getThis();
        }
    }
}
