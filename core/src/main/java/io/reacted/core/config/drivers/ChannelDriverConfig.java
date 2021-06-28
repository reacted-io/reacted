/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public abstract class ChannelDriverConfig<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                          BuiltT extends InheritableBuilder<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {
    public static final Duration NEVER_FAIL = Duration.ofNanos(Long.MAX_VALUE);
    public static final Duration DEFAULT_MSG_LOST_TIMEOUT = Duration.ofSeconds(20);
    public static final int DEFAULT_ACK_CACHE_SIZE = 10_000_000;
    public static final String CHANNEL_ID_PROPERTY_NAME = "channelName";
    private final String channelName;
    private final boolean deliveryAckRequiredByChannel;
    private final Duration atellAutomaticFailureTimeout;
    private final Duration ackCacheCleanupInterval;
    private final int ackCacheSize;

    protected ChannelDriverConfig(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.channelName = Objects.requireNonNull(builder.channelName,
                                                  "Channel name cannot be null");
        this.deliveryAckRequiredByChannel = builder.deliveryAckRequiredByChannel;
        this.atellAutomaticFailureTimeout = ObjectUtils.checkNonNullPositiveTimeIntervalWithLimit(builder.atellFailureTimeout,
                                                                                                  Long.MAX_VALUE,
                                                                                                  TimeUnit.NANOSECONDS);
        this.ackCacheCleanupInterval = ObjectUtils.checkNonNullPositiveTimeInterval(builder.ackCacheCleanupInterval);
        this.ackCacheSize = ObjectUtils.requiredInRange(builder.ackCacheSize, 0, Integer.MAX_VALUE,
                                                        IllegalArgumentException::new);
    }

    public Properties getProperties() {
        var props = getChannelProperties();
        props.setProperty(ChannelDriverConfig.CHANNEL_ID_PROPERTY_NAME, getChannelName());
        return props;
    }

    public String getChannelName() { return channelName; }

    public boolean isDeliveryAckRequiredByChannel() { return deliveryAckRequiredByChannel; }

    public Duration getAtellAutomaticFailureTimeout() { return atellAutomaticFailureTimeout; }

    public Properties getChannelProperties() { return new Properties(); }

    public Duration getAckCacheCleanupInterval() { return ackCacheCleanupInterval; }

    public int getAckCacheSize() { return ackCacheSize; }

    public abstract static class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String channelName;
        private Duration atellFailureTimeout = DEFAULT_MSG_LOST_TIMEOUT;
        private Duration ackCacheCleanupInterval = DEFAULT_MSG_LOST_TIMEOUT;
        private boolean deliveryAckRequiredByChannel;
        private int ackCacheSize = DEFAULT_ACK_CACHE_SIZE;

        public final BuilderT setChannelName(String channelName) {
            this.channelName = channelName;
            return getThis();
        }

        /**
         *
         * @param isDeliveryAckRequiredByChannel specify if the {@link io.reacted.core.config.ChannelId} provided
         *                                       by the driver is not reliable enough and it may require an ack.
         *                                       If this is set to false, all the messages sent through {@link io.reacted.core.reactorsystem.ReActorRef#atell}
         *                                       will behave like {@link io.reacted.core.reactorsystem.ReActorRef#tell}
         * @return this builder
         */
        public final BuilderT setChannelRequiresDeliveryAck(boolean isDeliveryAckRequiredByChannel) {
            this.deliveryAckRequiredByChannel = isDeliveryAckRequiredByChannel;
            return getThis();
        }

        /**
         * Specify after how much time a not acknowledged message from {@link io.reacted.core.reactorsystem.ReActorRef#atell}
         * should be automatically marked as completed as a failure
         * @param atellFailureTimeout the automatic failure timeout. Default {@link ChannelDriverConfig#DEFAULT_MSG_LOST_TIMEOUT}
         *                            Max Value: {@link Long#MAX_VALUE} nanosecs
         * @return this builder
         */
        public final BuilderT setAtellAutomaticFailureAfterTimeout(Duration atellFailureTimeout) {
            this.atellFailureTimeout = atellFailureTimeout;
            return getThis();
        }

        /**
         * Specify how often the cache used for keeping the pending acks triggers for {@link io.reacted.core.reactorsystem.ReActorRef#atell}
         * should be checked for maintenance
         *
         * @param ackCacheCleanupInterval Cache cleanup rate. Positive intervals only, default: {@link ChannelDriverConfig#DEFAULT_MSG_LOST_TIMEOUT}
         * @return this builder
         */
        public final BuilderT setAckCacheCleanupInterval(Duration ackCacheCleanupInterval) {
            this.ackCacheCleanupInterval = ackCacheCleanupInterval;
            return getThis();
        }

        /**
         * Gives a hint regarding the size of the ack cache for this driver
         * @param ackCacheSize A non negative integer
         * @return This builder
         */
        public final BuilderT setAckCacheSize(int ackCacheSize) {
            this.ackCacheSize = ackCacheSize;
            return getThis();
        }
    }
}
