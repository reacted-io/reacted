/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public abstract class ChannelDriverConfig<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                          BuiltT extends InheritableBuilder<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {
    public static final Duration DEFAULT_MSG_LOST_TIMEOUT = Duration.ofSeconds(20);
    public static final int DEFAULT_ACK_CACHE_SIZE = 10_000_000;
    public static final String CHANNEL_ID_PROPERTY_NAME = "channelName";
    private final String channelName;
    private final Duration aPublishAutomaticFailureTimeout;
    private final Duration ackCacheCleanupInterval;
    private final int ackCacheSize;

    protected ChannelDriverConfig(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.channelName = Objects.requireNonNull(builder.channelName,
                                                  "Channel name cannot be null");
        this.aPublishAutomaticFailureTimeout = ObjectUtils.checkNonNullPositiveTimeIntervalWithLimit(builder.aPublishFailureTimeout,
                                                                                                     Long.MAX_VALUE,
                                                                                                     TimeUnit.NANOSECONDS);
        this.ackCacheCleanupInterval = ObjectUtils.checkNonNullPositiveTimeInterval(builder.ackCacheCleanupInterval);
        this.ackCacheSize = ObjectUtils.requiredInRange(builder.ackCacheSize, 0, Integer.MAX_VALUE,
                                                        IllegalArgumentException::new);
    }
    public String getChannelName() { return channelName; }

    public Duration getApublishAutomaticFailureTimeout() { return aPublishAutomaticFailureTimeout; }

    public Properties getChannelProperties() {
        var props = new Properties();
        props.setProperty(ChannelDriverConfig.CHANNEL_ID_PROPERTY_NAME, getChannelName());
        return props;
    }

    public Duration getAckCacheCleanupInterval() { return ackCacheCleanupInterval; }

    public int getAckCacheSize() { return ackCacheSize; }

    public abstract static class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String channelName;
        private Duration aPublishFailureTimeout = DEFAULT_MSG_LOST_TIMEOUT;
        private Duration ackCacheCleanupInterval = DEFAULT_MSG_LOST_TIMEOUT;
        private int ackCacheSize = DEFAULT_ACK_CACHE_SIZE;

        public final BuilderT setChannelName(String channelName) {
            this.channelName = channelName;
            return getThis();
        }

        /**
         * Specify after how much time a not acknowledged message from {@link io.reacted.core.reactorsystem.ReActorRef#apublish}
         * should be automatically marked as completed as a failure
         * @param aPublishFailureTimeout the automatic failure timeout. Default {@link ChannelDriverConfig#DEFAULT_MSG_LOST_TIMEOUT}
         *                            Max Value: {@link Long#MAX_VALUE} nanosecs
         * @return this {@link Builder}
         */
        public final BuilderT setApublishAutomaticFailureAfterTimeout(Duration aPublishFailureTimeout) {
            this.aPublishFailureTimeout = aPublishFailureTimeout;
            return getThis();
        }

        /**
         * Specify how often the cache used for keeping the pending acks triggers for {@link io.reacted.core.reactorsystem.ReActorRef#apublish}
         * should be checked for maintenance
         *
         * @param ackCacheCleanupInterval Cache cleanup rate. Positive intervals only, default: {@link ChannelDriverConfig#DEFAULT_MSG_LOST_TIMEOUT}
         * @return this {@link Builder}
         */
        public final BuilderT setAckCacheCleanupInterval(Duration ackCacheCleanupInterval) {
            this.ackCacheCleanupInterval = ackCacheCleanupInterval;
            return getThis();
        }

        /**
         * Gives a hint regarding the size of the ack cache for this driver
         * @param ackCacheSize A positive integer. Default {@link ChannelDriverConfig#DEFAULT_ACK_CACHE_SIZE}
         * @return this {@link Builder}
         */
        public final BuilderT setAckCacheSize(int ackCacheSize) {
            this.ackCacheSize = ackCacheSize;
            return getThis();
        }
    }
}
