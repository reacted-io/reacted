/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.serviceregistries.zookeeper;

import io.reacted.core.config.reactors.ServiceRegistryConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public class ZooKeeperDriverConfig extends ServiceRegistryConfig<ZooKeeperDriverConfig.Builder, ZooKeeperDriverConfig> {
    public static final String ZOOKEEPER_DEFAULT_CONNECTION_STRING = "localhost:2181";
    public static final Duration ZOOKEEPER_DEFAULT_PING_INTERVAL = Duration.ofSeconds(20);
    public static final Duration ZOOKEEPER_DEFAULT_SESSION_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration ZOOKEEPER_DEFAULT_CONNECTION_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration ZOOKEEPER_DEFAULT_RECONNECTION_DELAY = Duration.ofSeconds(2);
    public static final int ZOOKEEPER_DEFAULT_MAX_RECONNECTION_ATTEMPTS = 20;
    private final Duration pingInterval;
    private final Duration sessionTimeout;
    private final Duration connectionTimeout;
    private final Duration reconnectionDelay;
    private final Executor asyncExecutionService;
    private final int maxReconnectionAttempts;
    private final String connectionString;

    private ZooKeeperDriverConfig(Builder builder) {
        super(builder);
        this.pingInterval = ObjectUtils.checkNonNullPositiveTimeInterval(builder.pingInterval);
        this.sessionTimeout = ObjectUtils.checkNonNullPositiveTimeIntervalWithLimit(builder.sessionTimeout,
                                                                                    Integer.MAX_VALUE,
                                                                                    TimeUnit.MILLISECONDS);
        this.connectionTimeout = ObjectUtils.checkNonNullPositiveTimeIntervalWithLimit(builder.connectionTimeout,
                                                                                       Integer.MAX_VALUE,
                                                                                       TimeUnit.MILLISECONDS);
        this.reconnectionDelay = ObjectUtils.checkNonNullPositiveTimeIntervalWithLimit(builder.reconnectionDelay,
                                                                                       Integer.MAX_VALUE,
                                                                                       TimeUnit.MILLISECONDS);
        this.maxReconnectionAttempts = ObjectUtils.requiredInRange(builder.maxReconnectionAttempts,
                                                                   0, Integer.MAX_VALUE,
                                                                   () -> new IllegalArgumentException("Invalid max reconnection attempts value"));
        this.asyncExecutionService = Objects.requireNonNull(builder.asyncExecutorService);
        this.connectionString = Objects.requireNonNull(builder.connectionString);
    }

    public Duration getPingInterval() { return pingInterval; }

    public Executor getAsyncExecutionService() { return asyncExecutionService; }

    public Duration getSessionTimeout() { return sessionTimeout; }

    public Duration getConnectionTimeout() { return connectionTimeout; }

    public Duration getReconnectionDelay() { return reconnectionDelay; }

    public int getMaxReconnectionAttempts() { return maxReconnectionAttempts; }

    public String getConnectionString() { return connectionString; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ServiceRegistryConfig.Builder<Builder, ZooKeeperDriverConfig> {
        private String connectionString = ZOOKEEPER_DEFAULT_CONNECTION_STRING;
        private Duration pingInterval = ZOOKEEPER_DEFAULT_PING_INTERVAL;
        private Duration sessionTimeout = ZOOKEEPER_DEFAULT_SESSION_TIMEOUT;
        private Duration connectionTimeout = ZOOKEEPER_DEFAULT_CONNECTION_TIMEOUT;
        private Duration reconnectionDelay = ZOOKEEPER_DEFAULT_RECONNECTION_DELAY;
        private int maxReconnectionAttempts = ZOOKEEPER_DEFAULT_MAX_RECONNECTION_ATTEMPTS;
        private Executor asyncExecutorService = ForkJoinPool.commonPool();

        private Builder() {
            setTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class));
        }

        /**
         * Specify after how often a ping should be sent to Zookeeper
         *
         * @param pingInterval ping delay. A positive amount no bigger than {@link Integer#MAX_VALUE} {@link TimeUnit#MILLISECONDS}
         *                     Default {@link ZooKeeperDriverConfig#ZOOKEEPER_DEFAULT_PING_INTERVAL}
         * @return this builder
         */
        public final Builder setPingInterval(Duration pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        /**
         * Sets the timeout for the zookeeper session.
         *
         * @param sessionTimeout A positive amount no bigger than {@link Integer#MAX_VALUE} {@link TimeUnit#MILLISECONDS}
         *                       Default: {@link ZooKeeperDriverConfig#ZOOKEEPER_DEFAULT_SESSION_TIMEOUT}
         * @return this builder
         */
        public final Builder setSessionTimeout(Duration sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        /**
         * Sets the timeout for the zookeeper connection.
         * @param connectionTimeout A positive amount no bigger than {@link Integer#MAX_VALUE} {@link TimeUnit#MILLISECONDS}
         *                          Default: {@link ZooKeeperDriverConfig#ZOOKEEPER_DEFAULT_CONNECTION_TIMEOUT}
         * @return this builder
         */
        public final Builder setConnectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Define a custom executor service for executing asynchronous operations
         * @param asyncExecutor Alternate executor. Default: {@link ForkJoinPool#commonPool()}
         * @return this builder
         */
        public Builder setAsyncExecutor(Executor asyncExecutor) {
            this.asyncExecutorService = asyncExecutor;
            return this;
        }

        /**
         * Defines the base for the exponential backoff reconnection system
         * @param reconnectionDelay Exponential backoff base.
         *                          A positive amount no bigger than {@link Integer#MAX_VALUE} {@link TimeUnit#MILLISECONDS}
         * @return this builder
         */
        public Builder setReconnectionDelay(Duration reconnectionDelay) {
            this.reconnectionDelay = reconnectionDelay;
            return this;
        }

        /**
         * Defines the maximum amount of reattempts that should be performed during the exponential backoff cycle
         * before permanently giving up
         * @param maxReconnectionAttempts A positive amount
         * @return this builder
         */
        public Builder setMaxReconnectionAttempts(int maxReconnectionAttempts) {
            this.maxReconnectionAttempts = maxReconnectionAttempts;
            return this;
        }

        /**
         * Set the connection string for connecting to ZooKeeper
         * @param connectionString connection string for connecting to ZooKeeper.
         *                         Default {@link ZooKeeperDriverConfig#ZOOKEEPER_DEFAULT_CONNECTION_STRING}
         * @return this builder
         */
        public Builder setConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        /**
         * @throws IllegalArgumentException is any of the supplied parameters is not within the specified boundaries
         */
        public ZooKeeperDriverConfig build() {
            return new ZooKeeperDriverConfig(this);
        }
    }
}
