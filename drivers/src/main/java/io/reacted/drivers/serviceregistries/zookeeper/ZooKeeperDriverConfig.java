/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.serviceregistries.zookeeper;

import io.reacted.core.config.reactors.ServiceRegistryConfig;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

@NonNullByDefault
public class ZooKeeperDriverConfig extends ServiceRegistryConfig<ZooKeeperDriverConfig.Builder, ZooKeeperDriverConfig> {
    public static final Duration ZOOKEEPER_DEFAULT_PING_INTERVAL = Duration.ofSeconds(20);
    private final Duration pingInterval;
    private final Executor asyncExecutionService;
    private ZooKeeperDriverConfig(Builder builder) {
        super(builder);
        this.pingInterval = ObjectUtils.checkNonNullPositiveTimeInterval(builder.pingInterval);
        this.asyncExecutionService = Objects.requireNonNull(builder.asyncExecutorService);
    }

    public Duration getPingInterval() {
        return pingInterval;
    }

    public Executor getAsyncExecutionService() { return asyncExecutionService; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ServiceRegistryConfig.Builder<Builder, ZooKeeperDriverConfig> {
        private Duration pingInterval = ZOOKEEPER_DEFAULT_PING_INTERVAL;
        private Executor asyncExecutorService = ForkJoinPool.commonPool();
        private Builder() { }

        /**
         * Specify after how often a ping should be sent to Zookeeper
         *
         * @param pingInterval ping delay. Positive delay only. Default {@link ZooKeeperDriverConfig#ZOOKEEPER_DEFAULT_PING_INTERVAL}
         * @return this builder
         */
        public final Builder setPingInterval(Duration pingInterval) {
            this.pingInterval = pingInterval;
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

        public ZooKeeperDriverConfig build() {
            return new ZooKeeperDriverConfig(this);
        }
    }
}
