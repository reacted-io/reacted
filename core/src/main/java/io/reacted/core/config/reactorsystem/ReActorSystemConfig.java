/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactorsystem;

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactors.ServiceRegistryConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.reactors.systemreactors.SystemMonitor;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@NonNullByDefault
public class ReActorSystemConfig {
    public static final int MAX_DISPATCHER_CONFIGS = 100;
    public static final int DEFAULT_FANOUT_POOL_SIZE = 1;
    public static final LocalDriver<? extends ChannelDriverConfig<?, ?>> DEFAULT_LOCAL_DRIVER = SystemLocalDrivers.DIRECT_COMMUNICATION;
    public static final Duration SYSTEM_MONITOR_DEFAULT_REFRESH_RATE = Duration.ofSeconds(20);
    private final String reactorSystemName;
    private final boolean recordedExecution;
    private final int msgFanOutPoolSize;
    private final Duration systemMonitorRefreshInterval;
    private final LocalDriver<? extends ChannelDriverConfig<?, ?>> localDriver;
    private final Set<DispatcherConfig> dispatchersConfigs;
    private final Set<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> remotingDrivers;
    private final Set<ServiceRegistryDriver<? extends ServiceRegistryConfig.Builder<?, ?>,
                                            ? extends ServiceRegistryConfig<?, ?>>> serviceRegistryDrivers;

    private ReActorSystemConfig(Builder reactorSystemConfig) {
        this.reactorSystemName = Objects.requireNonNull(reactorSystemConfig.reactorSystemName);
        this.msgFanOutPoolSize = ObjectUtils.requiredInRange(reactorSystemConfig.msgFanOutPoolSize,
                                                             DEFAULT_FANOUT_POOL_SIZE, 10,
                                                             IllegalArgumentException::new);
        this.localDriver = Objects.requireNonNull(reactorSystemConfig.localDriver);
        this.recordedExecution = reactorSystemConfig.shallRecordExecution;
        ObjectUtils.requiredInRange(reactorSystemConfig.dispatcherConfigs.size(), 0, MAX_DISPATCHER_CONFIGS,
                                    IllegalArgumentException::new);
        this.dispatchersConfigs = Set.copyOf(reactorSystemConfig.dispatcherConfigs);
        this.remotingDrivers = Set.copyOf(reactorSystemConfig.remotingDrivers);
        this.serviceRegistryDrivers = Set.copyOf(reactorSystemConfig.serviceRegistryDrivers);
        this.systemMonitorRefreshInterval = ObjectUtils.checkNonNullPositiveTimeInterval(reactorSystemConfig.systemMonitorRefreshInterval);
    }

    public String getReActorSystemName() { return reactorSystemName; }

    public int getMsgFanOutPoolSize() { return msgFanOutPoolSize; }

    public LocalDriver<? extends ChannelDriverConfig<?, ?>> getLocalDriver() { return localDriver; }

    public boolean isRecordedExecution() { return recordedExecution; }

    public Set<DispatcherConfig> getDispatchersConfigs() { return dispatchersConfigs; }

    public Set<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> getRemotingDrivers() { return remotingDrivers; }

    public Set<ServiceRegistryDriver<? extends ServiceRegistryConfig.Builder<?, ?>,
                                     ? extends ServiceRegistryConfig<?, ?>>> getServiceRegistryDrivers() {
        return serviceRegistryDrivers;
    }

    public Duration getSystemMonitorRefreshInterval() { return systemMonitorRefreshInterval; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String reactorSystemName;
        private int msgFanOutPoolSize = DEFAULT_FANOUT_POOL_SIZE;
        private LocalDriver<? extends ChannelDriverConfig<?, ?>> localDriver = DEFAULT_LOCAL_DRIVER;
        private Duration systemMonitorRefreshInterval = SYSTEM_MONITOR_DEFAULT_REFRESH_RATE;
        private boolean shallRecordExecution;
        private final Set<DispatcherConfig> dispatcherConfigs = new HashSet<>();
        private final Set<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> remotingDrivers = new HashSet<>();
        private final Set<ServiceRegistryDriver<? extends ServiceRegistryConfig.Builder<?, ?>,
                                                ? extends ServiceRegistryConfig<?, ?>>> serviceRegistryDrivers = new HashSet<>();

        private Builder() { /* No implementation required */ }

        /**
         * @param reactorSystemName Must be unique in the cluster
         */
        public Builder setReactorSystemName(String reactorSystemName) {
            this.reactorSystemName = reactorSystemName;
            return this;
        }

        /**
         * Message fan out to passive subscribers use a dedicated thread pool.
         * This specifies how bit it is
         *
         * @param msgFanOutPoolSize numbers of threads that should be used.
         *                          Range 1 - 200
         */
        public Builder setMsgFanOutPoolSize(int msgFanOutPoolSize) {
            this.msgFanOutPoolSize = msgFanOutPoolSize;
            return this;
        }

        /**
         * Enable logs for cold replay. These logs are going to be generated
         * as system messages, so they must be recorded using a suitable local driver
         *
         * @param shallRecordExecution true if data for cold replay should be generated
         *                             during execution.
         */
        public Builder setRecordExecution(boolean shallRecordExecution) {
            this.shallRecordExecution = shallRecordExecution;
            return this;
        }

        /**
         * Defining how messages should be exchanged within a reactor system may be useful for
         * profiling, debugging, post mortem analysis or logs replay. This parameter allows to
         * choose how messages should be physically transmitted from the source to the target
         * mailbox within this reactor system
         *
         * @param localDriver driver that should be used for communication within this
         *                    reactor system
         */
        public Builder setLocalDriver(LocalDriver<? extends ChannelDriverConfig<?, ?>> localDriver) {
            this.localDriver = localDriver;
            return this;
        }

        /**
         * {@link SystemMonitor} is a system reactor that collects statistics about the state of the system and
         * propagates them among subscribers
         *
         * @param refreshInterval Period after which a refresh of the system statistics should be done
         * @return this builder
         */
        public Builder setSystemMonitorRefreshInterval(Duration refreshInterval) {
            this.systemMonitorRefreshInterval = refreshInterval;
            return this;
        }

        /**
         * @param dispatcherConfig new dispatcher config
         */
        public Builder addDispatcherConfig(DispatcherConfig dispatcherConfig) {
            this.dispatcherConfigs.add(dispatcherConfig);
            return this;
        }

        /**
         * Define which drivers/technologies can be used for communicating this reactor system from another
         * It's like the local driver, but for communications with other reactor systems
         *
         * @param remotingDriver remoting driver
         */
        public Builder addRemotingDriver(RemotingDriver<? extends ChannelDriverConfig<?, ?>> remotingDriver) {
            this.remotingDrivers.add(remotingDriver);
            return this;
        }

        /**
         * Connect the reactor system to the service registries instance specified by these drivers
         *
         * @param serviceRegistryDriver service registry driver
         */
        public Builder addServiceRegistryDriver(ServiceRegistryDriver<? extends ServiceRegistryConfig.Builder<?, ?>,
                ? extends ServiceRegistryConfig<?, ?>> serviceRegistryDriver) {
            this.serviceRegistryDrivers.add(serviceRegistryDriver);
            return this;
        }

        /**
         * @throws IllegalArgumentException if any of the supplied arguments do not comply with the provided boundaries
         */
        public ReActorSystemConfig build() {
            return new ReActorSystemConfig(this);
        }
    }
}
