/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactorsystem;

import io.reacted.core.config.ConfigUtils;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@NonNullByDefault
public class ReActorSystemConfig {

    private final String reactorSystemName;
    private final boolean recordedExecution;
    private final int msgFanOutPoolSize;
    private final LocalDriver localDriver;
    private final Set<DispatcherConfig> dispatchersConfigs;
    private final Set<RemotingDriver> remotingDrivers;
    private final Set<ServiceRegistryDriver> serviceRegistryDrivers;
    private final Duration askTimeoutsCleanupInterval;

    private ReActorSystemConfig(Builder reactorSystemConfig) {
        this.reactorSystemName = Objects.requireNonNull(reactorSystemConfig.reactorSystemName);
        this.msgFanOutPoolSize = ConfigUtils.requiredInRange(reactorSystemConfig.msgFanOutPoolSize, 1, 10,
                                                             IllegalArgumentException::new);
        this.localDriver = Objects.requireNonNull(reactorSystemConfig.localDriver);
        this.recordedExecution = reactorSystemConfig.shallRecordExecution;
        ConfigUtils.requiredInRange(reactorSystemConfig.dispatcherConfigs.size(), 0, 100,
                                    IllegalArgumentException::new);
        this.dispatchersConfigs = Set.copyOf(reactorSystemConfig.dispatcherConfigs);
        this.remotingDrivers = Set.copyOf(reactorSystemConfig.remotingDrivers);
        this.serviceRegistryDrivers = Set.copyOf(reactorSystemConfig.serviceRegistryDrivers);
        this.askTimeoutsCleanupInterval = Objects.requireNonNull(reactorSystemConfig.askTimeoutsCleanupInterval);
    }

    public String getReActorSystemName() { return reactorSystemName; }

    public int getMsgFanOutPoolSize() { return msgFanOutPoolSize; }

    public LocalDriver getLocalDriver() { return localDriver; }

    public boolean isRecordedExecution() { return recordedExecution; }

    public Set<DispatcherConfig> getDispatchersConfigs() { return dispatchersConfigs; }

    public Set<RemotingDriver> getRemotingDrivers() { return remotingDrivers; }

    public Set<ServiceRegistryDriver> getServiceRegistryDrivers() { return serviceRegistryDrivers; }

    public Duration getAskTimeoutsCleanupInterval() { return askTimeoutsCleanupInterval; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String reactorSystemName;
        private int msgFanOutPoolSize = 1;
        private LocalDriver localDriver = SystemLocalDrivers.DIRECT_COMMUNICATION;
        private boolean shallRecordExecution;
        private Duration askTimeoutsCleanupInterval = Duration.ofSeconds(10);
        private final Set<DispatcherConfig> dispatcherConfigs = new HashSet<>();
        private final Set<RemotingDriver> remotingDrivers = new HashSet<>();
        private final Set<ServiceRegistryDriver> serviceRegistryDrivers = new HashSet<>();

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
        public Builder setLocalDriver(LocalDriver localDriver) {
            this.localDriver = localDriver;
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
        public Builder addRemotingDriver(RemotingDriver remotingDriver) {
            this.remotingDrivers.add(remotingDriver);
            return this;
        }

        /**
         * Connect the reactor system to the service registries instance specified by these drivers
         *
         * @param serviceRegistryDriver service registry driver
         */
        public Builder addServiceRegistryDriver(ServiceRegistryDriver serviceRegistryDriver) {
            this.serviceRegistryDrivers.add(serviceRegistryDriver);
            return this;
        }

        /**
         * Sending a message and getting a reply from a reactor can be done from outside a reactor
         * context using an Ask. Ask primitive supports a timeout before automatically expiring and
         * assuming that a reply will never arrive. Such timeout is backed up by a java timer that
         * requires periodic maintenance to remove expired entries. This parameter defines the
         * cleanup period.
         *
         * @param askTimeoutsCleanupInterval timer purge period
         */
        public Builder setAskTimeoutsCleanupInterval(Duration askTimeoutsCleanupInterval) {
            this.askTimeoutsCleanupInterval = askTimeoutsCleanupInterval;
            return this;
        }

        public ReActorSystemConfig build() {
            return new ReActorSystemConfig(this);
        }
    }
}
