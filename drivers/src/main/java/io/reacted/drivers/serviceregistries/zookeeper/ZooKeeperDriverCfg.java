/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.serviceregistries.zookeeper;

import io.reacted.core.config.reactors.ServiceRegistryCfg;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.time.Duration;

@NonNullByDefault
public class ZooKeeperDriverCfg extends ServiceRegistryCfg<ZooKeeperDriverCfg.Builder, ZooKeeperDriverCfg> {
    public static final Duration ZOOKEEPER_DEFAULT_REATTEMPT_ON_FAILURE_INTERVAL = Duration.ofMinutes(1);
    private final Duration reattemptOnFailureInterval;

    private ZooKeeperDriverCfg(Builder builder) {
        super(builder);
        this.reattemptOnFailureInterval = ObjectUtils.checkNonNullPositiveTimeInterval(builder.reattemptOnFailureInterval);
    }

    public Duration getReattemptOnFailureInterval() {
        return reattemptOnFailureInterval;
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ServiceRegistryCfg.Builder<Builder, ZooKeeperDriverCfg> {
        private Duration reattemptOnFailureInterval = ZOOKEEPER_DEFAULT_REATTEMPT_ON_FAILURE_INTERVAL;
        private Builder() { }

        public final Builder setReattemptOnFailureInterval(Duration reattemptOnFailureInterval) {
            this.reattemptOnFailureInterval = reattemptOnFailureInterval;
            return this;
        }

        public ZooKeeperDriverCfg build() {
            return new ZooKeeperDriverCfg(this);
        }
    }
}
