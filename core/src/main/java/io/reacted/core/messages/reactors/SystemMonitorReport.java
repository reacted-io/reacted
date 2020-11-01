/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.core.utils.ObjectUtils;
import java.io.Serializable;

public class SystemMonitorReport implements Serializable {
    private final double cpuLoad;
    private final long freeMemorySize;

    private SystemMonitorReport(Builder builder) {
        this.cpuLoad = ObjectUtils.requiredCondition(builder.cpuLoad,
                                                     load -> !Double.isNaN(load),
                                                     () -> new IllegalArgumentException("CPU Load is NaN!"));
        this.freeMemorySize = ObjectUtils.requiredInRange(builder.freeMemorySize,
                                                          0L, Long.MAX_VALUE,
                                                          () -> new IllegalArgumentException("Free memory has a " +
                                                                                             "negative size" +
                                                                                             builder.freeMemorySize));
    }

    public double getCpuLoad() {
        return cpuLoad;
    }

    public long getFreeMemorySize() {
        return freeMemorySize;
    }

    @Override
    public String toString() {
        return "SystemMonitorReport{" + "cpuLoad=" + cpuLoad + ", freeMemorySize=" + freeMemorySize + '}';
    }
    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private double cpuLoad = Double.NaN;
        private long freeMemorySize;
        private Builder() { /* No implementation required */ }

        public Builder setCpuLoad(double cpuLoad) {
            this.cpuLoad = cpuLoad;
            return this;
        }

        public Builder setFreeMemorySize(long freeMemorySize) {
            this.freeMemorySize = freeMemorySize;
            return this;
        }

        public SystemMonitorReport build() {
            return new SystemMonitorReport(this);
        }
    }
}
