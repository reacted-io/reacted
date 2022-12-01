/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.dispatchers;

import com.google.common.base.Strings;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
@NonNullByDefault
public class DispatcherConfig {
    public static final int DEFAULT_DISPATCHER_BATCH_SIZE = 10;
    public static final int DEFAULT_DISPATCHER_THREAD_NUM = 1;
    public static final String NULL_DISPATCHER_NAME = "NULL_DISPATCHER";
    public static final DispatcherConfig NULL_DISPATCHER_CFG = new DispatcherConfig();
    private final int batchSize;
    private final int dispatcherThreadsNum;
    private final String dispatcherName;

    private DispatcherConfig() {
        this.batchSize = 0;
        this.dispatcherThreadsNum = 0;
        this.dispatcherName = NULL_DISPATCHER_NAME;
    }
    private DispatcherConfig(Builder builder) {
        this.batchSize = ObjectUtils.requiredInRange(builder.batchSize, 1, Integer.MAX_VALUE,
                                                     () -> new IllegalArgumentException("Dispatcher batch size required"));
        this.dispatcherThreadsNum = ObjectUtils.requiredCondition(ObjectUtils.requiredInRange(builder.dispatcherThreadsNum,
                                                                                              1, Integer.MAX_VALUE,
                                                                                              () -> new IllegalArgumentException("Dispatcher threads must be greater than 0")),
                                                                  value -> (value & (value - 1)) == 0,
                                                                  () -> new IllegalArgumentException("Dispatcher threads must be a power of 2"));
        this.dispatcherName = Objects.requireNonNull(Strings.isNullOrEmpty(builder.dispatcherName)
                                                     ? null
                                                     : builder.dispatcherName,
                                                     "Dispatcher name cannot be null or empty");
    }
    public int getBatchSize() { return batchSize; }

    public int getDispatcherThreadsNum() { return dispatcherThreadsNum; }

    public String getDispatcherName() { return dispatcherName; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private int batchSize = DEFAULT_DISPATCHER_BATCH_SIZE;
        private int dispatcherThreadsNum = DEFAULT_DISPATCHER_THREAD_NUM;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String dispatcherName;

        private Builder() {
        }

        /**
         * A dispatcher processes messages from a reactor mailbox. How many of them in a row? This
         * parameter defines that. Default value: {@link #DEFAULT_DISPATCHER_BATCH_SIZE}
         *
         * @param batchSize A positive number indicating the number of messages that should be
         *                  processed at maximum for a single reactor for a single
         *                  scheduling request
         * @return this builder
         */
        public final Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * A dispatcher can process as many reactors in parallel as many thread it has.
         * Default: {@link #DEFAULT_DISPATCHER_THREAD_NUM}
         *
         * @param dispatcherThreadsNum A positive number indicating how many thread should be
         *                             allocated to this dispatcher. Must be a power of 2
         * @return this builder
         */
        public final Builder setDispatcherThreadsNum(int dispatcherThreadsNum) {
            this.dispatcherThreadsNum = dispatcherThreadsNum;
            return this;
        }

        /**
         * Dispatcher name must be unique in a reactor system
         *
         * @param dispatcherName Unique name of a dispatcher. Reactors can request to be scheduled on this
         *                       dispatcher specifying this name
         * @return this builder
         */
        public final Builder setDispatcherName(String dispatcherName) {
            this.dispatcherName = dispatcherName;
            return this;
        }

        public DispatcherConfig build() {
            return new DispatcherConfig(this);
        }
    }
}
