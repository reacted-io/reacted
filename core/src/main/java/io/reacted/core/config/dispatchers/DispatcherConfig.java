/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.dispatchers;

import com.google.common.base.Strings;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class DispatcherConfig {

    private final int batchSize;
    private final int dispatcherThreadsNum;
    private final String dispatcherName;

    private DispatcherConfig(Builder builder) {
        this.batchSize = ObjectUtils.requiredInRange(builder.batchSize, 1, Integer.MAX_VALUE,
                                                     IllegalArgumentException::new);
        this.dispatcherThreadsNum = ObjectUtils.requiredInRange(builder.dispatcherThreadsNum, 1, Integer.MAX_VALUE,
                                                                IllegalArgumentException::new);
        this.dispatcherName = Objects.requireNonNull(Strings.isNullOrEmpty(builder.dispatcherName)
                                                     ? null
                                                     : builder.dispatcherName);
    }

    public int getBatchSize() { return batchSize; }

    public int getDispatcherThreadsNum() { return dispatcherThreadsNum; }

    public String getDispatcherName() { return dispatcherName; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private int batchSize;
        private int dispatcherThreadsNum;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String dispatcherName;

        private Builder() {
        }

        /**
         * A dispatcher processes messages from a reactor mailbox. How many of them in a row? This
         * parameter defines that
         *
         * @param batchSize Number of messages that should be processed at maximum for a single reactor for a single
         *                  scheduling request
         * @return this builder
         */
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * A dispatcher can process as many reactors in parallel as many thread it has
         *
         * @param dispatcherThreadsNum How many thread should be allocated to this dispatcher
         * @return this builder
         */
        public Builder setDispatcherThreadsNum(int dispatcherThreadsNum) {
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
        public Builder setDispatcherName(String dispatcherName) {
            this.dispatcherName = dispatcherName;
            return this;
        }

        public DispatcherConfig build() {
            return new DispatcherConfig(this);
        }
    }
}
