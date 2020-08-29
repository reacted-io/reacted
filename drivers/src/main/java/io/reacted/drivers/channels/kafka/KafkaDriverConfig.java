/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.config.ConfigUtils;
import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;

@NonNullByDefault
public class KafkaDriverConfig extends ReActedDriverCfg<KafkaDriverConfig.Builder, KafkaDriverConfig> {
    private final String bootstrapEndpoint;
    private final String topic;
    private final String groupId;
    private final int maxPollRecords;

    private KafkaDriverConfig(Builder builder) {
        super(builder);
        this.bootstrapEndpoint = Objects.requireNonNull(builder.bootstrapEndpoint);
        this.topic = Objects.requireNonNull(builder.topic);
        this.groupId = Objects.requireNonNull(builder.groupId);
        this.maxPollRecords = ConfigUtils.requiredInRange(builder.maxPollRecords, 1, Integer.MAX_VALUE,
                                                          IllegalArgumentException::new);
    }

    public String getBootstrapEndpoint() { return bootstrapEndpoint; }

    public String getTopic() { return topic; }

    public String getGroupId() { return groupId; }

    public int getMaxPollRecords() { return maxPollRecords; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ReActedDriverCfg.Builder<KafkaDriverConfig.Builder, KafkaDriverConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String bootstrapEndpoint;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String topic;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String groupId;
        private int maxPollRecords;

        private Builder() { }

        public Builder setBootstrapEndpoint(String bootstrapEndpoint) {
            this.bootstrapEndpoint = bootstrapEndpoint;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder setMaxPollRecords(int maxPollRecords) {
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public KafkaDriverConfig build() {
            return new KafkaDriverConfig(this);
        }
    }
}
