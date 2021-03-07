/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
import java.util.Properties;

@NonNullByDefault
public class KafkaDriverConfig extends ChannelDriverConfig<KafkaDriverConfig.Builder, KafkaDriverConfig> {
    public static final String KAFKA_BOOTSTRAP_ENDPOINT = "bootstrapEndpoint";
    public static final String KAFKA_TOPIC = "topic";
    public static final String KAFKA_GROUP_ID = "groupId";
    public static final String KAFKA_MAX_POLL_RECORDS = "maxPollRecords";
    private final String bootstrapEndpoint;
    private final String topic;
    private final String groupId;
    private final int maxPollRecords;

    private KafkaDriverConfig(Builder builder) {
        super(builder);
        this.bootstrapEndpoint = Objects.requireNonNull(builder.bootstrapEndpoint,
                                                        "Bootstrap endpoint cannot be null");
        this.topic = Objects.requireNonNull(builder.topic,
                                            "Subscription topic cannot be null");
        this.groupId = Objects.requireNonNull(builder.groupId,
                                              "Group id cannot be null");
        this.maxPollRecords = ObjectUtils.requiredInRange(builder.maxPollRecords, 1, Integer.MAX_VALUE,
                                                          IllegalArgumentException::new);
    }

    public String getBootstrapEndpoint() { return bootstrapEndpoint; }

    public String getTopic() { return topic; }

    public String getGroupId() { return groupId; }

    public int getMaxPollRecords() { return maxPollRecords; }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public Properties getChannelProperties() {
        Properties properties = new Properties();
        properties.setProperty(KAFKA_BOOTSTRAP_ENDPOINT, getBootstrapEndpoint());
        properties.setProperty(KAFKA_GROUP_ID, getGroupId());
        properties.setProperty(KAFKA_TOPIC, getTopic());
        properties.setProperty(KAFKA_MAX_POLL_RECORDS, getMaxPollRecords() + "");
        return properties;
    }

    public static class Builder extends ChannelDriverConfig.Builder<KafkaDriverConfig.Builder, KafkaDriverConfig> {
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
