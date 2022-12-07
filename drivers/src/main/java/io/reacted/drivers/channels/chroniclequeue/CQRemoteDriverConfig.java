/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.patterns.NonNullByDefault;
import net.openhft.chronicle.wire.WireKey;

import java.util.Objects;
import java.util.Properties;

@NonNullByDefault
public class CQRemoteDriverConfig extends CQDriverConfig<CQRemoteDriverConfig.Builder, CQRemoteDriverConfig> {
    public static final String CQ_TOPIC_NAME = "topicName";
    private final String topicName;
    private final WireKey topicGetter;

    private CQRemoteDriverConfig(Builder configBuilder) {
        super(configBuilder);
        this.topicName = Objects.requireNonNull(configBuilder.topicName,
                                                "Topic name cannot be null");
        this.topicGetter = () -> topicName;
    }

    public static Builder newBuilder() { return new Builder(); }

    public WireKey getTopic() { return topicGetter; }

    @Override
    public Properties getChannelProperties() {
        Properties properties = super.getChannelProperties();
        properties.setProperty(CQ_TOPIC_NAME, topicName);
        return properties;
    }

    public static class Builder extends CQDriverConfig.Builder<Builder, CQRemoteDriverConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String topicName;
        private Builder() { }

        public Builder setTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public CQRemoteDriverConfig build() { return new CQRemoteDriverConfig(this); }
    }
}
