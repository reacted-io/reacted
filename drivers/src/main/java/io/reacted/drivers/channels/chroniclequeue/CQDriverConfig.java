/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.patterns.NonNullByDefault;
import net.openhft.chronicle.wire.WireKey;

import java.util.Objects;
import java.util.Properties;

@NonNullByDefault
public class CQDriverConfig extends ChannelDriverConfig<CQDriverConfig.Builder, CQDriverConfig> {

    public static final String CQ_FILES_DIRECTORY = "chronicleFilesDir";
    public static final String CQ_TOPIC_NAME = "topicName";
    private final String chronicleFilesDir;
    private final String topicName;

    private final WireKey topicGetter;

    private CQDriverConfig(Builder configBuilder) {
        super(configBuilder);
        this.chronicleFilesDir = Objects.requireNonNull(configBuilder.chronicleFilesDir,
                                                        "Output directory cannot be null");
        this.topicName = Objects.requireNonNull(configBuilder.topicName,
                                                "Topic name cannot be null");
        this.topicGetter = () -> topicName;
    }

    public static Builder newBuilder() { return new Builder(); }

    public String getChronicleFilesDir() { return chronicleFilesDir; }

    public WireKey getTopic() { return topicGetter; }

    @Override
    public Properties getChannelProperties() {
        Properties properties = new Properties();
        properties.setProperty(CQ_FILES_DIRECTORY, getChronicleFilesDir());
        properties.setProperty(CQ_TOPIC_NAME, topicName);
        return properties;
    }

    public static class Builder extends ChannelDriverConfig.Builder<Builder, CQDriverConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String chronicleFilesDir;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String topicName;

        private Builder() { }

        public Builder setChronicleFilesDir(String chronicleFilesDir) {
            this.chronicleFilesDir = chronicleFilesDir;
            return this;
        }

        public Builder setTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public CQDriverConfig build() { return new CQDriverConfig(this); }
    }
}
