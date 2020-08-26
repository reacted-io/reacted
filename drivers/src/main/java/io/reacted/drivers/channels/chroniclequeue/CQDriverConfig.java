/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.patterns.NonNullByDefault;
import net.openhft.chronicle.wire.WireKey;

import java.util.Objects;

@NonNullByDefault
public class CQDriverConfig extends ReActedDriverCfg<CQDriverConfig.Builder, CQDriverConfig> {

    private final String chronicleFilesDir;
    private final String topicName;

    private CQDriverConfig(Builder configBuilder) {
        super(configBuilder);
        this.chronicleFilesDir = Objects.requireNonNull(configBuilder.chronicleFilesDir);
        this.topicName = Objects.requireNonNull(configBuilder.topicName);
    }

    public static Builder newBuilder() { return new Builder(); }

    public String getChronicleFilesDir() { return chronicleFilesDir; }

    public WireKey getTopic() { return () -> topicName; }

    public static class Builder extends ReActedDriverCfg.Builder<Builder, CQDriverConfig> {
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
