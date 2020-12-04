/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;

@NonNullByDefault
public class DirectCommunicationSimplifiedLoggerConfig extends ChannelDriverConfig<DirectCommunicationSimplifiedLoggerConfig.Builder, DirectCommunicationSimplifiedLoggerConfig> {
    private final String logFilePath;
    private DirectCommunicationSimplifiedLoggerConfig(Builder builder) {
        super(builder);
        this.logFilePath = Objects.requireNonNull(builder.logFilePath,
                                                  "Log filepath cannot be null");
    }

    public String getLogFilePath() { return logFilePath; }

    public static DirectCommunicationSimplifiedLoggerConfig.Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverConfig.Builder<DirectCommunicationSimplifiedLoggerConfig.Builder, DirectCommunicationSimplifiedLoggerConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String logFilePath;
        private Builder() { }

        public Builder setLogFilePath(String logFilePath) {
            this.logFilePath = logFilePath;
            return this;
        }

        @Override
        public DirectCommunicationSimplifiedLoggerConfig build() {
            return new DirectCommunicationSimplifiedLoggerConfig(this);
        }
    }
}
