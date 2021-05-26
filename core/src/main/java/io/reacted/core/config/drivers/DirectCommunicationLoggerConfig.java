/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;

@NonNullByDefault
public class DirectCommunicationLoggerConfig extends ChannelDriverConfig<DirectCommunicationLoggerConfig.Builder, DirectCommunicationLoggerConfig> {
    private final String logFilePath;
    private DirectCommunicationLoggerConfig(Builder builder) {
        super(builder);
        this.logFilePath = Objects.requireNonNull(builder.logFilePath,
                                                  "Log filepath cannot be null");
    }

    public String getLogFilePath() { return logFilePath; }

    public static DirectCommunicationLoggerConfig.Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverConfig.Builder<DirectCommunicationLoggerConfig.Builder, DirectCommunicationLoggerConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String logFilePath;
        private Builder() { }

        public Builder setLogFilePath(String logFilePath) {
            this.logFilePath = logFilePath;
            return this;
        }
        @Override
        public DirectCommunicationLoggerConfig build() {
            return new DirectCommunicationLoggerConfig(this);
        }
    }
}
