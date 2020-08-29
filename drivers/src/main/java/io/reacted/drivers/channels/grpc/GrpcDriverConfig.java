/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.grpc;

import io.reacted.core.config.ConfigUtils;
import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;

@NonNullByDefault
public class GrpcDriverConfig extends ReActedDriverCfg<GrpcDriverConfig.Builder, GrpcDriverConfig> {
    public static final String PORT_PROPERTY_NAME = "port";
    public static final String HOST_PROPERTY_NAME = "hostName";

    private final String hostName;
    private final int port;

    private GrpcDriverConfig(Builder builder) {
        super(builder);
        this.port = ConfigUtils.requiredInRange(builder.port, 1, 65535, IllegalArgumentException::new);
        this.hostName = Objects.requireNonNull(builder.hostName);
    }

    public int getPort() { return port; }

    public String getHostName() { return hostName; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ReActedDriverCfg.Builder<Builder, GrpcDriverConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String hostName;
        private int port;

        private Builder() { }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public GrpcDriverConfig build() {
            return new GrpcDriverConfig(this);
        }
    }
}
