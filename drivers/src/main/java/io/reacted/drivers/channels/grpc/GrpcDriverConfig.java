/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.grpc;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
import java.util.Properties;

@NonNullByDefault
public class GrpcDriverConfig extends ChannelDriverConfig<GrpcDriverConfig.Builder, GrpcDriverConfig> {
    public static final String GRPC_PORT = "port";
    public static final String GRPC_HOST = "hostName";

    private final String hostName;
    private final int port;

    private GrpcDriverConfig(Builder builder) {
        super(builder);
        this.port = ObjectUtils.requiredInRange(builder.port, 1, 65535, IllegalArgumentException::new);
        this.hostName = Objects.requireNonNull(builder.hostName,
                                               "Bind address/hostname cannot be null");
    }

    public int getPort() { return port; }

    public String getHostName() { return hostName; }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public Properties getChannelProperties() {
        Properties properties = new Properties();
        properties.setProperty(GRPC_PORT, getPort() + "");
        properties.setProperty(GRPC_HOST, getHostName());
        return properties;
    }

    public static class Builder extends ChannelDriverConfig.Builder<Builder, GrpcDriverConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String hostName;
        private int port;

        private Builder() { }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public final Builder setHostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public final GrpcDriverConfig build() {
            return new GrpcDriverConfig(this);
        }
    }
}
