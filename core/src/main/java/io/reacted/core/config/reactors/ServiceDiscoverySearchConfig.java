/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.services.SelectionType;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

@NonNullByDefault
public class ServiceDiscoverySearchConfig extends InheritableBuilder<ServiceDiscoverySearchConfig.Builder,
                                                                     ServiceDiscoverySearchConfig>
        implements Serializable {

    private final String serviceName;
    private final SelectionType selectionType;
    private final double systemLoad;
    @Nullable
    private final ChannelId.ChannelType channelType;
    @Nullable
    private final InetAddress ipAddress;
    @Nullable
    private final Pattern hostName;

    private ServiceDiscoverySearchConfig(Builder builder) {
        super(builder);
        this.serviceName = Objects.requireNonNull(builder.serviceName);
        this.selectionType = Objects.requireNonNull(builder.selectionType);
        this.systemLoad = builder.systemLoad;
        this.ipAddress = builder.ipAddress;
        this.hostName = builder.hostName;
        this.channelType = builder.channelType;
    }

    public String getServiceName() { return serviceName; }

    public SelectionType getSelectionType() { return selectionType; }

    public double getSystemLoad() { return systemLoad; }

    public Optional<ChannelId.ChannelType> getChannelType() { return Optional.ofNullable(channelType); }

    public Optional<InetAddress> getIpAddress() { return Optional.ofNullable(ipAddress); }

    public Optional<Pattern> getHostName() { return Optional.ofNullable(hostName); }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends InheritableBuilder.Builder<Builder, ServiceDiscoverySearchConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String serviceName;
        private SelectionType selectionType = SelectionType.ROUTED;
        private double systemLoad = Double.NaN;
        @Nullable
        private ChannelId.ChannelType channelType;
        @Nullable
        private InetAddress ipAddress;
        @Nullable
        private Pattern hostName;

        private Builder() { /* Nothing to do */ }

        public Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return getThis();
        }

        public Builder setIpAddress(@Nullable InetAddress ipAddress) {
            this.ipAddress = ipAddress;
            return getThis();
        }

        public Builder setHostName(Pattern hostName) {
            this.hostName = hostName;
            return getThis();
        }

        public Builder setSystemLoad(double systemLoad) {
            this.systemLoad = systemLoad;
            return getThis();
        }

        public Builder setSelectionType(SelectionType selectionType) {
            this.selectionType = selectionType;
            return getThis();
        }

        private Builder setChannelType(ChannelId.ChannelType channelType) {
            this.channelType = channelType;
            return getThis();
        }

        @Override
        public ServiceDiscoverySearchConfig build() {
            return new ServiceDiscoverySearchConfig(this);
        }
    }
}
