/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import com.google.common.collect.Range;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.services.SelectionType;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

@NonNullByDefault
public class BasicServiceDiscoverySearchFilter extends InheritableBuilder<BasicServiceDiscoverySearchFilter.Builder,
                                                                          BasicServiceDiscoverySearchFilter>
        implements Serializable, ServiceDiscoverySearchFilter {
    public static final String FIELD_NAME_SERVICE_NAME = "serviceName";
    public static final String FIELD_NAME_CPU_LOAD = "cpuLoad";
    public static final String FIELD_NAME_FREE_MEMORY_SIZE = "freeMemorySize";
    public static final String FIELD_NAME_CHANNEL_TYPE = "channelId";
    public static final String FIELD_NAME_IP_ADDRESS = "ipAddress";
    public static final String FIELD_NAME_HOSTNAME = "hostName";

    private final String serviceName;
    private final SelectionType selectionType;
    @Nullable
    private final Range<Double> cpuLoad;
    @Nullable
    private final ChannelId channelId;
    @Nullable
    private final InetAddress ipAddress;
    @Nullable
    private final Pattern hostName;

    protected BasicServiceDiscoverySearchFilter(Builder builder) {
        super(builder);
        this.serviceName = Objects.requireNonNull(builder.serviceName);
        this.selectionType = Objects.requireNonNull(builder.selectionType);
        this.cpuLoad = builder.cpuLoad;
        this.ipAddress = builder.ipAddress;
        this.hostName = builder.hostName;
        this.channelId = builder.channelId;
    }

    public String getServiceName() { return serviceName; }

    public SelectionType getSelectionType() { return selectionType; }

    public Optional<Range<Double>> getCpuLoad() { return Optional.ofNullable(cpuLoad); }

    public Optional<ChannelId> getChannelId() { return Optional.ofNullable(channelId); }

    public Optional<InetAddress> getIpAddress() { return Optional.ofNullable(ipAddress); }

    public Optional<Pattern> getHostName() { return Optional.ofNullable(hostName); }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public boolean matches(Properties serviceInfos) {
        return isServiceNameMatching(serviceInfos.getProperty(FIELD_NAME_SERVICE_NAME)) &&
               isCpuLoadMatching(serviceInfos.getProperty(FIELD_NAME_CPU_LOAD)) &&
               isChannelTypeMatching(serviceInfos.getProperty(FIELD_NAME_CHANNEL_TYPE)) &&
               isIpAddressMatching(serviceInfos.getProperty(FIELD_NAME_IP_ADDRESS)) &&
               isHostNameMatching(serviceInfos.getProperty(FIELD_NAME_HOSTNAME));
    }

    private boolean isServiceNameMatching(@Nullable String serviceName) {
        return Objects.equals(getServiceName(), serviceName);
    }

    private boolean isCpuLoadMatching(@Nullable String cpuLoad) {
        return cpuLoad == null ||
               getCpuLoad().map(reqCpuLoad -> Try.of(() -> Double.parseDouble(cpuLoad))
                                                 .filter(reqCpuLoad::contains)
                                                 .isSuccess())
                           .orElse(true);
    }

    private boolean isChannelTypeMatching(@Nullable String channelType) {
        return channelType == null ||
               getChannelId().map(reqChannelType -> Try.of(() -> ChannelId.ChannelType.valueOf(channelType))
                                                       .filter(reqChannelType::equals)
                                                       .isSuccess())
                             .orElse(true);
    }

    private boolean isIpAddressMatching(@Nullable String ipAddress) {
        return ipAddress == null ||
               getIpAddress().map(reqIpAddress -> Try.of(() -> InetAddress.getByName(ipAddress))
                                                  .filter(reqIpAddress::equals)
                                                  .isSuccess())
                             .orElse(true);
    }

    private boolean isHostNameMatching(@Nullable String hostName) {
        return hostName == null ||
               getHostName().map(reqHostName -> reqHostName.matcher(hostName).matches())
                            .orElse(true);
    }

    public static class Builder extends InheritableBuilder.Builder<Builder, BasicServiceDiscoverySearchFilter> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String serviceName;
        private SelectionType selectionType = SelectionType.ROUTED;
        @Nullable
        private Range<Double> cpuLoad;
        @Nullable
        private ChannelId channelId;
        @Nullable
        private InetAddress ipAddress;
        @Nullable
        private Pattern hostName;

        protected Builder() { /* Nothing to do */ }

        /**
         *
         * @param serviceName Name of the service that should be found
         * @return this builder
         */
        public final Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return getThis();
        }

        /**
         *
         * @param ipAddress Ip address on which the service should run
         * @return this builder
         */
        public final Builder setIpAddress(@Nullable InetAddress ipAddress) {
            this.ipAddress = ipAddress;
            return getThis();
        }

        /**
         *
         * @param hostName regexp defining the hostname that the service should run onto
         * @return this builder
         */
        public final Builder setHostName(@Nullable Pattern hostName) {
            this.hostName = hostName;
            return getThis();
        }

        /**
         *
         * @param cpuLoad load range of the host on which the service is running on
         * @return this builder
         */
        public final Builder setCpuLoad(@Nullable Range<Double> cpuLoad) {
            this.cpuLoad = cpuLoad;
            return getThis();
        }

        /**
         *
         * @param selectionType Definition on the type of {@link io.reacted.core.services.ReActorService}
         *                      {@link io.reacted.core.reactorsystem.ReActorRef} required.
         *                      @see SelectionType
         * @return this builder
         */
        public final Builder setSelectionType(SelectionType selectionType) {
            this.selectionType = selectionType;
            return getThis();
        }

        /**
         *
         * @param channelId Definition of the requested {@link io.reacted.core.config.ChannelId} used to
         *                    communicate with the discovered {@link io.reacted.core.reactorsystem.ReActorRef}
         * @return this builder
         */
        public final Builder setChannelId(@Nullable ChannelId channelId) {
            this.channelId = channelId;
            return getThis();
        }

        @Override
        public BasicServiceDiscoverySearchFilter build() {
            return new BasicServiceDiscoverySearchFilter(this);
        }
    }
}