/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

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
public class ServiceDiscoverySearchFilter extends InheritableBuilder<ServiceDiscoverySearchFilter.Builder,
                                                                     ServiceDiscoverySearchFilter>
        implements Serializable {
    public static final String FIELD_NAME_SERVICE_NAME = "serviceName";
    public static final String FIELD_NAME_SYSTEM_LOAD = "systemLoad";
    public static final String FIELD_NAME_CHANNEL_TYPE = "channelType";
    public static final String FIELD_NAME_IP_ADDRESS = "ipAddress";
    public static final String FIELD_NAME_HOSTNAME = "hostName";

    private final String serviceName;
    private final SelectionType selectionType;
    @Nullable
    private final Range<Double> systemLoad;
    @Nullable
    private final ChannelId.ChannelType channelType;
    @Nullable
    private final InetAddress ipAddress;
    @Nullable
    private final Pattern hostName;

    private ServiceDiscoverySearchFilter(Builder builder) {
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

    public Optional<Range<Double>> getSystemLoad() { return Optional.ofNullable(systemLoad); }

    public Optional<ChannelId.ChannelType> getChannelType() { return Optional.ofNullable(channelType); }

    public Optional<InetAddress> getIpAddress() { return Optional.ofNullable(ipAddress); }

    public Optional<Pattern> getHostName() { return Optional.ofNullable(hostName); }

    public static Builder newBuilder() { return new Builder(); }

    public boolean matches(Properties serviceInfos) {
        return isServiceNameMatching(serviceInfos.getProperty(FIELD_NAME_SERVICE_NAME)) &&
               isLoadMatching(serviceInfos.getProperty(FIELD_NAME_SYSTEM_LOAD)) &&
               isChannelTypeMatching(serviceInfos.getProperty(FIELD_NAME_CHANNEL_TYPE)) &&
               isIpAddressMatching(serviceInfos.getProperty(FIELD_NAME_IP_ADDRESS)) &&
               isHostNameMatching(serviceInfos.getProperty(FIELD_NAME_HOSTNAME));
    }

    private boolean isServiceNameMatching(@Nullable String reqServiceName) {
        return Objects.equals(getServiceName(), reqServiceName);
    }

    private boolean isLoadMatching(@Nullable String reqLoad) {
        return reqLoad == null ||
               getSystemLoad().map(systemLoad -> Try.of(() -> Double.parseDouble(reqLoad))
                                                    .filter(systemLoad::contains)
                                                    .isSuccess())
                              .orElse(true);
    }

    private boolean isChannelTypeMatching(@Nullable String reqChannelType) {
        return reqChannelType == null ||
               getChannelType().map(channelType -> Try.of(() -> ChannelId.ChannelType.valueOf(reqChannelType))
                                                      .filter(channelType::equals)
                                                      .isSuccess())
                               .orElse(true);
    }

    private boolean isIpAddressMatching(@Nullable String reqIpAddress) {
        return reqIpAddress == null ||
               getIpAddress().map(ipAddress -> Try.of(() -> InetAddress.getByName(reqIpAddress))
                                                  .filter(ipAddress::equals)
                                                  .isSuccess())
                             .orElse(true);
    }

    private boolean isHostNameMatching(@Nullable String reqHostName) {
        return reqHostName == null ||
               getHostName().map(hostName -> hostName.matcher(reqHostName).matches())
                            .orElse(true);
    }

    public static class Builder extends InheritableBuilder.Builder<Builder, ServiceDiscoverySearchFilter> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String serviceName;
        private SelectionType selectionType = SelectionType.ROUTED;
        @Nullable
        private Range<Double> systemLoad;
        @Nullable
        private ChannelId.ChannelType channelType;
        @Nullable
        private InetAddress ipAddress;
        @Nullable
        private Pattern hostName;

        private Builder() { /* Nothing to do */ }

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
         * @param hostName
         * @return this builder
         */
        public final Builder setHostName(@Nullable Pattern hostName) {
            this.hostName = hostName;
            return getThis();
        }

        /**
         *
         * @param systemLoad load range of the host on which the service is running on
         * @return this builder
         */
        public final Builder setSystemLoad(@Nullable Range<Double> systemLoad) {
            this.systemLoad = systemLoad;
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
         * @param channelType Definition of the requested {@link io.reacted.core.config.ChannelId.ChannelType} used to
         *                    communicate with the discovered {@link io.reacted.core.reactorsystem.ReActorRef}
         * @return this builder
         */
        public final Builder setChannelType(@Nullable ChannelId.ChannelType channelType) {
            this.channelType = channelType;
            return getThis();
        }

        @Override
        public ServiceDiscoverySearchFilter build() {
            return new ServiceDiscoverySearchFilter(this);
        }
    }
}
