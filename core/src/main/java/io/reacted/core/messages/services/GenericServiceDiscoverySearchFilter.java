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
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.services.SelectionType;
import io.reacted.core.services.Service;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

@NonNullByDefault
public abstract class GenericServiceDiscoverySearchFilter<BuilderT extends GenericServiceDiscoverySearchFilter.Builder<BuilderT, BuiltT>,
                                                          BuiltT extends GenericServiceDiscoverySearchFilter<BuilderT, BuiltT> & ServiceDiscoverySearchFilter>
        extends InheritableBuilder<BuilderT, BuiltT> implements ServiceDiscoverySearchFilter {

    private final String serviceName;
    private final SelectionType selectionType;
    @Nullable
    private final Range<Double> cpuLoad;
    private final Set<ChannelId> channelIdSet;
    @Nullable
    private final InetAddress ipAddress;
    @Nullable
    private final Pattern hostNameExpr;

    protected GenericServiceDiscoverySearchFilter(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.serviceName = Objects.requireNonNull(builder.serviceName);
        this.selectionType = Objects.requireNonNull(builder.selectionType);
        this.cpuLoad = builder.cpuLoad;
        this.ipAddress = builder.ipAddress;
        this.hostNameExpr = builder.hostNameExpr;
        this.channelIdSet = Objects.requireNonNull(builder.channelIdSet);
    }

    public String getServiceName() { return serviceName; }

    public SelectionType getSelectionType() { return selectionType; }

    public Optional<Range<Double>> getCpuLoad() { return Optional.ofNullable(cpuLoad); }

    public Set<ChannelId> getChannelIdSet() { return channelIdSet; }

    public Optional<InetAddress> getIpAddress() { return Optional.ofNullable(ipAddress); }

    public Optional<Pattern> getHostNameExpr() { return Optional.ofNullable(hostNameExpr); }

    @Override
    public boolean matches(Properties serviceInfo, ReActorRef serviceGate) {
        return isServiceNameMatching(Objects.requireNonNull(serviceInfo).getProperty(FIELD_NAME_SERVICE_NAME)) &&
               isCpuLoadMatching(serviceInfo.getProperty(FIELD_NAME_CPU_LOAD)) &&
               isChannelIdMatching(Objects.requireNonNull(serviceGate)) &&
               isIpAddressMatching(serviceInfo.getProperty(FIELD_NAME_IP_ADDRESS)) &&
               isHostNameMatching(serviceInfo.getProperty(FIELD_NAME_HOSTNAME));
    }

    private boolean isServiceNameMatching(@Nullable String serviceName) {
        return Objects.equals(getServiceName(), serviceName);
    }

    private boolean isCpuLoadMatching(@Nullable String serviceCpuLoad) {
        return serviceCpuLoad == null ||
               getCpuLoad().map(reqCpuLoad -> Try.of(() -> Double.valueOf(serviceCpuLoad))
                                                 .filter(reqCpuLoad::contains)
                                                 .isSuccess())
                           .orElse(true);
    }

    private boolean isChannelIdMatching(ReActorRef serviceGate) {
        return getChannelIdSet().isEmpty() ||
               getChannelIdSet().contains(serviceGate.getReActorSystemRef()
                                                     .getBackingDriver()
                                                     .getChannelId());
    }

    private boolean isIpAddressMatching(@Nullable String serviceIpAddress) {
        return serviceIpAddress == null ||
               getIpAddress().map(reqIpAddress -> Try.of(() -> InetAddress.getByName(serviceIpAddress))
                                                     .filter(reqIpAddress::equals)
                                                     .isSuccess())
                             .orElse(true);
    }

    private boolean isHostNameMatching(@Nullable String serviceHostName) {
        return serviceHostName == null ||
               getHostNameExpr().map(reqHostName -> reqHostName.matcher(serviceHostName).matches())
                                .orElse(true);
    }

    @Override
    public String toString() {
        return "BasicServiceDiscoverySearchFilter{" + "serviceName='" + serviceName + '\'' + ", selectionType=" +
               selectionType + ", cpuLoad=" + cpuLoad + ", channelId=" + channelIdSet + ", ipAddress=" + ipAddress +
               ", hostName=" + hostNameExpr + '}';
    }

    public static abstract class Builder<BuilderT, BuiltT> extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String serviceName;
        private SelectionType selectionType = SelectionType.ROUTED;
        @Nullable
        private Range<Double> cpuLoad;
        private Set<ChannelId> channelIdSet;
        @Nullable
        private InetAddress ipAddress;
        @Nullable
        private Pattern hostNameExpr;

        protected Builder() { this.channelIdSet = Set.of(); }

        /**
         *
         * @param serviceName Name of the service that should be found
         * @return this builder
         */
        public final BuilderT setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return getThis();
        }

        /**
         *
         * @param ipAddress Ip address on which the service should run
         * @return this builder
         */
        public final BuilderT setIpAddress(@Nullable InetAddress ipAddress) {
            this.ipAddress = ipAddress;
            return getThis();
        }

        /**
         *
         * @param hostNameExpr regexp defining the hostname that the service should run onto
         * @return this builder
         */
        public final BuilderT setHostNameExpression(@Nullable Pattern hostNameExpr) {
            this.hostNameExpr = hostNameExpr;
            return getThis();
        }

        /**
         *
         * @param cpuLoad load range of the host on which the service is running on
         * @return this builder
         */
        public final BuilderT setCpuLoad(@Nullable Range<Double> cpuLoad) {
            this.cpuLoad = cpuLoad;
            return getThis();
        }

        /**
         *
         * @param selectionType Definition on the type of {@link Service}
         *                      {@link ReActorRef} required.
         *                      @see SelectionType
         * @return this builder
         */
        public final BuilderT setSelectionType(SelectionType selectionType) {
            this.selectionType = selectionType;
            return getThis();
        }

        /**
         *
         * @param channelIdSet Definition of the requested {@link ChannelId} used to
         *                     communicate with the discovered {@link ReActorRef}
         * @return this builder
         */
        public final BuilderT setChannelId(Set<ChannelId> channelIdSet) {
            this.channelIdSet = channelIdSet;
            return getThis();
        }

        public final BuilderT setChannelId(ChannelId channelId) {
            this.channelIdSet = Set.of(channelId);
            return getThis();
        }
    }
}
