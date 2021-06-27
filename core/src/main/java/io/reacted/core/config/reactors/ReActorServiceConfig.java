/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.services.Service;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.concurrent.Immutable;

@NonNullByDefault
@Immutable
public abstract class ReActorServiceConfig<BuilderT extends ReActiveEntityConfig.Builder<BuilderT, BuiltT>,
                                           BuiltT extends ReActorServiceConfig<BuilderT, BuiltT>>
    extends ReActiveEntityConfig<BuilderT, BuiltT> {
    public static final int MIN_ROUTEES_PER_SERVICE = 1;
    public static final int MAX_ROUTEES_PER_SERVICE = 1000;
    public static final Duration DEFAULT_SERVICE_REPUBLISH_ATTEMPT_ON_ERROR_DELAY = Duration.ofMinutes(2);
    private final int routeesNum;
    private final UnChecked.CheckedFunction<BuiltT, ? extends ReActor> routeeProvider;
    private final Service.LoadBalancingPolicy loadBalancingPolicy;
    private final Duration serviceRepublishReattemptDelayOnError;
    private final boolean remoteService;

    protected ReActorServiceConfig(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.routeesNum = ObjectUtils.requiredInRange(builder.routeesNum, MIN_ROUTEES_PER_SERVICE,
                                                      MAX_ROUTEES_PER_SERVICE, IllegalArgumentException::new);
        this.routeeProvider = Objects.requireNonNull(builder.routeeProvider,
                                                     "Routee provider cannot be null");
        this.loadBalancingPolicy = Objects.requireNonNull(builder.loadBalancingPolicy,
                                                          "Load balancing policy cannot be null");
        this.serviceRepublishReattemptDelayOnError = ObjectUtils.checkNonNullPositiveTimeInterval(builder.serviceRepublishReattemptDelayOnError);
        this.remoteService = builder.remoteService;
    }

    public int getRouteesNum() {
        return routeesNum;
    }

    public Service.LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    public UnChecked.CheckedFunction<BuiltT, ? extends ReActor> getRouteeProvider() {
        return routeeProvider;
    }

    public Duration getServiceRepublishReattemptDelayOnError() {
        return serviceRepublishReattemptDelayOnError;
    }

    public boolean isRemoteService() { return remoteService; }

    public abstract static class Builder<BuilderT, BuiltT> extends ReActiveEntityConfig.Builder<BuilderT, BuiltT> {
        protected int routeesNum = MIN_ROUTEES_PER_SERVICE;
        @SuppressWarnings("NotNullFieldNotInitialized")
        protected UnChecked.CheckedFunction<BuiltT, ? extends ReActor> routeeProvider;
        protected Service.LoadBalancingPolicy loadBalancingPolicy = Service.LoadBalancingPolicy.ROUND_ROBIN;
        protected Duration serviceRepublishReattemptDelayOnError = DEFAULT_SERVICE_REPUBLISH_ATTEMPT_ON_ERROR_DELAY;
        protected boolean remoteService;

        protected Builder() { setIsRemoteService(false); }

        /**
         * A Service exposes the behavior of a reactor in a resilient and load balanced manneer. Here we specify
         * how many instances of the exposed reactor should be automatically created and maintained by the router.
         * Valid range [{@link ReActorServiceConfig#MIN_ROUTEES_PER_SERVICE}, {@link ReActorServiceConfig#MAX_ROUTEES_PER_SERVICE}]
         *
         * @param routeesNum number of instances of the exposed reactor that should be created/maintained
         * @return this builder
         */
        public final BuilderT setRouteesNum(int routeesNum) {
            this.routeesNum = routeesNum;
            return getThis();
        }

        /**
         * On request, the service may need to dynamically spawn a new routee. This provider is
         * their factory
         *
         * @param routeeProvider Used to spawn a new routee reactor on request
         * @return this builder
         */
        public final BuilderT setRouteeProvider(UnChecked.CheckedFunction<BuiltT, ReActor> routeeProvider) {
            this.routeeProvider = routeeProvider;
            return getThis();
        }

        /**
         * On request, the service may need to dynamically spawn a new routee. This provider is
         * their factory
         *
         * @param routeeProvider Used to spawn a new routee reactor on request
         * @return this builder
         */
        public final BuilderT setRouteeProvider(UnChecked.CheckedSupplier<ReActor> routeeProvider) {
            this.routeeProvider = serviceCfg -> routeeProvider.get();
            return getThis();
        }

        /**
         * A service automatically load balances messages to its routees. Here we define how that should be done
         * Default value: {@link Service.LoadBalancingPolicy#ROUND_ROBIN}
         *
         * @param loadBalancingPolicy Policy to use for selecting the destination among routee when a message
         *                            when a message for a routee is received by the service
         * @return this builder
         */
        public final BuilderT setLoadBalancingPolicy(Service.LoadBalancingPolicy loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
            return getThis();
        }

        /**
         * A service automatically try to publish itself to the connected service registries. If an error should occur,
         * the service would not be discoverable. This parameter defines in how long the service should reattempt to
         * publish itself on the service registries
         *
         * Default value: {@link ReActorServiceConfig#DEFAULT_SERVICE_REPUBLISH_ATTEMPT_ON_ERROR_DELAY}
         *
         * @param republicationReattemptDelayOnError delay after than the republication should be reattempted
         * @return this builder
         */
        public final BuilderT setServiceRepublishReattemptDelayOnError(Duration republicationReattemptDelayOnError) {
            this.serviceRepublishReattemptDelayOnError = republicationReattemptDelayOnError;
            return getThis();
        }

        /**
         * Specify if this service should be published or not.
         * If the service is marked as non remote, service discovery subscription is automatically
         * enabled for this service.
         * If the desired behaviour should be different, the subscriptions can be overridden
         * using {@link ReActiveEntityConfig&lt;&gt;#setTypedSubscriptions}
         *
         * @param remoteService true if this service is meant to be published on the registries
         * @return this builder
         */
        public final BuilderT setIsRemoteService(boolean remoteService) {
            this.remoteService = remoteService;
            if (!remoteService) {
                setTypedSubscriptions(Stream.concat(Arrays.stream(super.typedSubscriptions),
                                                    Stream.of(TypedSubscription.LOCAL.forType(ServiceDiscoveryRequest.class)))
                                            .distinct()
                                            .toArray(TypedSubscription[]::new));
            }
            return getThis();
        }
    }
}
