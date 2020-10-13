/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.config.reactors.ReActiveEntityConfig;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.services.ReActorService;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked;

import java.time.Duration;
import java.util.Objects;

@NonNullByDefault
public class ReActorServiceConfig extends ReActiveEntityConfig<ReActorServiceConfig.Builder,
                                                               ReActorServiceConfig> {
    public static final int MAX_ROUTEES_PER_SERVICE = 1000;
    public static final Duration DEFAULT_SERVICE_REPUBLISH_ATTEMPT_ON_ERROR_DELAY = Duration.ofMinutes(2);
    private final int routeesNum;
    private final UnChecked.CheckedSupplier<? extends ReActor> routeeProvider;
    private final ReActorService.LoadBalancingPolicy loadBalancingPolicy;
    private final Duration serviceRepublishReattemptDelayOnError;

    private ReActorServiceConfig(Builder builder) {
        super(builder);
        this.routeesNum = ObjectUtils.requiredInRange(builder.routeesNum, 1, MAX_ROUTEES_PER_SERVICE,
                                                      IllegalArgumentException::new);
        this.routeeProvider = Objects.requireNonNull(builder.routeeProvider);
        this.loadBalancingPolicy = Objects.requireNonNull(builder.loadBalancingPolicy);
        this.serviceRepublishReattemptDelayOnError = ObjectUtils.checkNonNullPositiveInterval(builder.serviceRepublishReattemptDelayOnError);
    }

    public int getRouteesNum() {
        return routeesNum;
    }

    public ReActorService.LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public UnChecked.CheckedSupplier<? extends ReActor> getRouteeProvider() {
        return routeeProvider;
    }

    public Duration getServiceRepublishReattemptDelayOnError() {
        return serviceRepublishReattemptDelayOnError;
    }

    public static class Builder extends ReActiveEntityConfig.Builder<Builder, ReActorServiceConfig> {
        private int routeesNum;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private UnChecked.CheckedSupplier<? extends ReActor> routeeProvider;
        private ReActorService.LoadBalancingPolicy loadBalancingPolicy = ReActorService.LoadBalancingPolicy.ROUND_ROBIN;
        private Duration serviceRepublishReattemptDelayOnError = DEFAULT_SERVICE_REPUBLISH_ATTEMPT_ON_ERROR_DELAY;

        private Builder() { }

        /**
         * A Service exposes the behavior of a reactor in a resilient and load balanced manneer. Here we specify
         * how many instances of the exposed reactor should be automatically created and mantained by the router.
         * Valid range [1, {@link ReActorServiceConfig#MAX_ROUTEES_PER_SERVICE}]
         *
         * @param routeesNum number of instances of the exposed reactor that should be created/mantained
         */
        public Builder setRouteesNum(int routeesNum) {
            this.routeesNum = routeesNum;
            return this;
        }

        /**
         * On request, the service may need to dynamically spawn a new routee. This provider is
         * their factory
         *
         * @param routeeProvider Used to spawn a new routee reactor on request
         */
        public Builder setRouteeProvider(UnChecked.CheckedSupplier<ReActor> routeeProvider) {
            this.routeeProvider = routeeProvider;
            return this;
        }

        /**
         * A service automatically load balances messages to its routees. Here we define how that should be done
         * Default value: {@link ReActorService.LoadBalancingPolicy#ROUND_ROBIN}
         *
         * @param loadBalancingPolicy Policy to use for selecting the destination among routee when a message
         *                            when a message for a routee is received by the service
         */
        public Builder setLoadBalancingPolicy(ReActorService.LoadBalancingPolicy loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
            return this;
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
        public Builder setServiceRepublishReattemptDelayOnError(Duration republicationReattemptDelayOnError) {
            this.serviceRepublishReattemptDelayOnError = republicationReattemptDelayOnError;
            return this;
        }

        @Override
        public ReActorServiceConfig build() {
            return new ReActorServiceConfig(this);
        }
    }
}
