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

import java.util.Objects;

@NonNullByDefault
public class ReActorServiceConfig extends ReActiveEntityConfig<ReActorServiceConfig.Builder,
                                                               ReActorServiceConfig> {

    private final int routeesNum;
    private final UnChecked.CheckedSupplier<? extends ReActor> routeeProvider;
    private final ReActorService.LoadBalancingPolicy loadBalancingPolicy;

    private ReActorServiceConfig(Builder reActorServiceConfig) {
        super(reActorServiceConfig);
        this.routeesNum = ObjectUtils.requiredInRange(reActorServiceConfig.routeesNum, 1, 10_000,
                                                      IllegalArgumentException::new);
        this.routeeProvider = Objects.requireNonNull(reActorServiceConfig.routeeProvider);
        this.loadBalancingPolicy = Objects.requireNonNull(reActorServiceConfig.loadBalancingPolicy);
    }

    public int getRouteesNum() {
        return routeesNum;
    }

    public ReActorService.LoadBalancingPolicy getSelectionPolicy() {
        return loadBalancingPolicy;
    }

    @Override
    public ReActiveEntityConfig.Builder<Builder, ReActorServiceConfig> toBuilder() {
        return fillBuilder(newBuilder());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public UnChecked.CheckedSupplier<? extends ReActor> getRouteeProvider() {
        return routeeProvider;
    }

    public static class Builder extends ReActiveEntityConfig.Builder<Builder, ReActorServiceConfig> {
        private int routeesNum;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private UnChecked.CheckedSupplier<? extends ReActor> routeeProvider;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActorService.LoadBalancingPolicy loadBalancingPolicy;

        private Builder() {
            setEntityType(ReActiveEntityType.REACTORSERVICE);
        }

        /**
         * A Service exposes the behavior of a reactor in a resilient and load balanced manneer. Here we specify
         * how many instances of the exposed reactor should be automatically created and mantained by the router.
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
         *
         * @param loadBalancingPolicy Policy to use for selecting the destination among routee when a message
         *                            when a message for a routee is received by the service
         */
        public Builder setSelectionPolicy(ReActorService.LoadBalancingPolicy loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
            return this;
        }

        @Override
        public ReActorServiceConfig build() {
            return new ReActorServiceConfig(this);
        }
    }
}
