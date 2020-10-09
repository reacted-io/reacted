/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.serviceregistries;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

@NonNullByDefault
public class ServiceRegistryInit {
    private final ReActorSystem reActorSystem;
    private final ReActorRef driverReActor;
    private final ScheduledExecutorService timerService;

    private ServiceRegistryInit(Builder builder) {
        this.reActorSystem = Objects.requireNonNull(builder.reActorSystem);
        this.driverReActor = Objects.requireNonNull(builder.driverActor);
        this.timerService = Objects.requireNonNull(builder.timerService);
    }

    public ReActorSystem getReActorSystem() { return this.reActorSystem; }

    public ReActorRef getDriverReActor() { return this.driverReActor; }

    public ScheduledExecutorService getTimerService() { return this.timerService; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActorSystem reActorSystem;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActorRef driverActor;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ScheduledExecutorService timerService;

        private Builder() { }

        public Builder setReActorSystem(ReActorSystem reActorSystem) {
            this.reActorSystem = reActorSystem;
            return this;
        }

        public Builder setDriverReActor(ReActorRef driverActor) {
            this.driverActor = driverActor;
            return this;
        }

        public Builder setTimerService(ScheduledExecutorService timerService) {
            this.timerService = timerService;
            return this;
        }

        public ServiceRegistryInit build() {
            return new ServiceRegistryInit(this);
        }
    }
}
