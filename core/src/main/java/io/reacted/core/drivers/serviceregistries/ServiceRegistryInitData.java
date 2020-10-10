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
public class ServiceRegistryInitData {
    private final ScheduledExecutorService timerService;

    public ServiceRegistryInitData(ScheduledExecutorService timerService) {
        this.timerService = Objects.requireNonNull(timerService);
    }

    public ScheduledExecutorService getTimerService() { return this.timerService; }
}
