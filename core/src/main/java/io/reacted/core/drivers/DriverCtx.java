/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers;

import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class DriverCtx {
    private final ReActorSystem localReActorSystem;
    private final ReActorSystemDriver<? extends ReActedDriverCfg<?, ?>> decodingDriver;

    public DriverCtx(ReActorSystem reActorSystem, ReActorSystemDriver<? extends ReActedDriverCfg<?, ?>> driver) {
        this.localReActorSystem = reActorSystem;
        this.decodingDriver = driver;
    }

    public ReActorSystem getLocalReActorSystem() { return localReActorSystem; }

    public ReActorSystemDriver<? extends ReActedDriverCfg<?, ?>> getDecodingDriver() { return decodingDriver; }
}
