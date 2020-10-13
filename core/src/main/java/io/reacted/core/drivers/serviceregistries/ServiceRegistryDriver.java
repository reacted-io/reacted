/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.serviceregistries;

import io.reacted.core.config.reactors.ServiceRegistryCfg;
import io.reacted.core.reactors.GenericReActor;

import javax.annotation.Nonnull;

public abstract class ServiceRegistryDriver<BuilderT extends ServiceRegistryCfg.Builder<BuilderT, BuiltT>,
                                            BuiltT extends ServiceRegistryCfg<BuilderT, BuiltT>>
        implements GenericReActor<BuilderT, BuiltT> {
    @Nonnull
    private final BuiltT driverCfg;
    protected ServiceRegistryDriver(@Nonnull BuiltT driverCfg) {
        this.driverCfg = driverCfg;
    }

    @Nonnull
    @Override
    public final BuiltT getConfig() { return driverCfg;  }
}
