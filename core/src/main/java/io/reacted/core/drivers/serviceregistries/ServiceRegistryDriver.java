/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.serviceregistries;

import io.reacted.core.reactors.ReActions;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.util.Properties;

public interface ServiceRegistryDriver {
    ReActions getReActions();
    Properties getConfiguration();
    void init(@Nonnull ServiceRegistryInit initInfo) throws Exception;
    @Nonnull
    Try<Void> stop() throws Exception;
}
