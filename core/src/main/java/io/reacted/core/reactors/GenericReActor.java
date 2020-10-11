/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import io.reacted.core.config.reactors.ReActiveEntityConfig;

import javax.annotation.Nonnull;

public interface GenericReActor<BuilderT extends ReActiveEntityConfig.Builder<BuilderT, BuiltT>,
                                BuiltT extends ReActiveEntityConfig<BuilderT, BuiltT>> extends ReActiveEntity {
    @Nonnull
    BuiltT getConfig();
}
