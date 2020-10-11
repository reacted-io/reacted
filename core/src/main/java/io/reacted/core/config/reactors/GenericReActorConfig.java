/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public abstract class GenericReActorConfig<BuilderT extends GenericReActorConfig.Builder<BuilderT, BuiltT>,
                                           BuiltT extends GenericReActorConfig<BuilderT, BuiltT>>
        extends ReActiveEntityConfig<BuilderT, BuiltT> {

    protected GenericReActorConfig(Builder<BuilderT, BuiltT> reActorConfig) {
        super(reActorConfig);
    }

    public abstract static class Builder<BuilderT, BuiltT> extends ReActiveEntityConfig.Builder<BuilderT, BuiltT> {
        protected Builder() { setEntityType(ReActiveEntityType.REACTOR); }
    }
}
