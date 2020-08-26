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
public class ReActorConfig extends ReActiveEntityConfig<ReActorConfig, ReActorConfig.Builder> {

    public ReActorConfig(Builder reActorConfig) {
        super(reActorConfig);
    }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public Builder toBuilder() {
        return fillBuilder(newBuilder());
    }

    public static class Builder extends ReActiveEntityConfig.Builder<ReActorConfig, Builder> {
        private Builder() { setEntityType(ReActiveEntityType.REACTOR); }
        @Override
        public ReActorConfig build() {
            return new ReActorConfig(this);
        }
    }
}
