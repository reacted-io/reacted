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
public class ReActorConfig extends ReActiveEntityConfig<ReActorConfig.Builder, ReActorConfig> {

    protected ReActorConfig(Builder reActorConfig) {
        super(reActorConfig);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static Builder fromConfig(ReActorConfig reActorConfig) {
        return newBuilder().setMailBoxProvider(reActorConfig.getMailBoxProvider())
                           .setDispatcherName(reActorConfig.getDispatcherName())
                           .setTypedSubscriptions(reActorConfig.getTypedSubscriptions())
                           .setReActorName(reActorConfig.getReActorName());
    }

    public static class Builder extends ReActiveEntityConfig.Builder<Builder, ReActorConfig> {
        protected Builder() { }
        @Override
        public ReActorConfig build() {
            return new ReActorConfig(this);
        }
    }
}
