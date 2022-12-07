/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.patterns.NonNullByDefault;
@NonNullByDefault
public class CQLocalDriverConfig extends CQDriverConfig<CQLocalDriverConfig.Builder, CQLocalDriverConfig> {
    private CQLocalDriverConfig(Builder configBuilder) {
        super(configBuilder);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends CQDriverConfig.Builder<Builder, CQLocalDriverConfig> {
        private Builder() { }

        public CQLocalDriverConfig build() { return new CQLocalDriverConfig(this); }
    }
}
