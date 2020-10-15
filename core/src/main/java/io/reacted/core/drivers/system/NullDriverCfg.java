/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ReActedDriverCfg;

public class NullDriverCfg extends ReActedDriverCfg<NullDriverCfg.Builder, NullDriverCfg> {
    private NullDriverCfg(Builder builder) {
        super(builder);
    }

    static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ReActedDriverCfg.Builder<Builder, NullDriverCfg> {
        private Builder() { setChannelName(ChannelId.NO_CHANNEL_ID.getChannelName()); }
        @Override
        public NullDriverCfg build() {
            return new NullDriverCfg(this);
        }
    }
}
