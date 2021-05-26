/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.ChannelId;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class NullDriverConfig extends ChannelDriverConfig<NullDriverConfig.Builder, NullDriverConfig> {
    private NullDriverConfig(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverConfig.Builder<Builder, NullDriverConfig> {
        private Builder() { setChannelName(ChannelId.NO_CHANNEL_ID.getChannelName());
                            setAckCacheSize(0); }
        @Override
        public NullDriverConfig build() {
            return new NullDriverConfig(this);
        }
    }
}
