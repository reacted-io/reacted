/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class DirectCommunicationConfig extends ChannelDriverConfig<DirectCommunicationConfig.Builder,
                                                                   DirectCommunicationConfig> {
    private DirectCommunicationConfig(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverConfig.Builder<Builder, DirectCommunicationConfig> {
        private Builder() { }

        @Override
        public DirectCommunicationConfig build() {
            return new DirectCommunicationConfig(this);
        }
    }
}
