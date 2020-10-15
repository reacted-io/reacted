/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverCfg;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class DirectCommunicationCfg extends ChannelDriverCfg<DirectCommunicationCfg.Builder,
                                                             DirectCommunicationCfg> {
    private DirectCommunicationCfg(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverCfg.Builder<Builder, DirectCommunicationCfg> {
        private Builder() { }

        @Override
        public DirectCommunicationCfg build() {
            return new DirectCommunicationCfg(this);
        }
    }
}
