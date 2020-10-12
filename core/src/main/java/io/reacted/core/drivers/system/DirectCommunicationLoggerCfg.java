/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ReActedDriverCfg;

public class DirectCommunicationLoggerCfg extends ReActedDriverCfg<DirectCommunicationLoggerCfg.Builder,
                                                                   DirectCommunicationLoggerCfg> {

    private DirectCommunicationLoggerCfg(Builder builder) {
        super(builder);
    }

    public static DirectCommunicationLoggerCfg.Builder newBuilder() { return new Builder(); }

    public static class Builder extends ReActedDriverCfg.Builder<DirectCommunicationLoggerCfg.Builder,
            DirectCommunicationLoggerCfg> {
        private Builder() { }

        @Override
        public DirectCommunicationLoggerCfg build() {
            return new DirectCommunicationLoggerCfg(this);
        }
    }
}