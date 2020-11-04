/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config;

public enum ChannelType {
    INVALID_CHANNEL_TYPE,
    NULL_CHANNEL_TYPE,
    DIRECT_COMMUNICATION,
    REPLAY_CHRONICLE_QUEUE,
    LOCAL_CHRONICLE_QUEUE,
    REMOTING_CHRONICLE_QUEUE,
    KAFKA,
    GRPC;
    public ChannelId withChannelName(String channelName) { return new ChannelId(this, channelName); }
}
