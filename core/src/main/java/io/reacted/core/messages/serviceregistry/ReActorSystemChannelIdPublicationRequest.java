/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.config.ChannelId;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Properties;

@Immutable
@NonNullByDefault
public class ReActorSystemChannelIdPublicationRequest implements Serializable {
    private final ReActorSystemId reActorSystemId;
    private final ChannelId channelId;
    private final Properties channelIdData;

    public ReActorSystemChannelIdPublicationRequest(ReActorSystemId reActorSystemId, ChannelId channelId,
                                                    Properties channelIdData) {
        this.reActorSystemId = reActorSystemId;
        this.channelId = channelId;
        this.channelIdData = channelIdData;
    }

    public ReActorSystemId getReActorSystemId() { return reActorSystemId; }
    public ChannelId getChannelId() { return channelId; }
    public Properties getChannelIdData() { return channelIdData; }
}
