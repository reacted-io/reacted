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

@Immutable
@NonNullByDefault
public class ReActorSystemChannelIdCancellationRequest implements Serializable {
    private final ReActorSystemId reActorSystemId;
    private final ChannelId channelId;

    public ReActorSystemChannelIdCancellationRequest(ReActorSystemId reActorSystemId, ChannelId channelId) {
        this.reActorSystemId = reActorSystemId;
        this.channelId = channelId;
    }

    public ReActorSystemId getReActorSystemId() { return reActorSystemId; }

    public ChannelId getChannelId() { return channelId; }
}
