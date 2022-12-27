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
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class RegistryGateRemoved implements ReActedMessage {
    private final ReActorSystemId removedReActorSystem;
    private final ChannelId channelId;
    public RegistryGateRemoved(String reActorSystemName, ChannelId channelId) {
        this.removedReActorSystem = new ReActorSystemId(Objects.requireNonNull(reActorSystemName));
        this.channelId = Objects.requireNonNull(channelId);
    }

    public ReActorSystemId getReActorSystem() { return removedReActorSystem; }

    public ChannelId getChannelId() { return channelId; }
}
