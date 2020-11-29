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
import java.util.Objects;
import java.util.Properties;

@Immutable
@NonNullByDefault
public class RegistryGateUpserted implements Serializable {
    private final ReActorSystemId reActorSystemId;
    private final ChannelId channelId;
    private final Properties channelData;
    public RegistryGateUpserted(String reActorSystemName, ChannelId channelId, Properties channelData) {
        this.reActorSystemId = new ReActorSystemId(Objects.requireNonNull(reActorSystemName));
        this.channelId = Objects.requireNonNull(channelId);
        this.channelData = Objects.requireNonNull(channelData);
    }

    public ReActorSystemId getReActorSystemId() { return reActorSystemId; }

    public ChannelId getChannelId() { return channelId; }

    public Properties getChannelData() { return channelData; }

    @Override
    public String toString() {
        return "RegistryGateUpserted{" + "reActorSystemId=" + reActorSystemId + ", channelId=" + channelId + ", " +
               "channelData=" + channelData + '}';
    }
}
