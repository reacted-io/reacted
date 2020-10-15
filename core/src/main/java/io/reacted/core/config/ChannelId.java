/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config;

import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

@Immutable
@NonNullByDefault
public class ChannelId implements Serializable {
    public static final ChannelId NO_CHANNEL_ID = new ChannelId(ChannelType.INVALID_CHANNEL_TYPE,"");
    private static final String SEPARATOR = "@";
    private final String channelName;
    private final ChannelType channelType;
    private final String channelString;
    private final int hashCode;

    public ChannelId(ChannelType channelType, String channelName) {
        this.channelType = channelType;
        this.channelName = channelName;
        this.channelString = channelType.name() + SEPARATOR + channelName;
        this.hashCode = Objects.hash(channelType, channelName);
    }

    public static Optional<ChannelId> fromToString(String inputString) {
        return Try.of(() -> inputString.split(SEPARATOR))
                  .map(splitted -> new ChannelId(ChannelType.valueOf(splitted[0]), splitted[1]))
                  .toOptional();
    }

    public String getChannelName() { return channelName; }

    public ChannelType getChannelType() { return channelType; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelId channelId1 = (ChannelId) o;
        return channelType == channelId1.channelType &&
               Objects.equals(channelName, channelId1.channelName);
    }

    @Override
    public int hashCode() { return hashCode; }

    @Override
    public String toString() { return channelString; }

    public enum ChannelType {
        INVALID_CHANNEL_TYPE,
        DIRECT_COMMUNICATION,
        REPLAY_CHRONICLE_QUEUE,
        LOCAL_CHRONICLE_QUEUE,
        REMOTING_CHRONICLE_QUEUE,
        KAFKA,
        GRPC
    }
}
