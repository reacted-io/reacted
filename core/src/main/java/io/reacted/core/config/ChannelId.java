/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Optional;

@Immutable
@NonNullByDefault
public class ChannelId implements Externalizable {
    public static final ChannelId NO_CHANNEL_ID = new ChannelId(ChannelType.NULL_CHANNEL_TYPE,"");
    public static final ChannelId INVALID_CHANNEL_ID = new ChannelId(ChannelType.INVALID_CHANNEL_TYPE,
                                                                     "INVALID CHANNEL NAME");
    private static final String SEPARATOR = "@";

    private static final long CHANNEL_NAME_OFFSET = SerializationUtils.getFieldOffset(ChannelId.class,
                                                                                      "channelName")
                                                                      .orElseSneakyThrow();
    private static final long CHANNEL_TYPE_OFFSET = SerializationUtils.getFieldOffset(ChannelId.class,
                                                                                      "channelType")
                                                                      .orElseSneakyThrow();
    private static final long CHANNEL_STRING_OFFSET = SerializationUtils.getFieldOffset(ChannelId.class,
                                                                                        "channelString")
                                                                        .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ChannelId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    private final String channelName;
    private final ChannelType channelType;
    private final String channelString;
    private final int hashCode;

    public ChannelId() {
        this.channelName = INVALID_CHANNEL_ID.channelName;
        this.channelType = INVALID_CHANNEL_ID.channelType;
        this.channelString = INVALID_CHANNEL_ID.channelString;
        this.hashCode = INVALID_CHANNEL_ID.hashCode;
    }

    public ChannelId(ChannelType channelType, String channelName) {
        this.channelType = channelType;
        this.channelName = channelName;
        this.channelString = channelType.name() + SEPARATOR + channelName;
        this.hashCode = Objects.hash(channelType, channelName);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(channelType);
        out.writeObject(channelName);
        out.writeInt(hashCode);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setChannelType((ChannelType)in.readObject());
        setChannelName((String)in.readObject());
        setHashCode(in.readInt());
        setChannelStringOffset(channelType.name() + SEPARATOR + channelName);
    }

    public static Optional<ChannelId> fromToString(String inputString) {
        return Try.of(() -> inputString.split(SEPARATOR))
                  .map(split -> new ChannelId(ChannelType.valueOf(split[0]), split[1]))
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

    private void setChannelName(String channelName) {
        SerializationUtils.setObjectField(this, CHANNEL_NAME_OFFSET, channelName);
    }

    private void setChannelType(ChannelType channelType) {
        SerializationUtils.setObjectField(this, CHANNEL_TYPE_OFFSET, channelType);
    }

    private void setChannelStringOffset(String channelString) {
        SerializationUtils.setObjectField(this, CHANNEL_STRING_OFFSET, channelString);
    }

    private void setHashCode(int hashCode) {
        SerializationUtils.setIntField(this, HASHCODE_OFFSET, hashCode);
    }

    public enum ChannelType {
        INVALID_CHANNEL_TYPE,
        NULL_CHANNEL_TYPE,
        DIRECT_COMMUNICATION,
        REPLAY_CHRONICLE_QUEUE,
        LOCAL_CHRONICLE_QUEUE,
        REMOTING_CHRONICLE_QUEUE,
        KAFKA,
        GRPC
    }
}
