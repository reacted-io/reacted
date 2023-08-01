/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;

import javax.annotation.concurrent.Immutable;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Immutable
public class ReActorSystemId implements ReActedMessage {
    public static final String NO_REACTORSYSTEM_ID_NAME = "NO_REACTORSYSTEM_ID";
    public static final int NO_REACTORSYSTEM_ID_MARKER = 1;
    public static final int COMMON_REACTORSYSTEM_ID_MARKER = 0;

    @Serial
    private static final long serialVersionUID = 1;
    private static final long REACTORSYSTEM_UUID_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class,
                                                                                            "reActorSystemUUID")
                                                                            .orElseSneakyThrow();
    private static final long REACTORSYSTEM_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class,
                                                                                            "reActorSystemName")
                                                                            .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorSystemId NO_REACTORSYSTEM_ID = new ReActorSystemId(NO_REACTORSYSTEM_ID_NAME)
            .setReActorSystemUUID(ReActorId.NO_REACTOR_ID_UUID)
            .setHashCode(Objects.hash(ReActorId.NO_REACTOR_ID_UUID, NO_REACTORSYSTEM_ID_NAME));
    private final UUID reActorSystemUUID;
    private final String reActorSystemName;
    private final int hashCode;

    public ReActorSystemId() { /* Required by externalizable */
        this.reActorSystemUUID = NO_REACTORSYSTEM_ID.getReActorSystemUUID();
        this.reActorSystemName = NO_REACTORSYSTEM_ID.getReActorSystemName();
        this.hashCode = NO_REACTORSYSTEM_ID.getHashCode();
    }

    public ReActorSystemId(String reActorSystemName) {
        this.reActorSystemName = reActorSystemName;
        this.reActorSystemUUID = UUID.nameUUIDFromBytes(reActorSystemName.getBytes(StandardCharsets.UTF_8));
        this.hashCode = Objects.hash(this.reActorSystemUUID, reActorSystemName);
    }

    public String getReActorSystemName() { return reActorSystemName; }
    public int getHashCode() { return hashCode(); }

    public UUID getReActorSystemUUID() { return reActorSystemUUID; }

    @Override
    public void encode(Serializer serializer) {
        if (this == NO_REACTORSYSTEM_ID) {
            serializer.put(NO_REACTORSYSTEM_ID_MARKER);
        } else {
            serializer.put(COMMON_REACTORSYSTEM_ID_MARKER);
            serializer.put(reActorSystemUUID.getMostSignificantBits());
            serializer.put(reActorSystemUUID.getLeastSignificantBits());
            serializer.put(reActorSystemName);
        }
    }

    @Override
    public void decode(Deserializer deserializer) {
        if (deserializer.getInt() == COMMON_REACTORSYSTEM_ID_MARKER) {
            setReActorSystemUUID(new UUID(deserializer.getLong(), deserializer.getLong()))
                    .setReActorSystemName(deserializer.getString());
            setHashCode(Objects.hash(reActorSystemUUID, reActorSystemName));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReActorSystemId that = (ReActorSystemId) o;
        return Objects.equals(reActorSystemUUID, that.reActorSystemUUID) &&
               Objects.equals(reActorSystemName, that.reActorSystemName);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "ReActorSystemId{" +
               "reActorSystemUUID=" + reActorSystemUUID +
               ", reActorSystemName='" + reActorSystemName + '\'' +
               ", hashCode=" + hashCode +
               '}';
    }
    private ReActorSystemId setReActorSystemUUID(UUID uuid) {
        return SerializationUtils.setObjectField(this, REACTORSYSTEM_UUID_OFFSET, uuid);
    }

    private ReActorSystemId setReActorSystemName(String reActorSystemName) {
        return SerializationUtils.setObjectField(this, REACTORSYSTEM_NAME_OFFSET, reActorSystemName);
    }

    private ReActorSystemId setHashCode(int hashCode) {
        return SerializationUtils.setIntField(this, HASHCODE_OFFSET, hashCode);
    }
}
