/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Immutable
public final class ReActorId implements ReActedMessage {
    public static final UUID NO_REACTOR_ID_UUID = new UUID(0, 0);
    public static final int NO_REACTOR_ID_MARKER = 1;
    public static final int COMMON_REACTOR_ID_MARKER = 0;
    @Serial
    private static final long serialVersionUID = 1;
    private static final int UUID_BYTES_SIZE = 16;
    private static final long REACTOR_UUID_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorUUID")
                                                                      .orElseSneakyThrow();
    private static final long REACTOR_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorName")
                                                                      .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorId NO_REACTOR_ID = new ReActorId(NO_REACTOR_ID_UUID, "Init")
            .setReActorUUID(NO_REACTOR_ID_UUID)
            .setHashCode(Objects.hash(NO_REACTOR_ID_UUID, "Init"));
    private final UUID reActorUUID;
    private final String reActorName;
    private final int hashCode;

    public ReActorId(ReActorId fatherReActorId, String reActorName) {
        this(fatherReActorId.getReActorUUID(), reActorName);
    }

    public ReActorId() {
        this.reActorUUID = NO_REACTOR_ID.getReActorUUID();
        this.reActorName = NO_REACTOR_ID.getReActorName();
        this.hashCode = NO_REACTOR_ID.hashCode();
    }

    private ReActorId(UUID seedUUID, String reActorName) {
        this.reActorUUID = UUID.nameUUIDFromBytes((seedUUID.toString() + reActorName).getBytes(StandardCharsets.UTF_8));
        this.reActorName = reActorName;
        this.hashCode = Objects.hash(reActorUUID, reActorName);
    }

    @Override
    public String toString() {
        return "ReActorId{" + "reActorUUID=" + reActorUUID + ", reActorName='" + reActorName + '\'' + ", hashCode=" + hashCode + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReActorId reActorId1 = (ReActorId) o;
        return Objects.equals(getReActorUUID(), reActorId1.getReActorUUID()) && Objects.equals(getReActorName(),
                                                                                               reActorId1.getReActorName());
    }

    @SuppressWarnings("SameReturnValue")
    public int getRawIdSize() { return UUID_BYTES_SIZE; }

    @Override
    public int hashCode() { return hashCode; }

    public UUID getReActorUUID() {
        return reActorUUID;
    }

    public String getReActorName() {
        return reActorName;
    }

    @Override
    public void encode(Serializer serializer) {
        if (this == NO_REACTOR_ID) {
            serializer.put(NO_REACTOR_ID_MARKER);
        } else {
            serializer.put(COMMON_REACTOR_ID_MARKER);
            serializer.put(getReActorUUID().getMostSignificantBits());
            serializer.put(getReActorUUID().getLeastSignificantBits());
            serializer.put(reActorName);
        }
    }

    @Override
    public void decode(Deserializer deserializer) {
        if (deserializer.getInt() != NO_REACTOR_ID_MARKER) {
            setReActorUUID(new UUID(deserializer.getLong(), deserializer.getLong()));
            setReActorName(deserializer.getString());
            setHashCode(Objects.hash(getReActorUUID(), getReActorName()));
        }
    }

    public ReActorId setReActorUUID(UUID uuid) {
        return SerializationUtils.setObjectField(this, REACTOR_UUID_OFFSET, uuid);
    }

    public ReActorId setReActorName(String reActorName) {
        return SerializationUtils.setObjectField(this, REACTOR_NAME_OFFSET, reActorName);
    }

    public ReActorId setHashCode(int hashCode) {
        return SerializationUtils.setIntField(this, HASHCODE_OFFSET, hashCode);
    }
}
