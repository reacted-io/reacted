/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import io.reacted.core.messages.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class ReActorId implements Externalizable {
    @Serial
    private static final long serialVersionUID = 1;
    private static final int UUID_BYTES_SIZE = 16;
    private static final long REACTOR_UUID_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorUUID")
                                                                      .orElseSneakyThrow();
    private static final long REACTOR_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorName")
                                                                      .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorId NO_REACTOR_ID = new ReActorId().setReActorName("Init")
                                                                 .setReActorUUID(new UUID(0, 0))
                                                                 .setHashCode(Objects.hash(new UUID(0, 0), "Init"));

    private final UUID reActorUUID;
    private final String reActorName;
    private final int hashCode;

    public ReActorId(ReActorId fatherReActorId, String reActorName) {
        this(fatherReActorId.getReActorUUID(), reActorName);
    }

    public ReActorId() {
        this.reActorUUID = new UUID(0,0);
        this.reActorName = "This " + ReActorId.class.getSimpleName() + " should not be leaked"; /* no parent */
        this.hashCode = Objects.hash(reActorUUID, reActorName);
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
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getReActorUUID());
        out.writeObject(reActorName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setReActorUUID((UUID)in.readObject());
        setReActorName((String)in.readObject());
        setHashCode(Objects.hash(getReActorUUID(), getReActorName()));
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
