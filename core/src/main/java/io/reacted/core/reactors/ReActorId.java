/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import io.reacted.core.messages.SerializationUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Immutable
public final class ReActorId implements Externalizable {
    private static final long serialVersionUID = 1;
    private static final long REACTOR_UUID_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorUUID")
                                                                      .orElseSneakyThrow();
    private static final long REACTOR_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorName")
                                                                      .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorId NO_REACTOR_ID = new ReActorId().setReActorName("Init")
                                                                 .setReActorUUID(new UUID(0, 0))
                                                                 .setHashCode(Objects.hash("Init", new UUID(0, 0)));

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
        return "ReActorId{" + "reActorUUID=" + reActorUUID + ", actorName='" + reActorName + '\'' + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReActorId reActorId1 = (ReActorId) o;
        return Objects.equals(getReActorUUID(), reActorId1.getReActorUUID()) && Objects.equals(getReActorName(),
                                                                                               reActorId1.getReActorName());
    }

    @Override
    public int hashCode() { return hashCode; }

    public UUID getReActorUUID() {
        return reActorUUID;
    }

    public String getReActorName() {
        return reActorName;
    }

    public int getHashCode() { return hashCode(); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(getReActorUUID().getMostSignificantBits());
        out.writeLong(getReActorUUID().getLeastSignificantBits());
        out.writeObject(reActorName);
        out.writeInt(hashCode);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setReActorUUID(new UUID(in.readLong(), in.readLong()));
        setReActorName((String)in.readObject());
        setHashCode(in.readInt());
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
