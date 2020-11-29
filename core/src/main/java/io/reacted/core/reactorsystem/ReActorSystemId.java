/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.messages.SerializationUtils;

import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

@Immutable
public class ReActorSystemId implements Externalizable  {
    public static final String NO_REACTORSYSTEM_ID_NAME = "NO_REACTORSYSTEM_ID";
    private static final long serialVersionUID = 1;
    private static final long REACTORSYSTEM_UUID_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class,
                                                                                            "reActorSystemUUID")
                                                                            .orElseSneakyThrow();
    private static final long REACTORSYSTEM_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class,
                                                                                            "reActorSystemName")
                                                                            .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorSystemId NO_REACTORSYSTEM_ID = new ReActorSystemId()
            .setReActorSystemUUID(new UUID(0, 0))
            .setReActorSystemName(NO_REACTORSYSTEM_ID_NAME)
            .setHashCode(Objects.hash(new UUID(0, 0), NO_REACTORSYSTEM_ID_NAME));
    private final UUID reActorSystemUUID;
    private final String reActorSystemName;
    private final int hashCode;

    public ReActorSystemId() { /* Required by externalizable */
        this.reActorSystemUUID = null;
        this.reActorSystemName = null;
        this.hashCode = Integer.MIN_VALUE;
    }

    public ReActorSystemId(String reActorSystemName) {
        this.reActorSystemName = reActorSystemName;
        this.reActorSystemUUID = UUID.nameUUIDFromBytes(reActorSystemName.getBytes(StandardCharsets.UTF_8));
        this.hashCode = Objects.hash(this.reActorSystemUUID, reActorSystemName);
    }

    public String getReActorSystemName() { return reActorSystemName; }

    public UUID getReActorSystemUUID() { return reActorSystemUUID; }

    public int getHashCode() { return hashCode(); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(reActorSystemUUID.getMostSignificantBits());
        out.writeLong(reActorSystemUUID.getLeastSignificantBits());
        out.writeObject(reActorSystemName);
        out.writeInt(hashCode);
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
               "reActorSysUUID=" + reActorSystemUUID +
               ", reActorSystemName='" + reActorSystemName + '\'' +
               ", hashCode=" + hashCode +
               '}';
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setReActorSystemUUID(new UUID(in.readLong(), in.readLong()))
                .setReActorSystemName((String)(in.readObject()))
                .setHashCode(in.readInt());
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
