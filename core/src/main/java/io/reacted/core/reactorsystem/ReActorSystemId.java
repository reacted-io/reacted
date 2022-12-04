/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.messages.SerializationUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ReActorSystemId implements Externalizable  {
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
            .setReActorSystemUUID(new UUID(0, 0))
            .setHashCode(Objects.hash(new UUID(0, 0), NO_REACTORSYSTEM_ID_NAME));
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
    public void writeExternal(ObjectOutput out) throws IOException {
        if (this == NO_REACTORSYSTEM_ID) {
            out.writeInt(NO_REACTORSYSTEM_ID_MARKER);
        } else {
            out.writeInt(COMMON_REACTORSYSTEM_ID_MARKER);
            out.writeObject(reActorSystemUUID);
            out.writeObject(reActorSystemName);
        }
    }
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readInt() == COMMON_REACTORSYSTEM_ID_MARKER) {
            setReActorSystemUUID((UUID) in.readObject())
                    .setReActorSystemName((String) (in.readObject()));
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
