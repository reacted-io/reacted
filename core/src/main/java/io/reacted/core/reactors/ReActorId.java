/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors;

import io.reacted.core.messages.SerializationUtils;

import java.io.Serial;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.UUID;

@Immutable
public final class ReActorId implements Externalizable {
    @Serial
    private static final long serialVersionUID = 1;
    private static final long REACTOR_RAW_ID_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorRawId")
                                                                        .orElseSneakyThrow();
    private static final long REACTOR_NAME_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "reActorName")
                                                                      .orElseSneakyThrow();
    private static final long HASHCODE_OFFSET = SerializationUtils.getFieldOffset(ReActorId.class, "hashCode")
                                                                  .orElseSneakyThrow();
    public static final ReActorId NO_REACTOR_ID = new ReActorId().setReActorName("Init")
                                                                 .setReActorRawId(0L)
                                                                 .setHashCode(Objects.hash("Init", new UUID(0, 0)));

    private final long reActorRawId;
    private final String reActorName;
    private final int hashCode;

    public ReActorId(ReActorId fatherReActorId, String reActorName) {
        this(fatherReActorId.getReActorRawId(), reActorName);
    }

    public ReActorId() {
        this.reActorRawId = 0;
        this.reActorName = "This " + ReActorId.class.getSimpleName() + " should not be leaked"; /* no parent */
        this.hashCode = Objects.hash(reActorRawId, reActorName);
    }

    private ReActorId(long seedUUID, String reActorName) {
        this.reActorRawId = seedUUID + reActorName.hashCode();
        this.reActorName = reActorName;
        this.hashCode = Objects.hash(reActorRawId, reActorName);
    }

    @Override
    public String toString() {
        return "ReActorId{" + "reActorUUID=" + reActorRawId + ", reActorName='" + reActorName + '\'' + ", hashCode=" + hashCode + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReActorId reActorId1 = (ReActorId) o;
        return Objects.equals(getReActorRawId(), reActorId1.getReActorRawId());
    }

    @Override
    public int hashCode() { return hashCode; }

    public long getReActorRawId() {
        return reActorRawId;
    }

    public String getReActorName() {
        return reActorName;
    }

    public int getHashCode() { return hashCode(); }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(getReActorRawId());
        out.writeObject(reActorName);
        out.writeInt(hashCode);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setReActorRawId(in.readLong());
        setReActorName((String)in.readObject());
        setHashCode(in.readInt());
    }

    public ReActorId setReActorRawId(long rawId) {
        return SerializationUtils.setLongField(this, REACTOR_RAW_ID_OFFSET, rawId);
    }

    public ReActorId setReActorName(String reActorName) {
        return SerializationUtils.setObjectField(this, REACTOR_NAME_OFFSET, reActorName);
    }

    public ReActorId setHashCode(int hashCode) {
        return SerializationUtils.setIntField(this, HASHCODE_OFFSET, hashCode);
    }
}
