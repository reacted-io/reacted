/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages;

import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

@NonNullByDefault
public class DataLink implements Externalizable {
    public static final DataLink NO_DATALINK = new DataLink();
    private static final long GENERATING_REACTORSYSTEM_OFFSET = SerializationUtils.getFieldOffset(DataLink.class,
                                                                                        "generatingReActorSystem")
                                                                                  .orElseSneakyThrow();
    private static final long ACKING_POLICY_OFFSET = SerializationUtils.getFieldOffset(DataLink.class, "ackingPolicy")
                                                                       .orElseSneakyThrow();

    private final ReActorSystemId generatingReActorSystem;
    private final AckingPolicy ackingPolicy;

    public DataLink() {
        /* Required by externalizable */
        this.generatingReActorSystem = ReActorSystemId.NO_REACTORSYSTEM_ID;
        this.ackingPolicy = AckingPolicy.NONE;
    }

    DataLink(ReActorSystemId generatingReActorSystem, AckingPolicy ackingPolicy) {
        this.generatingReActorSystem = generatingReActorSystem;
        this.ackingPolicy = ackingPolicy;
    }

    public ReActorSystemId getGeneratingReActorSystem() { return generatingReActorSystem; }

    public AckingPolicy getAckingPolicy() { return ackingPolicy; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Objects.requireNonNull(generatingReActorSystem).writeExternal(out);
        out.writeInt(Objects.requireNonNull(ackingPolicy).ordinal());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorSystemId generatingReActorSystem = new ReActorSystemId();
        generatingReActorSystem.readExternal(in);
        setGeneratingReActorSystem(generatingReActorSystem);
        setAckingPolicy(AckingPolicy.values()[in.readInt()]);
    }

    @SuppressWarnings("UnusedReturnValue")
    public DataLink setGeneratingReActorSystem(ReActorSystemId generatingReActorSystem) {
        return SerializationUtils.setObjectField(this, GENERATING_REACTORSYSTEM_OFFSET, generatingReActorSystem);
    }

    @SuppressWarnings("UnusedReturnValue")
    public DataLink setAckingPolicy(AckingPolicy ackingPolicy) {
        return SerializationUtils.setObjectField(this, ACKING_POLICY_OFFSET, ackingPolicy);
    }
}
