/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serial;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class DeadMessage implements Externalizable, ReActedMessage {

    @Serial
    private static final long serialVersionUID = 1;
    private static final long PAYLOAD_OFFSET = SerializationUtils.getFieldOffset(DeadMessage.class, "payload")
                                                                 .orElseSneakyThrow();

    private final ReActedMessage payload;

    public DeadMessage() { this.payload = SerializationUtils.NO_PAYLOAD; /* required for Externalizable */ }
    public DeadMessage(ReActedMessage payload) { this.payload = payload; }

    @SuppressWarnings("unchecked")
    public <PayloadT extends ReActedMessage> PayloadT getPayload() { return (PayloadT)payload; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeadMessage that = (DeadMessage) o;
        return Objects.equals(getPayload(), that.getPayload());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPayload());
    }

    @Override
    public String toString() {
        return "DeadMessage{" + "payload=" + getPayload() + '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getPayload());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SerializationUtils.setObjectField(this, PAYLOAD_OFFSET, in.readObject());
    }

    @Override
    public void encode(Serializer serializer) {

    }

    @Override
    public void decode(Deserializer deserializer) {

    }
}
