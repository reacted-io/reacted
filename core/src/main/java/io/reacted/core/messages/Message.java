/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serial;
import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Objects;

@NonNullByDefault
public final class Message implements Externalizable {
    @Serial
    private static final long serialVersionUID = 1;
    private static final long SENDER_OFFSET = SerializationUtils.getFieldOffset(Message.class, "sender")
                                                                .orElseSneakyThrow();
    private static final long DESTINATION_OFFSET = SerializationUtils.getFieldOffset(Message.class, "destination")
                                                                     .orElseSneakyThrow();
    private static final long SEQ_NUM_OFFSET = SerializationUtils.getFieldOffset(Message.class, "sequenceNumber")
                                                                 .orElseSneakyThrow();
    private static final long PAYLOAD_OFFSET = SerializationUtils.getFieldOffset(Message.class, "payload")
                                                                 .orElseSneakyThrow();
    private static final long GENERATING_REACTORSYSTEM_OFFSET = SerializationUtils.getFieldOffset(Message.class,
                                                                                                  "generatingReActorSystem")
                                                                                  .orElseSneakyThrow();
    private static final long ACKING_POLICY_OFFSET = SerializationUtils.getFieldOffset(Message.class, "ackingPolicy")
                                                                       .orElseSneakyThrow();

    private final ReActorRef sender;
    private final ReActorRef destination;
    private final long sequenceNumber;
    private final ReActorSystemId generatingReActorSystem;
    private final AckingPolicy ackingPolicy;
    private final Serializable payload;

    public Message() {
        /* Required by Externalizable */
        this.sender = ReActorRef.NO_REACTOR_REF;
        this.destination = ReActorRef.NO_REACTOR_REF;
        this.sequenceNumber = 0;
        this.payload = SerializationUtils.NO_PAYLOAD;
        this.generatingReActorSystem = ReActorSystemId.NO_REACTORSYSTEM_ID;
        this.ackingPolicy = AckingPolicy.NONE;
    }

    public Message(ReActorRef sender, ReActorRef dest, long seqNum, ReActorSystemId generatingReActorSystem,
                   AckingPolicy ackingPolicy, Serializable payload) {
        this.sender = sender;
        this.destination = dest;
        this.sequenceNumber = seqNum;
        this.generatingReActorSystem = generatingReActorSystem;
        this.ackingPolicy = ackingPolicy;
        this.payload = payload;
    }

    public ReActorRef getSender() { return sender; }

    public ReActorRef getDestination() { return destination; }

    @SuppressWarnings("unchecked")
    public <PayloadT extends Serializable> PayloadT getPayload() { return (PayloadT)payload; }

    public long getSequenceNumber() { return sequenceNumber; }

    public ReActorSystemId getGeneratingReActorSystem() { return generatingReActorSystem; }

    public AckingPolicy getAckingPolicy() { return ackingPolicy; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(getSender(), message.getSender()) &&
               Objects.equals(getDestination(), message.getDestination()) &&
               Objects.equals(getPayload(), message.getPayload());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSender(), getDestination(), getPayload());
    }

    @Override
    public String toString() {
        return "Message{" +
               "sender=" + sender +
               ", destination=" + destination +
               ", sequenceNumber=" + sequenceNumber +
               ", payload=" + payload +
               '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Objects.requireNonNull(sender).writeExternal(out);
        Objects.requireNonNull(destination).writeExternal(out);
        Objects.requireNonNull(generatingReActorSystem).writeExternal(out);
        out.writeInt(ackingPolicy.ordinal());
        out.writeLong(sequenceNumber);
        out.writeObject(payload);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorRef senderRef = new ReActorRef();
        senderRef.readExternal(in);
        setSender(senderRef);
        var destinationRef = new ReActorRef();
        destinationRef.readExternal(in);
        setDestination(destinationRef);
        var receivedGeneratingReActorSystem = new ReActorSystemId();
        receivedGeneratingReActorSystem.readExternal(in);
        setGeneratingReActorSystem(receivedGeneratingReActorSystem);
        setAckingPolicy(AckingPolicy.forOrdinal(in.readInt()));
        setSequenceNumber(in.readLong());
        try {
            setPayload((Serializable)in.readObject());
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setSender(ReActorRef sender) {
        return SerializationUtils.setObjectField(this, SENDER_OFFSET, sender);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setDestination(ReActorRef destination) {
        return SerializationUtils.setObjectField(this, DESTINATION_OFFSET, destination);
    }

    @SuppressWarnings("UnusedReturnValue")
    private void setGeneratingReActorSystem(ReActorSystemId generatingReActorSystem) {
        SerializationUtils.setObjectField(this, GENERATING_REACTORSYSTEM_OFFSET, generatingReActorSystem);
    }

    private void setAckingPolicy(AckingPolicy ackingPolicy) {
        SerializationUtils.setObjectField(this, ACKING_POLICY_OFFSET, ackingPolicy);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setSequenceNumber(long sequenceNumber) {
        return SerializationUtils.setLongField(this, SEQ_NUM_OFFSET, sequenceNumber);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setPayload(Serializable payload) {
        return SerializationUtils.setObjectField(this, PAYLOAD_OFFSET, payload);
    }
}
