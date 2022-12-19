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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Objects;

@NonNullByDefault
public final class Message {
    private static final long SENDER_OFFSET = SerializationUtils.getFieldOffset(Message.class, "sender")
                                                                .orElseSneakyThrow();
    private static final long DESTINATION_OFFSET = SerializationUtils.getFieldOffset(Message.class, "destination")
                                                                     .orElseSneakyThrow();
    private static final long SEQ_NUM_OFFSET = SerializationUtils.getFieldOffset(Message.class, "sequenceNumber")
                                                                 .orElseSneakyThrow();
    private static final long CREATING_REACTOR_SYSTEM_ID = SerializationUtils.getFieldOffset(Message.class, "creatingReactorSystemId")
                                                                 .orElseSneakyThrow();
    private static final long ACKING_POLICY = SerializationUtils.getFieldOffset(Message.class, "ackingPolicy")
                                                                 .orElseSneakyThrow();
    private static final long PAYLOAD_OFFSET = SerializationUtils.getFieldOffset(Message.class, "payload")
                                                                 .orElseSneakyThrow();
    private final ReActorRef sender;
    private final ReActorRef destination;
    private final long sequenceNumber;
    private final ReActorSystemId creatingReactorSystemId;
    private final AckingPolicy ackingPolicy;
    private final Serializable payload;

    private Message() {
        this(ReActorRef.NO_REACTOR_REF, ReActorRef.NO_REACTOR_REF, 0L,
             ReActorSystemId.NO_REACTORSYSTEM_ID, AckingPolicy.NONE, SerializationUtils.NO_PAYLOAD);
    }

    private Message(ReActorRef sender, ReActorRef destination, long sequenceNumber,
                    ReActorSystemId creatingReActorSystem, AckingPolicy ackingPolicy, Serializable payload) {
        this.sender = sender;
        this.destination = destination;
        this.sequenceNumber = sequenceNumber;
        this.creatingReactorSystemId = creatingReActorSystem;
        this.ackingPolicy = ackingPolicy;
        this.payload = payload;
    }
    public static Message of(ReActorRef sender, ReActorRef destination, long sequenceNumber,
                             ReActorSystemId generatingReActorSystem, AckingPolicy ackingPolicy, Serializable payload) {
        return new Message(sender, destination, sequenceNumber, generatingReActorSystem, ackingPolicy,
                           payload);
    }

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
        return "Message{" + "sender=" + sender + ", destination=" + destination + ", sequenceNumber=" + sequenceNumber + ", creatingReactorSystemId=" + creatingReactorSystemId + ", ackingPolicy=" + ackingPolicy + ", payload=" + payload + '}';
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        Objects.requireNonNull(sender).writeExternal(out);
        Objects.requireNonNull(destination).writeExternal(out);
        Objects.requireNonNull(creatingReactorSystemId).writeExternal(out);
        out.writeInt(ackingPolicy.ordinal());
        out.writeLong(sequenceNumber);
        out.writeObject(payload);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorRef senderRef = new ReActorRef();
        senderRef.readExternal(in);
        setSender(senderRef);
        var destinationRef = new ReActorRef();
        destinationRef.readExternal(in);
        setDestination(destinationRef);
        var receivedGeneratingReActorSystem = new ReActorSystemId();
        receivedGeneratingReActorSystem.readExternal(in);
        setCreatingReactorSystemId(receivedGeneratingReActorSystem);
        setAckingPolicy(AckingPolicy.forOrdinal(in.readInt()));
        setSequenceNumber(in.readLong());
        try {
            setPayload((Serializable)in.readObject());
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
    public ReActorRef getSender() { return sender; }

    public ReActorRef getDestination() { return destination; }

    public long getSequenceNumber() { return sequenceNumber; }

    public ReActorSystemId getCreatingReactorSystemId() { return creatingReactorSystemId; }

    public AckingPolicy getAckingPolicy() { return ackingPolicy; }

    public Serializable getPayload() { return payload; }

    @SuppressWarnings("UnusedReturnValue")
    private Message setSender(ReActorRef sender) {
        return SerializationUtils.setObjectField(this, SENDER_OFFSET, sender);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setDestination(ReActorRef destination) {
        return SerializationUtils.setObjectField(this, DESTINATION_OFFSET, destination);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setAckingPolicy(AckingPolicy ackingPolicy) {
        return SerializationUtils.setObjectField(this, ACKING_POLICY, ackingPolicy);
    }

    @SuppressWarnings("UnusedReturnValue")
    private Message setCreatingReactorSystemId(ReActorSystemId creatingReactorSystemId) {
        return SerializationUtils.setObjectField(this, CREATING_REACTOR_SYSTEM_ID, creatingReactorSystemId);
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
