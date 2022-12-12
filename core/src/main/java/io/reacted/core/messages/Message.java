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
    private ReActorRef sender;
    private ReActorRef destination;
    private long sequenceNumber;
    private ReActorSystemId generatingReActorSystem;
    private AckingPolicy ackingPolicy;
    private Serializable payload;

    private Message() {
        /* Required by Externalizable */
        this.sender = ReActorRef.NO_REACTOR_REF;
        this.destination = ReActorRef.NO_REACTOR_REF;
        this.sequenceNumber = 0;
        this.payload = SerializationUtils.NO_PAYLOAD;
        this.generatingReActorSystem = ReActorSystemId.NO_REACTORSYSTEM_ID;
        this.ackingPolicy = AckingPolicy.NONE;
    }

    public static Message forParams(ReActorRef sender, ReActorRef dest, long seqNum, ReActorSystemId generatingReActorSystem,
                                    AckingPolicy ackingPolicy, Serializable payload) {
        Message message = new Message();
        return message.setSender(sender)
                      .setDestination(dest)
                      .setSequenceNumber(seqNum)
                      .setGeneratingReActorSystem(generatingReActorSystem)
                      .setAckingPolicy(ackingPolicy)
                      .setPayload(payload);
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

    public ReActorRef getSender() { return sender; }

    public ReActorRef getDestination() { return destination; }

    public long getSequenceNumber() { return sequenceNumber; }

    public ReActorSystemId getGeneratingReActorSystem() { return generatingReActorSystem; }

    public AckingPolicy getAckingPolicy() { return ackingPolicy; }

    public <PayloadT extends Serializable> PayloadT getPayload() { return (PayloadT)payload; }
    public Message setSender(ReActorRef sender) {
        this.sender = sender;
        return this;
    }

    public Message setDestination(ReActorRef destination) {
        this.destination = destination;
        return this;
    }

    public Message setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        return this;
    }

    public Message setGeneratingReActorSystem(ReActorSystemId generatingReActorSystem) {
        this.generatingReActorSystem = generatingReActorSystem;
        return this;
    }

    public Message setAckingPolicy(AckingPolicy ackingPolicy) {
        this.ackingPolicy = ackingPolicy;
        return this;
    }
    public Message setPayload(Serializable payload) {
        this.payload = payload;
        return this;
    }
}
