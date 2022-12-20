/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.drivers.channels.kafka.avro.ChannelId;
import io.reacted.drivers.channels.kafka.avro.ChannelType;
import io.reacted.drivers.channels.kafka.avro.Message;
import io.reacted.drivers.channels.kafka.avro.ReActorId;
import io.reacted.drivers.channels.kafka.avro.UUID;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

final class KafkaUtils {
    private KafkaUtils() { throw new AssertionError("We are not supposed to be called"); }

    @Nullable
    static <PayloadT extends Serializable>
    Message toKafkaMessage(ReActorRef source, ReActorRef destination, long sequenceNumber,
                           ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                           PayloadT payload) throws IOException {
        Message.Builder avroMessageBuilder = Message.newBuilder()
                .setSequenceNumber(sequenceNumber)
                .setAckingPolicy(fromAckingPolicy(ackingPolicy))
                .setCreatorReactorSystem(fromReActorSystemId(reActorSystemId));
        if (!source.equals(ReActorRef.NO_REACTOR_REF)) {
            avroMessageBuilder.setSource(fromReActorRef(source));
        }
        if (!destination.equals(ReActorRef.NO_REACTOR_REF)) {
            avroMessageBuilder.setDestination(fromReActorRef(destination));
        }
        avroMessageBuilder.setPayload(toSerializedPayload(payload));
        return avroMessageBuilder.build();
    }

    static ByteBuffer toSerializedPayload(Serializable payload) throws IOException {
        try(ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(byteArray)) {
            oos.writeObject(payload);
            return ByteBuffer.wrap(byteArray.toByteArray());
        }
    }

    static Serializable fromSerializedPayload(ByteBuffer serializedPayload) throws IOException, ClassNotFoundException {
        try (var bais = new ByteArrayInputStream(serializedPayload.array());
             var inputStream = new ObjectInputStream(bais)) {
            return (Serializable) inputStream.readObject();
        }
    }

    static ReActorRef toReActorRef(@Nullable io.reacted.drivers.channels.kafka.avro.ReActorRef reActorRef,
                                   DriverCtx driverCtx) {
        if (reActorRef == null) {
            return ReActorRef.NO_REACTOR_REF;
        }
        return new ReActorRef(toReActorId(reActorRef.getReactorId()),
                              toReActorSystemRef(reActorRef.getReactorSystemRef(), driverCtx));
    }

    static AckingPolicy toAckingPolicy(io.reacted.drivers.channels.kafka.avro.AckingPolicy ackingPolicy) {
        return AckingPolicy.forOrdinal(ackingPolicy.getAckingPolicyOrdinal());
    }
    private static io.reacted.drivers.channels.kafka.avro.AckingPolicy fromAckingPolicy(AckingPolicy ackingPolicy) {
        return io.reacted.drivers.channels.kafka.avro.AckingPolicy.newBuilder()
                                                                  .setAckingPolicyOrdinal(ackingPolicy.ordinal())
                                                                  .build();
    }

    private static io.reacted.drivers.channels.kafka.avro.ReActorRef fromReActorRef(ReActorRef reActorRef) {
        return io.reacted.drivers.channels.kafka.avro.ReActorRef.newBuilder()
                                                                .setReactorId(fromReActorId(reActorRef.getReActorId()))
                                                                .setReactorSystemRef(fromReActorSystemRef(reActorRef.getReActorSystemRef()))
                                                                .build();
    }

    private static ReActorSystemRef toReActorSystemRef(io.reacted.drivers.channels.kafka.avro.ReActorSystemRef reActorSystemRef,
                                                       DriverCtx driverCtx) {
        var newRef = new ReActorSystemRef();
        var reActorSystemId = toReActorSystemId(reActorSystemRef.getReactorSystemId());
        var channelId = toChannelId(reActorSystemRef.getChannelId());
        ReActorSystemRef.setGateForReActorSystem(newRef, reActorSystemId, channelId, driverCtx);
        return newRef;
    }
    private static io.reacted.drivers.channels.kafka.avro.ReActorSystemRef fromReActorSystemRef(ReActorSystemRef reActorSystemRef) {
        return io.reacted.drivers.channels.kafka.avro.ReActorSystemRef.newBuilder()
                                                                      .setChannelId(fromChannelId(reActorSystemRef.getChannelId()))
                                                                      .setReactorSystemId(fromReActorSystemId(reActorSystemRef.getReActorSystemId()))
                                                                      .build();
    }

    private static io.reacted.core.reactors.ReActorId toReActorId(ReActorId reActorId) {
        java.util.UUID uuid = toUUID(reActorId.getId());
        String reActorName = reActorId.getName().toString();
        return new io.reacted.core.reactors.ReActorId()
                .setReActorName(reActorName)
                .setReActorUUID(uuid)
                .setHashCode(Objects.hash(uuid, reActorName));
    }
    private static ReActorId fromReActorId(io.reacted.core.reactors.ReActorId reActorId) {
        return ReActorId.newBuilder()
                        .setId(fromUUID(reActorId.getReActorUUID()))
                        .setName(reActorId.getReActorName())
                        .build();
    }

    static ReActorSystemId toReActorSystemId(io.reacted.drivers.channels.kafka.avro.ReActorSystemId reactorSystemId) {
        return new ReActorSystemId(reactorSystemId.getReactorSystemName().toString());
    }
    private static io.reacted.drivers.channels.kafka.avro.ReActorSystemId fromReActorSystemId(ReActorSystemId reActorSystemId) {
        return io.reacted.drivers.channels.kafka.avro.ReActorSystemId.newBuilder()
                                                                     .setReactorSystemName(reActorSystemId.getReActorSystemName())
                                                                     .build();
    }

    private static io.reacted.core.config.ChannelId toChannelId(ChannelId channelId) {
        return toChannelType(channelId.getChannelType()).forChannelName(channelId.getChannelName().toString());
    }
    private static ChannelId fromChannelId(io.reacted.core.config.ChannelId channelId) {
        return ChannelId.newBuilder()
                        .setChannelName(channelId.getChannelName())
                        .setChannelType(fromChannelType(channelId.getChannelType()))
                        .build();
    }

    private static io.reacted.core.config.ChannelId.ChannelType toChannelType(ChannelType channelType) {
        return io.reacted.core.config.ChannelId.ChannelType.forOrdinal(channelType.getChannelTypeOrdinal());
    }

    private static ChannelType fromChannelType(io.reacted.core.config.ChannelId.ChannelType channelType) {
        return ChannelType.newBuilder()
                          .setChannelTypeOrdinal(channelType.ordinal())
                          .build();
    }

    private static java.util.UUID toUUID(UUID uuid) {
        return new java.util.UUID(uuid.getMostSignificant(),
                                  uuid.getLeastSignificant());
    }
    private static UUID fromUUID(java.util.UUID uuid) {
        return UUID.newBuilder()
                   .setMostSignificant(uuid.getMostSignificantBits())
                   .setLeastSignificant(uuid.getLeastSignificantBits())
                   .build();
    }
}
