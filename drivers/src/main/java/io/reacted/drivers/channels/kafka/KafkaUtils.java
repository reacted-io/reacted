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
    Message toAvroMessage(ReActorRef source, ReActorRef destination, long sequenceNumber,
                          ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                          PayloadT payload) throws IOException {
        Message.Builder avroMessageBuilder = Message.newBuilder()
                .setSequenceNumber(sequenceNumber)
                .setAckingPolicy(toAvroAckingPolicy(ackingPolicy))
                .setCreatorReactorSystem(toAvroReActorSystemId(reActorSystemId));
        if (!source.equals(ReActorRef.NO_REACTOR_REF)) {
            avroMessageBuilder.setSource(toAvroReActorRef(source));
        }
        if (!destination.equals(ReActorRef.NO_REACTOR_REF)) {
            avroMessageBuilder.setDestination(toAvroReActorRef(destination));
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

    static ReActorRef fromAvroReActorRef(@Nullable io.reacted.drivers.channels.kafka.avro.ReActorRef reActorRef,
                                         DriverCtx driverCtx) {
        if (reActorRef == null) {
            return ReActorRef.NO_REACTOR_REF;
        }
        return new ReActorRef(fromAvroReActorId(reActorRef.getReactorId()),
                              fromAvroReActorSystemRef(reActorRef.getReactorSystemRef(), driverCtx));
    }

    static AckingPolicy fromAvroAckingPolicy(io.reacted.drivers.channels.kafka.avro.AckingPolicy ackingPolicy) {
        return AckingPolicy.forOrdinal(ackingPolicy.getAckingPolicyOrdinal());
    }

    static ReActorSystemId fromAvroReActorSystemId(io.reacted.drivers.channels.kafka.avro.ReActorSystemId reactorSystemId) {
        return new ReActorSystemId(reactorSystemId.getReactorSystemName().toString());
    }

    private static io.reacted.drivers.channels.kafka.avro.AckingPolicy toAvroAckingPolicy(AckingPolicy ackingPolicy) {
        return io.reacted.drivers.channels.kafka.avro.AckingPolicy.newBuilder()
                                                                  .setAckingPolicyOrdinal(ackingPolicy.ordinal())
                                                                  .build();
    }
    private static io.reacted.drivers.channels.kafka.avro.ReActorRef toAvroReActorRef(ReActorRef reActorRef) {
        return io.reacted.drivers.channels.kafka.avro.ReActorRef.newBuilder()
                                                                .setReactorId(toAvroReActorId(reActorRef.getReActorId()))
                                                                .setReactorSystemRef(toAvroReActorSystemRef(reActorRef.getReActorSystemRef()))
                                                                .build();
    }
    private static ReActorSystemRef fromAvroReActorSystemRef(io.reacted.drivers.channels.kafka.avro.ReActorSystemRef reActorSystemRef,
                                                             DriverCtx driverCtx) {
        var newRef = new ReActorSystemRef();
        var reActorSystemId = fromAvroReActorSystemId(reActorSystemRef.getReactorSystemId());
        var channelId = fromAvroChannelId(reActorSystemRef.getChannelId());
        ReActorSystemRef.setGateForReActorSystem(newRef, reActorSystemId, channelId, driverCtx);
        return newRef;
    }
    private static io.reacted.drivers.channels.kafka.avro.ReActorSystemRef toAvroReActorSystemRef(ReActorSystemRef reActorSystemRef) {
        return io.reacted.drivers.channels.kafka.avro.ReActorSystemRef.newBuilder()
                                                                      .setChannelId(toAvroChannelId(reActorSystemRef.getChannelId()))
                                                                      .setReactorSystemId(toAvroReActorSystemId(reActorSystemRef.getReActorSystemId()))
                                                                      .build();
    }
    private static io.reacted.core.reactors.ReActorId fromAvroReActorId(ReActorId reActorId) {
        java.util.UUID uuid = fromAvroUUID(reActorId.getId());
        String reActorName = reActorId.getName().toString();
        return new io.reacted.core.reactors.ReActorId()
                .setReActorName(reActorName)
                .setReActorUUID(uuid)
                .setHashCode(Objects.hash(uuid, reActorName));
    }

    private static ReActorId toAvroReActorId(io.reacted.core.reactors.ReActorId reActorId) {
        return ReActorId.newBuilder()
                        .setId(toAvroUUID(reActorId.getReActorUUID()))
                        .setName(reActorId.getReActorName())
                        .build();
    }

    private static io.reacted.drivers.channels.kafka.avro.ReActorSystemId toAvroReActorSystemId(ReActorSystemId reActorSystemId) {
        return io.reacted.drivers.channels.kafka.avro.ReActorSystemId.newBuilder()
                                                                     .setReactorSystemName(reActorSystemId.getReActorSystemName())
                                                                     .setReactorSystemId(toAvroUUID(reActorSystemId.getReActorSystemUUID()))
                                                                     .build();
    }

    private static io.reacted.core.config.ChannelId fromAvroChannelId(ChannelId channelId) {
        return fromAvroChannelType(channelId.getChannelType()).forChannelName(channelId.getChannelName().toString());
    }
    private static ChannelId toAvroChannelId(io.reacted.core.config.ChannelId channelId) {
        return ChannelId.newBuilder()
                        .setChannelName(channelId.getChannelName())
                        .setChannelType(toAvroChannelType(channelId.getChannelType()))
                        .build();
    }

    private static io.reacted.core.config.ChannelId.ChannelType fromAvroChannelType(ChannelType channelType) {
        return io.reacted.core.config.ChannelId.ChannelType.forOrdinal(channelType.getChannelTypeOrdinal());
    }

    private static ChannelType toAvroChannelType(io.reacted.core.config.ChannelId.ChannelType channelType) {
        return ChannelType.newBuilder()
                          .setChannelTypeOrdinal(channelType.ordinal())
                          .build();
    }

    private static java.util.UUID fromAvroUUID(UUID uuid) {
        return new java.util.UUID(uuid.getMostSignificant(),
                                  uuid.getLeastSignificant());
    }
    private static UUID toAvroUUID(java.util.UUID uuid) {
        return UUID.newBuilder()
                   .setMostSignificant(uuid.getMostSignificantBits())
                   .setLeastSignificant(uuid.getLeastSignificantBits())
                   .build();
    }
}
