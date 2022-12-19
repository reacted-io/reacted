/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.drivers.channels.kafka.avro.ChannelId;
import io.reacted.drivers.channels.kafka.avro.ChannelType;
import io.reacted.drivers.channels.kafka.avro.Message;
import io.reacted.drivers.channels.kafka.avro.ReActorId;
import io.reacted.drivers.channels.kafka.avro.ReactorSystemId;
import io.reacted.drivers.channels.kafka.avro.ReactorSystemRef;
import io.reacted.drivers.channels.kafka.avro.UUID;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

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
        try(ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(byteArray)) {
            oos.writeObject(payload);
            avroMessageBuilder.setPayload(ByteBuffer.wrap(byteArray.toByteArray()));
        }
        return avroMessageBuilder.build();
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

    private static ReactorSystemRef fromReActorSystemRef(ReActorSystemRef reActorSystemRef) {
        return ReactorSystemRef.newBuilder()
                               .setChannelId(fromChannelId(reActorSystemRef.getChannelId()))
                               .setReactorSystemId(fromReActorSystemId(reActorSystemRef.getReActorSystemId()))
                               .build();
    }
    private static ReActorId fromReActorId(io.reacted.core.reactors.ReActorId reActorId) {
        return ReActorId.newBuilder()
                        .setId(fromUUID(reActorId.getReActorUUID()))
                        .setName(reActorId.getReActorName())
                        .build();
    }
    private static ReactorSystemId fromReActorSystemId(ReActorSystemId reActorSystemId) {
        return ReactorSystemId.newBuilder()
                              .setReactorSystemId(fromUUID(reActorSystemId.getReActorSystemUUID()))
                              .setReactorSystemName(reActorSystemId.getReActorSystemName())
                              .build();
    }

    private static ChannelId fromChannelId(io.reacted.core.config.ChannelId channelId) {
        return ChannelId.newBuilder()
                        .setChannelName(channelId.getChannelName())
                        .setChannelType(fromChannelType(channelId.getChannelType()))
                        .build();
    }

    private static ChannelType fromChannelType(io.reacted.core.config.ChannelId.ChannelType channelType) {
        return ChannelType.newBuilder()
                          .setChannelTypeOrdinal(channelType.ordinal())
                          .build();
    }
    private static UUID fromUUID(java.util.UUID uuid) {
        return UUID.newBuilder()
                   .setMostSignificant(uuid.getMostSignificantBits())
                   .setLeastSignificant(uuid.getLeastSignificantBits())
                   .build();
    }
}
