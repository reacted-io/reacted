/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.kafka;

import io.reacted.core.config.ChannelId;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.drivers.channels.grpc.ReActedLinkProtocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;


final class MessageSchema {

    static final String REACTOR_ID = "RI";
    static final String REACTOR_NAME = "RN";
    static final String SENDER = "S";
    static final String DESTINATION = "D";
    static final String SEQUENCE_NUMBER = "s";
    static final String CREATOR_REACTOR_SYSTEM_ID = "CRSI";
    static final String ACKING_POLICY = "A";

    static final String PAYLOAD = "P";
    static final String REACTOR_SYSTEM_NAME = "RSN";
    static final String REACTOR_SYSTEM_ID = "RSI";

    static final String REACTOR_SYSTEM_REF = "RSR";
    static final String CHANNEL_ID = "CI";
    static final String CHANNEL_TYPE = "CT";
    static final String CHANNEL_NAME = "CN";
    static final String UUID = "U";
    static final String UUID_MOST_SIGNIFICANT = "UM";
    static final String UUID_LEAST_SIGNIFICANT = "UL";
    static final String MARKER = "MK";
    private MessageSchema() { throw new AssertionError("This is not supposed to be called"); }

    static final Schema UUID_SCHEMA = SchemaBuilder.record(java.util.UUID.class.getSimpleName())
                                                   .fields()
                                                   .name(UUID_MOST_SIGNIFICANT)
                                                   .type()
                                                   .longType()
                                                   .longDefault(0)
                                                   .name(UUID_LEAST_SIGNIFICANT)
                                                   .type()
                                                   .longType()
                                                   .longDefault(0)
                                                   .endRecord();

    static final Schema CHANNEL_TYPE_SCHEMA = SchemaBuilder.enumeration(ChannelId.ChannelType.class.getSimpleName())
                                                           .symbols(Arrays.stream(ChannelId.ChannelType.values())
                                                                          .map(ChannelId.ChannelType::toString)
                                                                          .toArray(String[]::new));

    static final Schema CHANNEL_ID_SCHEMA = SchemaBuilder.record(ChannelId.class.getSimpleName())
                                                         .fields()
                                                         .requiredString(CHANNEL_NAME)
                                                         .name(CHANNEL_TYPE)
                                                         .type(CHANNEL_TYPE_SCHEMA)
                                                         .noDefault()
                                                         .endRecord();


    static final Schema REACTOR_SYSTEM_ID_SCHEMA = SchemaBuilder.record(ReActorSystemId.class.getSimpleName())
                                                                .fields()
                                                                .name(MARKER)
                                                                .type()
                                                                .intType()
                                                                .intDefault(ReActorSystemId.NO_REACTORSYSTEM_ID_MARKER)
                                                                .optionalString(REACTOR_SYSTEM_NAME)
                                                                .name(UUID)
                                                                .type()
                                                                .optional()
                                                                .type(UUID_SCHEMA)
                                                                .endRecord();
    static final Schema REACTOR_SYSTEM_REF_SCHEMA = SchemaBuilder.record(ReActedLinkProtocol.ReActorSystemRef.class.getSimpleName())
                                                                 .fields()
                                                                 .name(REACTOR_SYSTEM_ID)
                                                                 .type(REACTOR_SYSTEM_ID_SCHEMA)
                                                                 .noDefault()
                                                                 .name(CHANNEL_ID)
                                                                 .type(CHANNEL_ID_SCHEMA)
                                                                 .noDefault()
                                                                 .endRecord();
    static final Schema REACTOR_ID_SCHEMA = SchemaBuilder.record(ReActorId.class.getSimpleName())
                                                         .fields()
                                                         .name(MARKER)
                                                         .type()
                                                         .intType()
                                                         .intDefault(ReActorId.NO_REACTOR_ID_MARKER)
                                                         .optionalString(REACTOR_NAME)
                                                         .name(UUID)
                                                         .type()
                                                         .optional()
                                                         .type(UUID_SCHEMA)
                                                         .endRecord();

    static final Schema REACTOR_REF_SCHEMA = SchemaBuilder.record(ReActorRef.class.getSimpleName())
                                                          .fields()
                                                          .name(MARKER)
                                                          .type()
                                                          .intType()
                                                          .intDefault(ReActorRef.NO_REACTOR_REF_MARKER)
                                                          .name(REACTOR_ID)
                                                          .type()
                                                          .optional()
                                                          .type(REACTOR_ID_SCHEMA)
                                                          .name(REACTOR_SYSTEM_REF)
                                                          .type()
                                                          .optional()
                                                          .type(REACTOR_SYSTEM_REF_SCHEMA)
                                                          .endRecord();
    static final Schema ACKING_POLICY_SCHEMA = SchemaBuilder.enumeration(AckingPolicy.class.getSimpleName())
                                                            .defaultSymbol(AckingPolicy.NONE.toString())
                                                            .symbols(Arrays.stream(AckingPolicy.values())
                                                                           .map(AckingPolicy::toString)
                                                                           .toArray(String[]::new));

    public static final Schema MESSAGE_SCHEMA = SchemaBuilder.record(Message.class.getSimpleName())
                                                      .fields()
                                                      .name(SENDER)
                                                      .type(REACTOR_REF_SCHEMA)
                                                      .noDefault()
                                                      .name(DESTINATION)
                                                      .type(REACTOR_REF_SCHEMA)
                                                      .noDefault()
                                                      .requiredLong(SEQUENCE_NUMBER)
                                                      .name(CREATOR_REACTOR_SYSTEM_ID)
                                                      .type(REACTOR_SYSTEM_ID_SCHEMA)
                                                      .noDefault()
                                                      .name(ACKING_POLICY)
                                                      .type(ACKING_POLICY_SCHEMA)
                                                      .noDefault()
                                                      .name(PAYLOAD)
                                                      .type()
                                                      .bytesType()
                                                      .noDefault()
                                                      .endRecord();

    public static void main(String[] args) {
        System.out.println(MESSAGE_SCHEMA);
    }
}
