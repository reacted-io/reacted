/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.serialization;

import java.io.Serializable;

public interface ReActedMessage extends Serializable {
    default Class<? extends ReActedMessage> getMessageType() {
        return this.getClass();
    }
    default void encode(Serializer serializer) { }
    default void decode(Deserializer deserializer) { }
    static ReActedMessage of(String payload) {
        var msg = new StringMessage();
        msg.payload = payload;
        return msg;
    }

    static ReActedMessage of(int payload) {
        var msg = new IntMessage();
        msg.payload = payload;
        return msg;
    }

    static ReActedMessage of(long payload) {
        var msg = new LongMessage();
        msg.payload = payload;
        return msg;
    }

    static ReActedMessage of(byte[] payload) {
        var msg = new ByteArrayMessage();
        msg.payload = payload;
        return msg;
    }

    static ReActedMessage of(Throwable payload) {
        var msg = new ThrowableMessage();
        msg.value = payload;
        return msg;
    }

    static <T extends Enum<T>, R extends ReActedMessage> R of(T payload) {
        return (R) new EnumMessage<T>(payload);
    }

    class ByteArrayMessage implements ReActedMessage {
        byte[] payload = null;
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
        }

        @Override
        public void decode(Deserializer deserializer) {
            this.payload = deserializer.getBytes();
        }
    }

    class EnumMessage<T extends Enum<T>> implements ReActedMessage {

        private T payload = null;

        public EnumMessage(T value) {
            this.payload = value;
        }

        public EnumMessage() { }

        public EnumMessage<T> setPayload(T payload) {
            this.payload = payload;
            return this;
        }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
        }

        @Override
        public void decode(Deserializer deserializer) {
            ReActedMessage.super.decode(deserializer);
        }
    }
    class LongMessage implements ReActedMessage {
        private Long payload = null;

        public LongMessage() { }

        public LongMessage(long payload) { this.payload = payload; }
        public Long getPayload() { return payload; }

        public void setPayload(long payload) { this.payload = payload; }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.payload = deserializer.getLong();
        }
    }

    class LongsMessage implements ReActedMessage {
        private long[] payload = null;

        public long[] getPayload() { return payload; }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload.length);
            serializer.put(payload);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.payload = new long[deserializer.getInt()];
            deserializer.getLongs(payload);
        }
    }

    class IntMessage implements ReActedMessage {
        private Integer payload = null;

        public IntMessage() { }

        public IntMessage(int payload) { this.payload = payload; }
        public int getPayload() { return payload; }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.payload = deserializer.getInt();
        }
    }

    class IntsMessage implements ReActedMessage {
        private int[] payload = null;
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload.length);
            serializer.put(payload);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.payload = new int[deserializer.getInt()];
            deserializer.getInts(payload);
        }
    }

    class StringMessage implements ReActedMessage {
        protected String payload = null;

        public StringMessage() { }

        public StringMessage(String payload) { this.payload = payload; }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.payload = deserializer.getString();
        }
    }

    class ThrowableMessage extends SerializableMessage { }
    class SerializableMessage implements ReActedMessage {
        Serializable value = null;
        @Override
        public void encode(Serializer serializer) {
            serializer.putObject(value);
        }
        @Override
        public void decode(Deserializer deserializer) {
            this.value = deserializer.getObject();
        }
    }
}
