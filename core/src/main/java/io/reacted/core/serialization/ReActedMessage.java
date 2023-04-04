/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.serialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

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

        @Override
        public String toString() { return "EnumMessage{" + "payload=" + payload + '}'; }
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

        @Override
        public String toString() { return "LongMessage{" + "payload=" + payload + '}'; }
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

        @Override
        public String toString() { return "LongsMessage{" + "payload=" + Arrays.toString(payload) + '}'; }
    }

    class IntMessage implements ReActedMessage {
        public static final Comparator<IntMessage> COMPARATOR = Comparator.comparingInt(m -> m.payload);
        private int payload;

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

        @Override
        public String toString() { return "IntMessage{" + "payload=" + payload + '}'; }
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

        @Override
        public String toString() { return "IntsMessage{" + "payload=" + Arrays.toString(payload) + '}'; }
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

        @Override
        public String toString() { return payload; }
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
