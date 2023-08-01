/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;

import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
@NonNullByDefault
public final class StorageMessages {
    private StorageMessages() { /* No implementation required*/ }

    public record QueryReply(String payload) implements ReActedMessage {
            public QueryReply(String payload) {
                this.payload = Objects.requireNonNull(payload);
            }
        }

    public record QueryRequest(String key) implements ReActedMessage {
            public QueryRequest(String key) {
                this.key = Objects.requireNonNull(key);
            }
        }

    public static class StoreError implements ReActedMessage {
        private Throwable error;
        public StoreError() { }
        public StoreError(Throwable anyError) { this.error = anyError; }

        @Override
        public Class<? extends ReActedMessage> getMessageType() { return StoreError.class; }

        @Override
        public void encode(Serializer serializer) {
            serializer.putObject(error);
        }

        @Override
        public void decode(Deserializer deserializer) {
            this.error = deserializer.getObject();
        }

        @Override
        public String toString() { return "StoreError{" + "error=" + error + '}'; }
    }

    public static class StoreReply implements ReActedMessage { }

    public static class StoreRequest implements ReActedMessage {
        private String key;
        private String payload;
        public StoreRequest(String key, String payload) {
            this.payload = Objects.requireNonNull(payload);
            this.key = Objects.requireNonNull(key);
        }

        public String getPayload() { return payload; }
        public String getKey() { return key; }
        @Override
        public void encode(Serializer serializer) {
            serializer.put(payload);
            serializer.put(key);
        }

        @Override
        public void decode(Deserializer deserializer) {
            this.payload = deserializer.getString();
            this.key = deserializer.getString();
        }
    }
}
