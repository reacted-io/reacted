/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;

import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Objects;
@NonNullByDefault
public final class StorageMessages {
    private StorageMessages() { /* No implementation required*/ }
    public static class QueryReply implements Serializable {
        private final String payload;
        public QueryReply(String payload) {
            this.payload = Objects.requireNonNull(payload);
        }
        public String getPayload() { return payload; }
    }
    public static class QueryRequest implements Serializable {
        private final String key;
        public QueryRequest(String key) { this.key = Objects.requireNonNull(key); }
        public String getKey() { return key; }
    }

    public static class StoreError extends Throwable {
        public StoreError(Throwable anyError) { super(anyError); }
    }

    public static class StoreReply implements Serializable { }
    public static class StoreRequest implements Serializable {
        private final String payload;
        private final String key;
        public StoreRequest(String key, String payload) {
            this.payload = Objects.requireNonNull(payload);
            this.key = Objects.requireNonNull(key);
        }
        public Serializable getPayload() { return payload; }
        public String getKey() { return key; }
    }
}
