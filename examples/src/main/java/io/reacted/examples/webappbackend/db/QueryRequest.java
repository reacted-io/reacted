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
public class QueryRequest implements Serializable {
    private final String key;
    public QueryRequest(String key) {
        this.key = Objects.requireNonNull(key);
    }

    public String getKey() {
        return key;
    }
}
