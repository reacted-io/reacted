/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.pingpong;

import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;

class Ping implements ReActedMessage {
    int pingValue;

    Ping(int pingValue) { this.pingValue = pingValue; }
    @Override
    public void encode(Serializer serializer) {
        serializer.put(pingValue);
    }

    @Override
    public void decode(Deserializer deserializer) {
        this.pingValue = deserializer.getInt();
    }
}
