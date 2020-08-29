/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.pingpong;

import java.io.Serializable;

class Pong implements Serializable {
    private final int pongValue;

    Pong(int pongValue) {
        this.pongValue = pongValue;
    }

    public int getPingValue() {
        return pongValue;
    }
}
