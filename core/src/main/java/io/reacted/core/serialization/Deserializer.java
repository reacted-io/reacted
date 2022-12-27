/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.serialization;

import java.io.Serializable;

public interface Deserializer {
    long getLong();
    long[] getLongs();
    int getInt();

    int[] getInts();
    String getString();
    byte getByte();
    byte[] getBytes();
    Serializable getObject();
}
