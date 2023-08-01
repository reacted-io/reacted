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
    <T extends Enum<T>> T getEnum(Class<T> type);

    long getLong();
    int getLongs(long[] target);

    int getInt();

    int getInts(int[] target);

    String getString();
    byte getByte();
    byte[] getBytes();

    <T extends Serializable> T getObject();

    Serializable getObject(Serializable target);
}
