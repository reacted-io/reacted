/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.serialization;

import java.io.Serializable;

public interface Serializer {
    void putLong(long value);
    void putLongs(long[] value);
    void putInt(int value);

    void putInts(int[] value);
    void putString(String value);
    void putByte(byte value);
    void putBytes(byte[] value);

    void putObject(Serializable value);
    default void put(String value) { putString(value);}
    default void put(long value) { putLong(value); }
    default void put(int value) { putInt(value); }
    default void put(byte value) { putByte(value); }
    default void put(byte[] value) { putBytes(value); }

    default void put(int[] value) { putInts(value);}
    default void put(long[] value) { putLongs(value); }
}
