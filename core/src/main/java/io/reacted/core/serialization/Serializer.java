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
    <T extends Enum<T>> Serializer putEnum(T value);

    Serializer putLong(long value);
    Serializer putLongs(long[] value);
    Serializer putInt(int value);

    Serializer putInts(int[] value);
    Serializer putString(String value);
    Serializer putByte(byte value);
    Serializer putBytes(byte[] value);

    Serializer putObject(Serializable value);
    default Serializer put(String value) { return putString(value);}
    default Serializer put(long value) { return putLong(value); }
    default Serializer put(int value) { return putInt(value); }
    default Serializer put(byte value) { return putByte(value); }
    default Serializer put(byte[] value) { return putBytes(value); }

    default Serializer put(int[] value) { return putInts(value);}
    default Serializer put(long[] value) { return putLongs(value); }

    default <T extends Enum<T>> Serializer put(T value) { return putEnum(value); }
}
