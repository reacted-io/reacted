/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;
import net.openhft.chronicle.wire.WireOut;

import java.io.Serializable;

@NonNullByDefault
public class CQSerializer implements Serializer {

    private WireOut out;

    CQSerializer(WireOut out) {setSerializerOutput(out);}

    CQSerializer setSerializerOutput(WireOut out) {
        this.out = out;
        return this;
    }

    @Override
    public <T extends Enum<T>> Serializer putEnum(T value) {
        out.write()
           .asEnum(value);
        return this;
    }

    @Override
    public Serializer putLong(long value) {
        out.write()
           .writeLong(value);
        return this;
    }

    @Override
    public Serializer putLongs(long[] value) {
        out.write()
           .array(value, value.length);
        return this;
    }

    @Override
    public Serializer putInt(int value) {
        out.write()
           .int32(value);
        return this;
    }

    @Override
    public Serializer putInts(int[] value) {
        out.write()
           .array(value, value.length);
        return this;
    }

    @Override
    public Serializer putString(String value) {
        out.write()
           .writeString(value);
        return this;
    }

    @Override
    public Serializer putByte(byte value) {
        out.write()
           .int8(value);
        return this;
    }

    @Override
    public Serializer putBytes(byte[] value) {
        out.write()
           .array(value, value.length);
        return this;
    }

    @Override
    public Serializer putObject(Serializable value) {
        out.write()
           .object(value);
        return this;
    }
}
