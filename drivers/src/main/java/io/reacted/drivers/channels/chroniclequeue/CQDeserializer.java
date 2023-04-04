/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.serialization.Deserializer;
import io.reacted.patterns.NonNullByDefault;
import net.openhft.chronicle.wire.WireIn;

import java.io.Serializable;

@NonNullByDefault
public class CQDeserializer implements Deserializer {
    private WireIn input;

    public CQDeserializer(WireIn input) { setDeserializerInput(input); }

    CQDeserializer setDeserializerInput(WireIn input) { this.input = input; return this; }

    @Override
    public <T extends Enum<T>> T getEnum(Class<T> type) { return input.read().asEnum(type); }
    @Override
    public long getLong() { return input.read().readLong(); }

    @Override
    public int getLongs(long[] target) {
        return input.read().array(target);
    }

    @Override
    public int getInt() { return input.read().int32(); }


    @Override
    public int getInts(int[] target) {
        return input.read().array(target);
    }

    @Override
    public String getString() { return input.read().readString(); }

    @Override
    public byte getByte() { return input.read().readByte(); }

    @Override
    public byte[] getBytes() { return input.read().bytes(); }

    @Override
    public <T extends Serializable> T getObject() { return (T)input.read().object(); }

    @Override
    public Serializable getObject(Serializable target) { return input.read().object(target, target.getClass()); }
}
