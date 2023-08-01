/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages;

import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

@NonNullByDefault
public final class SerializationUtils {
    public static final ReActedMessage NO_PAYLOAD = new ReActedMessage() {
        @Override
        public void encode(Serializer serializer) {

        }

        @Override
        public void decode(Deserializer deserializer) {

        }
    };
    private static final Unsafe UNSAFE = Try.of(SerializationUtils::getUnsafeRef)
                                            .orElseSneakyThrow();

    private SerializationUtils() { /* No Implementation required */ }

    public static Try<Long> getFieldOffset(Class<?> messageClass, String fieldName) {
        return Try.of(() -> UNSAFE.objectFieldOffset(messageClass.getDeclaredField(fieldName)));
    }

    public static <TypeT> TypeT setObjectField(TypeT object, long offset, Object value) {
        UNSAFE.putObject(object, offset, value);
        return object;
    }

    public static <TypeT> TypeT setIntField(TypeT object, long offset, int value) {
        UNSAFE.putInt(object, offset, value);
        return object;
    }

    public static <TypeT> TypeT setLongField(TypeT object, long offset, long value) {
        UNSAFE.putLong(object, offset, value);
        return object;
    }

    private static Unsafe getUnsafeRef() throws NoSuchFieldException, IllegalAccessException {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (Unsafe)unsafeField.get(null);
    }
}
