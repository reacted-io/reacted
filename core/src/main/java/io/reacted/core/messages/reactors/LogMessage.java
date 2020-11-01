/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@NonNullByDefault
public abstract class LogMessage implements Serializable {
    private final String format;
    private final String[] arguments;
    public LogMessage(String format, Serializable ...arguments) {
        this.format = Objects.requireNonNull(format);
        this.arguments = Arrays.stream(Objects.requireNonNull(arguments))
                               .map(argument -> argument instanceof Throwable
                                                ? ExceptionUtils.getStackTrace((Throwable)argument)
                                                : argument.toString())
                               .toArray(String[]::new);
    }

    public String getFormat() { return format; }

    public String[] getArguments() { return arguments; }

    @Override
    public String toString() {
        return "LogMessage{" + "format='" + format + '\'' + ", arguments=" + Arrays.toString(arguments) + '}';
    }
}
