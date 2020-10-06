/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Objects;

@NonNullByDefault
public abstract class LogMessage implements Serializable {
    private final String format;
    private final Serializable[] arguments;
    public LogMessage(String format, Serializable ...arguments) {
        this.format = Objects.requireNonNull(format);
        this.arguments = Objects.requireNonNull(arguments);
    }

    public String getFormat() {
        return format;
    }

    public Serializable[] getArguments() {
        return arguments;
    }
}
