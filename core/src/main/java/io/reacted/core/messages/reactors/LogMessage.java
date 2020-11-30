/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.helpers.MessageFormatter;

import java.io.Serializable;
@NonNullByDefault
public abstract class LogMessage implements Serializable {
    private final String message;
    public LogMessage(String format, Serializable ...arguments) {
        var formattingTuple = MessageFormatter.arrayFormat(format, arguments);
        this.message = formattingTuple.getMessage() + (formattingTuple.getThrowable() != null
                                                       ? ExceptionUtils.getStackTrace(formattingTuple.getThrowable())
                                                       : "");
    }

    public String getMessage() { return message; }

    @Override
    public String toString() {
        return "LogMessage{" + "message='" + message + '\'' + '}';
    }
}
