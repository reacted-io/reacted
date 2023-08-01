/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;

@Immutable
@NonNullByDefault
public class ReActedError extends LogMessage {
    public ReActedError(String format, Serializable ...arguments) {
        super(format, arguments);
    }
}
