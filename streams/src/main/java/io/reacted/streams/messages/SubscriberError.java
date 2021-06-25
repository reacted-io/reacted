/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams.messages;

import java.io.Serializable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SubscriberError implements Serializable {
    private final Throwable error;
    public SubscriberError(Throwable error) {
        this.error = error;
    }
    public Throwable getError() { return error; }

    @Override
    public String toString() {
        return "SubscriberError{" +
               "error=" + error +
               '}';
    }
}
