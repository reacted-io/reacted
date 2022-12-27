/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.core.serialization.ReActedMessage;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ReActorStop implements ReActedMessage {
    public static final ReActorStop STOP = new ReActorStop();
    @Override
    public String toString() {
        return "ReActorStop{}";
    }
}
