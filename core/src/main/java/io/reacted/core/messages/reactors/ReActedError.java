/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Optional;

@Immutable
@NonNullByDefault
public class ReActedError implements Serializable {
    @Nullable
    private final Throwable error;
    @Nullable
    private final String message;

    public ReActedError(@Nullable String message, @Nullable Throwable error, Object ...args) {
        if (message != null) {
            this.message = String.format(message, args);
        } else {
            this.message = null;
        }
        this.error = error;
    }

    public Optional<Throwable> getError() { return Optional.ofNullable(error); }

    public Optional<String> getMessage() { return Optional.ofNullable(message); }
}
