/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.exceptions;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;

@Immutable
@NonNullByDefault
public class ReActorSystemInitException extends RuntimeException {
    public ReActorSystemInitException(Throwable cause) { super(cause); }
    public ReActorSystemInitException(String cause) { super(cause); }
}
