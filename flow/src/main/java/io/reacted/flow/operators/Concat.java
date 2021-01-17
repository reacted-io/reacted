/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactorsystem.ReActorRef;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public final class Concat extends Merge {
    public Concat() {
        super(List.of(Serializable.class), Function.identity());
    }
}
