/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactorsystem.ReActorContext;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;

public interface Reducer {
    @Nonnull
    Collection<? extends Serializable> reduce(@Nonnull ReActorContext raCtx,
                                              @Nonnull Serializable message);
}
