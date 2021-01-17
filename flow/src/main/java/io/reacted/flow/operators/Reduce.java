/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;

@NonNullByDefault
public abstract class Reduce extends PipelineStage {
    protected Reduce() { }
    protected abstract Reducer getReductions();
    @Override
    protected Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx) {
        return getReductions().reduce(raCtx, input);
    }
}
