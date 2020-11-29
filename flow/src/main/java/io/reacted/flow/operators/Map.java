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

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Function;

public class Map<InputT extends Serializable, OutputT extends Serializable> extends PipelineStage {
    private final Function<? super InputT, Collection<? extends OutputT>> mapper;
    public Map(Collection<ReActorRef> nextStages, Function<? super InputT, Collection<? extends OutputT>> mapper) {
        super(nextStages);
        this.mapper = mapper;
    }
    @Override
    protected Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx) {
        return mapper.apply((InputT) input);
    }
}
