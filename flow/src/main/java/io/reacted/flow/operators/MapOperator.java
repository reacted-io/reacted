/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;

import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@NonNullByDefault
public class MapOperator<InputT extends Serializable, OutputT extends Serializable> extends FlowStage {
    private final Function<? super InputT, Collection<? extends OutputT>> mapperFunction;
    private MapOperator(Function<? super InputT, Collection<? extends OutputT>> mapperFunction) {
        this.mapperFunction = mapperFunction;
    }

    public static CompletionStage<Try<ReActorRef>>
    of(ReActorSystem localReActorSystem, ReActorConfig operatorCfg,
       Function<? extends Serializable, Collection<? extends Serializable>> mapper) {
        return CompletableFuture.completedStage(localReActorSystem.spawn(new MapOperator<>(mapper),
                                                                         operatorCfg));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx) {
        return mapperFunction.apply((InputT) input);
    }
}
