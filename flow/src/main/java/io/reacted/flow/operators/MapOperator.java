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
public class MapOperator extends FlowOperator {
    private final MapOperatorConfig mapperConfig;
    private MapOperator(MapOperatorConfig operatorConfig) {
        this.mapperConfig = operatorConfig;
    }

    public static CompletionStage<Try<ReActorRef>>
    of(ReActorSystem localReActorSystem, MapOperatorConfig operatorCfg) {
        return CompletableFuture.completedStage(localReActorSystem.spawn(new MapOperator(operatorCfg),
                                                                         operatorCfg));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        return CompletableFuture.completedStage(mapperConfig.getMappingFunction().apply(input));
    }
}
