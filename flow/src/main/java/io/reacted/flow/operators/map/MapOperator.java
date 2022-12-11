/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.map;

import io.reacted.core.reactorsystem.ReActorContext;

import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.map.MapOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@NonNullByDefault
public class MapOperator extends FlowOperator<Builder, MapOperatorConfig> {
    private final Function<Object, Collection<? extends Serializable>> mappingFunction;

    protected MapOperator(MapOperatorConfig config) {
        super(config);
        this.mappingFunction = Objects.requireNonNull(config.getMapper(),
                                                      "Mapping function cannot be null");
    }

    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext ctx) {
        return CompletableFuture.completedStage(mappingFunction.apply(input));
    }
}
