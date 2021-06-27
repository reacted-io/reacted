/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.reduce.ReduceOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ReduceOperator extends FlowOperator<Builder,
                                                 ReduceOperatorConfig> {
    private final Map<Class<? extends Serializable>,
                      Map<ReduceKey, Serializable>> storage;
    protected ReduceOperator(ReduceOperatorConfig config) {
        super(config);
        this.storage = config.getKeyExtractors().keySet().stream()
                             .collect(Collectors.toUnmodifiableMap(Function.identity(),
                                                                   type -> new HashMap<>()));
    }

    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        if (storage.containsKey(input.getClass())) {
            ReduceKey key = getOperatorCfg().getKeyExtractors()
                                            .get(input.getClass()).apply(input);
            storage.get(input.getClass()).putIfAbsent(key, input);
            if (canReduce(key)) {
                return CompletableFuture.completedStage(getOperatorCfg().getReducer()
                                                                        .apply(getReduceData(key)));
            }
        }
        return CompletableFuture.completedStage(List.of());
    }

    private Map<Class<? extends Serializable>, Serializable> getReduceData(ReduceKey key) {
        return storage.values().stream()
                      .map(keyStorage -> keyStorage.remove(key))
                      .collect(Collectors.toUnmodifiableMap(Serializable::getClass,
                                                            Function.identity()));
    }
    private boolean canReduce(ReduceKey reduceKey) {
        return storage.values().stream()
                      .allMatch(keyStorage -> keyStorage.containsKey(reduceKey));
    }
}
