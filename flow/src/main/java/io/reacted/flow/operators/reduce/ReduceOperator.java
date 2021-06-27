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
import java.util.LinkedList;
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
                      Map<ReduceKey, List<Serializable>>> storage;
    protected ReduceOperator(ReduceOperatorConfig config) {
        super(config);
        this.storage = config.getKeyExtractors().keySet().stream()
                             .collect(Collectors.toUnmodifiableMap(Function.identity(),
                                                                   type -> new HashMap<>()));
    }

    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        Class<? extends Serializable> inputType = input.getClass();
        if (storage.containsKey(inputType)) {
            ReduceKey key = getOperatorCfg().getKeyExtractors()
                                            .get(input.getClass()).apply(input);
            storage.get(inputType).putIfAbsent(key, new LinkedList<>())
                   .add(input);
            if (canReduce(key)) {
                return CompletableFuture.completedStage(getOperatorCfg().getReducer()
                                                                        .apply(getReduceData(key)));
            }
        }
        return CompletableFuture.completedStage(List.of());
    }

    private Map<Class<? extends Serializable>,
                List<? extends Serializable>> getReduceData(ReduceKey key) {
        return storage.values().stream()
                      .map(keyStorage -> keyStorage.remove(key))
                      .collect(Collectors.toUnmodifiableMap(payload -> payload.get(0).getClass(),
                                                            Function.identity()));
    }
    private boolean canReduce(ReduceKey reduceKey) {
        return getOperatorCfg().getReductionRules().entrySet().stream()
                               .allMatch(typeToNum -> storage.getOrDefault(typeToNum.getKey(),
                                                                           Map.of())
                                                             .getOrDefault(reduceKey, List.of())
                                                             .size() == typeToNum.getValue());
    }
}
