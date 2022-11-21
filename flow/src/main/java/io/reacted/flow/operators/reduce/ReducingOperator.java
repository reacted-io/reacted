/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

import com.google.common.collect.ImmutableMap;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.annotations.unstable.Unstable;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
@Unstable
public abstract class ReducingOperator<ConfigBuilderT extends ReducingOperatorConfig.Builder<ConfigBuilderT, ConfigT>,
                                       ConfigT extends ReducingOperatorConfig<ConfigBuilderT, ConfigT>>
    extends FlowOperator<ConfigBuilderT, ConfigT> {
    private static final Queue<Serializable> NO_ELEMENTS = new LinkedList<>();
    private final Map<Class<? extends Serializable>, Queue<Serializable>> storage;
    protected ReducingOperator(ConfigT config) {
        super(config);
        this.storage = config.getReductionRules().keySet().stream()
                             .collect(Collectors.toMap(Function.identity(), msgType -> new LinkedList<>()));
    }

    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        Collection<? extends Serializable> result = List.of();
        Class<? extends Serializable> inputType = input.getClass();
        if (storage.containsKey(inputType)) {
            storage.computeIfAbsent(inputType, newKey -> new LinkedList<>())
                   .add(input);
            if (canReduce(inputType)) {
                var reduceData = getReduceData(getOperatorCfg().getReductionRules());
                result = getOperatorCfg().getReducer().apply(reduceData);
            }
        }
        return CompletableFuture.completedStage(result);
    }

    private Map<Class<? extends Serializable>, List<? extends Serializable>>
    getReduceData(Map<Class<? extends Serializable>, Long> reductionRules) {
        ImmutableMap.Builder<Class<? extends Serializable>, List<? extends Serializable>> output;
        output = ImmutableMap.builder();
        for(var entry : storage.entrySet()) {
            Class<? extends Serializable> type = entry.getKey();
            var payloads = entry.getValue();
            var required = reductionRules.get(type).intValue();
            var elements = removeNfromInput(payloads, required);
            output.put(type, elements);
        }
        return output.build();
    }

    private static List<Serializable> removeNfromInput(Queue<Serializable> input,
                                                       long howManyToRemove) {
        List<Serializable> output = new LinkedList<>();
        for(int iter = 0; iter < howManyToRemove; iter++) {
            output.add(input.poll());
        }
        return output;
    }
    private boolean canReduce(Class<? extends Serializable> inputType) {
        return getOperatorCfg().getReductionRules().entrySet().stream()
                               .allMatch(typeToNum -> storage.getOrDefault(inputType, NO_ELEMENTS)
                                                             .size() >= typeToNum.getValue()
                                                                                 .intValue());
    }
}
