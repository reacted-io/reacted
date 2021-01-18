/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class Merge extends Reduce {
    private final Reducer reductions;
    private final Map<Class<? extends Serializable>, Deque<Serializable>> storage;
    private final Map<Class<? extends Serializable>, Long> typeToRequiredForMerge;
    private final Function<Collection<Serializable>, Collection<Serializable>> merger;
    protected Merge(Collection<Class<Serializable>> dataTypesToBeMerged,
                    Function<Collection<Serializable>, Collection<Serializable>> merger) {
        this.merger = merger;
        this.storage = dataTypesToBeMerged.stream()
                                          .collect(Collectors.toUnmodifiableMap(Function.identity(),
                                                                                dataType -> new LinkedList<>()));
        this.typeToRequiredForMerge = Map.copyOf(dataTypesToBeMerged.stream()
                                                                    .collect(Collectors.groupingBy(Function.identity(),
                                                                                                   Collectors.counting())));
        var reductionsBuilder = Reductions.newBuilder();
        dataTypesToBeMerged.forEach(dataType -> reductionsBuilder.setReduction(dataType,
                                                                               this::mergeAttempt));
        this.reductions = reductionsBuilder.build();
    }
    @Override
    public Reducer getReductions() { return reductions; }

    private Collection<Serializable> mergeAttempt(Serializable anyMessage,
                                                  ReActorContext reducerCtx) {
        Deque<Serializable> typeStorage = storage.get(anyMessage.getClass());
        if (typeStorage != null) {
            typeStorage.addLast(anyMessage);
            if (canMerge()) {
                Stack<Serializable> dataForMerge = new Stack<>();
                for(var requiredForMerge : typeToRequiredForMerge.entrySet()) {
                    for (long extracted = 0; extracted < requiredForMerge.getValue(); extracted++) {
                        dataForMerge.add(storage.get(requiredForMerge.getKey()).getFirst());
                    }
                }
                return merger.apply(dataForMerge);
            }
        }
        return List.of();
    }

    private boolean canMerge() {
        for (Map.Entry<Class<? extends Serializable>, Deque<Serializable>> savedMessages : storage.entrySet()) {
            if (savedMessages.getValue().size() < typeToRequiredForMerge.get(savedMessages.getKey())) {
                return false;
            }
        }
        return true;
    }
}
