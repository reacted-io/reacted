/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZipOperator extends FlowOperator<Builder,
    ZipOperatorConfig> {
    private final Map<Class<? extends Serializable>, Long> zipperRequiredTypes;
    private final Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                           Collection<? extends Serializable>> zipper;
    private final Map<Class<? extends Serializable>, List<Serializable>> storage;
    protected ZipOperator(ZipOperatorConfig config) {
        super(config);
        this.zipper = config.getZipper();
        this.zipperRequiredTypes = config.getZipperRequiredTypes();
        this.storage = zipperRequiredTypes.keySet().stream()
                                          .collect(Collectors.toUnmodifiableMap(Function.identity(),
                                                                                dataType -> new LinkedList<>()));
    }

    @Override
    protected CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx) {
        if (zipperRequiredTypes.containsKey(input.getClass())) {
            storage.get(input.getClass()).add(input);
            if (canZip()) {
                return CompletableFuture.completedStage(zipper.apply(retrieveZipData()));
            }
        }
        return CompletableFuture.completedStage(List.of());
    }

    private Map<Class<? extends Serializable>, List<? extends Serializable>> retrieveZipData() {
        return zipperRequiredTypes.entrySet().stream()
                                  .collect(Collectors.toMap(Entry::getKey,
                                                        entry -> removeNfromInput(storage.get(entry.getKey()),
                                                                                  entry.getValue()),
                                                        (f, s) -> { throw new UnsupportedOperationException(); }));
    }

    private boolean canZip() {
        return zipperRequiredTypes.entrySet().stream()
                                  .allMatch(entry -> storage.get(entry.getKey()).size() >=
                                                 entry.getValue());
    }

    private static List<Serializable> removeNfromInput(List<Serializable> input, long n) {
        var output = new LinkedList<Serializable>();
        for(int removedNum = 0; removedNum < n; removedNum++) {
            output.add(input.remove(0));
        }
        return output;
    }
}
