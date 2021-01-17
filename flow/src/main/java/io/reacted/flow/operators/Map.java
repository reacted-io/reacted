/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;

import io.reacted.core.reactorsystem.ReActorSystem;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Map<InputT extends Serializable, OutputT extends Serializable> extends PipelineStage {
    private final Function<? super InputT, Collection<? extends OutputT>> mapper;
    private Map(Function<? super InputT, Collection<? extends OutputT>> mapper) {
        this.mapper = mapper;
    }

    public static CompletionStage<ReActorRef>
    of(ReActorSystem localReActorSystem, ReActorConfig operatorCfg,
       Function<? extends Serializable, Collection<? extends Serializable>> mapper) {
        return (CompletionStage<ReActorRef>)localReActorSystem.spawn(new Map<>(mapper), operatorCfg)
                                                              .mapOrElse(CompletableFuture::completedFuture,
                                                                         CompletableFuture::failedFuture)
                                                              .orElseSneakyThrow();
    }

    @Override
    protected Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx) {
        return mapper.apply((InputT) input);
    }
}
