/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import com.google.common.collect.Streams;
import io.reacted.core.config.reactors.ReActiveEntityConfig;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.messages.OperatorInitComplete;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.annotations.unstable.Unstable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NonNullByDefault
@Unstable
public class ReActedGraph extends ReActiveEntityConfig<ReActedGraph.Builder,
                                                       ReActedGraph> implements FlowGraph {
    private final Collection<? extends FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?,?>,
                                                          ? extends FlowOperatorConfig<?, ?>>> operatorsCfgs;
    private Map<String, ReActorRef> operatorsByName;
    @Nullable
    private ReActorRef graphControllerGate;
    @SuppressWarnings("unchecked")
    private ReActedGraph(Builder builder) {
        super(builder);
        this.operatorsCfgs = (Collection<? extends FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?, ?>, ? extends FlowOperatorConfig<?, ?>>>)
            Objects.requireNonNull(builder.operatorsCfgs, "Flow operators cannot be null").stream()
                   .map(operatorCfg -> operatorCfg.toBuilder()
                                                  .setFlowName(getFlowName())
                                                  .setDispatcherName(operatorCfg.getDispatcherName().equals(Dispatcher.DEFAULT_DISPATCHER_NAME)
                                                                     ? getDispatcherName()
                                                                     : operatorCfg.getDispatcherName())
                                                  .build())
                   .collect(Collectors.toUnmodifiableList());
        this.operatorsByName = Map.of();
    }

    @Nonnull
    @Override
    public String getFlowName() { return getReActorName(); }
    
    @Override
    public Optional<CompletionStage<Void>> stop(ReActorSystem localReActorSystem) {
        this.operatorsByName = Map.of();
        return getGraphController().map(ReActorRef::getReActorId)
                                   .flatMap(localReActorSystem::stop);
    }

    public Optional<ReActorRef> getGraphController() {
        return Optional.ofNullable(graphControllerGate);
    }

    @Nonnull
    @Override
    public CompletionStage<Try<Void>> run(ReActorSystem localReActorSystem) {
        CompletableFuture<Try<Void>> graphInited = new CompletableFuture<>();
        var graphController = new GraphController(getFlowName(), operatorsCfgs, graphInited);
        var spawnResult = localReActorSystem.spawn(graphController, this)
                                            .map(controller -> {
                                                this.graphControllerGate = controller;
                                                this.operatorsByName = graphController.getOperatorsByName();
                                                return null; });
        spawnResult.ifError(error -> graphInited.complete(Try.ofFailure(error)));
        return graphInited;
    }

    @Nonnull
    @Override
    public Map<String, ReActorRef> getOperatorsByName() { return Map.copyOf(operatorsByName); }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends ReActiveEntityConfig.Builder<Builder, ReActedGraph> {
        private final Collection<FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?,?>,
                                                    ? extends FlowOperatorConfig<?, ?>>> operatorsCfgs;
        private Builder() { this.operatorsCfgs = new LinkedList<>(); }

        public final
        <BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
         BuiltT extends FlowOperatorConfig<BuilderT, BuiltT>,
         OperatorCfgT extends FlowOperatorConfig<BuilderT, BuiltT>>
        Builder addOperator(OperatorCfgT operatorCfg) {
            operatorsCfgs.add(operatorCfg);
            return this;
        }
        /**
         * Build a runnable graph
         * @return A runnable FlowGraph
         * @throws IllegalArgumentException is stage names are not unique or there are missing stages
         */
        public ReActedGraph build() {
            setTypedSubscriptions(Streams.concat(Arrays.stream(typedSubscriptions),
                                                 Stream.of(TypedSubscription.LOCAL.forType(OperatorInitComplete.class)))
                                         .distinct()
                                         .toArray(TypedSubscription[]::new));
            return new ReActedGraph(this);
        }
    }
}
