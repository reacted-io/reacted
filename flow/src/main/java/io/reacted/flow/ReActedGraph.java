/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.FlowOperatorConfig.Builder;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@SuppressWarnings("NotNullFieldNotInitialized")
@NonNullByDefault
public class ReActedGraph implements FlowGraph {
    private final String flowName;
    private final Collection<? extends FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?,?>,
                                                          ? extends FlowOperatorConfig<?, ?>>> operatorsCfgs;
    private final Map<String, ReActorRef> operatorNameToOperator;
    private ReActedGraph(Builder builder) {
        this.flowName = Objects.requireNonNull(builder.flowName, "Flow name cannot be null");
        this.operatorsCfgs = List.copyOf(Objects.requireNonNull(builder.operatorsCfgs,
                                                                    "Flow operators cannot be null"));
        this.operatorNameToOperator = new ConcurrentHashMap<>();
    }

    @Nonnull
    @Override
    public String getFlowName() { return flowName; }
    
    @Override
    public void stop(ReActorSystem localReActorSystem) {
        destroyGraph(localReActorSystem, operatorNameToOperator.values());
        operatorNameToOperator.clear();
    }

    @Override
    public Try<Void> run(ReActorSystem localReActorSystem) {
        stop(localReActorSystem);
        try {
            for(var operatorCfg : operatorsCfgs) {
                operatorNameToOperator.put(operatorCfg.getReActorName(),
                                           localReActorSystem.spawnService(operatorCfg)
                                                             .orElseSneakyThrow());
            }
            operatorsCfgs.stream()
                         .filter(operatorCfg -> !operatorCfg.getInputStreams().isEmpty())
                         .forEachOrdered(operatorConfig -> operatorConfig.getInputStreams()
                                                                         .forEach(inputStream -> spawnNewStreamConsumer(operatorNameToOperator.get(operatorConfig.getReActorName()),
                                                                                                                        inputStream, localReActorSystem, flowName,
                                                                                                                        operatorConfig)));
        } catch (Exception operatorsInitError) {
            destroyGraph(localReActorSystem, operatorNameToOperator.values());
            return Try.ofFailure(operatorsInitError);
        }
        return Try.VOID;
    }

    public static Builder newBuilder() { return new Builder(); }
    private static
    void spawnNewStreamConsumer(ReActorRef operator, Stream<? extends Serializable> inputStream,
                                ReActorSystem localReActorSystem, String flowName,
                                FlowOperatorConfig<?, ?> operatorCfg) {
        ExecutorService streamConsumerExecutor = spawnNewStageInputStreamExecutor(localReActorSystem,
                                                                                  flowName,
                                                                                  operatorCfg.getReActorName());
        var errorHandler = (TriConsumer<ReActorSystem, Object, ? super Throwable>) operatorCfg.getInputStreamErrorHandler();
        AsyncUtils.asyncForeach(operator::atell, inputStream.iterator(),
                                error -> errorHandler.accept(localReActorSystem, operatorCfg,
                                                             error),
                                streamConsumerExecutor)
                  .thenAccept(finished -> streamConsumerExecutor.shutdownNow());
    }

    private static ExecutorService spawnNewStageInputStreamExecutor(ReActorSystem localReActorSystem,
                                                                    String flowName, String stageName) {
        var inputStreamThreadFactory = new ThreadFactoryBuilder();
        inputStreamThreadFactory.setNameFormat(String.format("InputStreamExecutor-Flow[%s]-Stage[%s]-",
                                                             flowName, stageName))
                                .setUncaughtExceptionHandler((thread, error) -> localReActorSystem.logError("Uncaught exception in {}",
                                                                                                            thread.getName(), error));
        return Executors.newSingleThreadExecutor(inputStreamThreadFactory.build());
    }

    private static void destroyGraph(ReActorSystem localReActorSystem,
                                     Collection<ReActorRef> operators) {
        operators.stream()
                 .filter(localReActorSystem::isLocal)
                 .map(ReActorRef::getReActorId)
                 .forEach(localReActorSystem::stop);
    }

    public static class Builder {
        private final Collection<FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?,?>,
                                                    ? extends FlowOperatorConfig<?, ?>>> operatorsCfgs;
        private String flowName;
        private Builder() { this.operatorsCfgs = new LinkedList<>(); }

        public Builder setFlowName(String flowName) {
            this.flowName = flowName;
            return this;
        }

        public <BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
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
        public ReActedGraph build() { return new ReActedGraph(this); }
    }
}
