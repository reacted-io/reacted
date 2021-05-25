/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.flow.operators.messages.SetNextStagesRequest;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@SuppressWarnings("NotNullFieldNotInitialized")
@NonNullByDefault
public class ReActedGraph implements FlowGraph {
    private final String flowName;
    private final Map<String, Stage> flowStagesByName;
    private final Map<String, ReActorRef> flowOperatorByStageName;
    private ReActedGraph(Builder builder) {
        this.flowStagesByName = validateFlowStages(Objects.requireNonNull(builder.flowStages,
                                                                          "flow Stages cannot be null")).stream()
        .collect(Collectors.toUnmodifiableMap(Stage::getStageName,
                                              Function.identity()));
        this.flowName = Objects.requireNonNull(builder.flowName, "Flow name cannot be null");
        this.flowOperatorByStageName = new HashMap<>();
    }

    @Nonnull
    @Override
    public String getFlowName() { return flowName; }
    
    @Nonnull
    @Override
    public Collection<Stage> getFlowStages() { return flowStagesByName.values(); }

    @Override
    public void stop(ReActorSystem localReActorSystem) {
        destroyGraph(localReActorSystem, flowOperatorByStageName.values());
        flowOperatorByStageName.clear();
    }

    @Override
    public Try<Void> run(ReActorSystem localReActorSystem) {
        stop(localReActorSystem);
        try {
            for (Entry<String, Stage> currentStage : flowStagesByName.entrySet()) {
                flowOperatorByStageName.put(currentStage.getKey(),
                                            currentStage.getValue()
                                                        .getOperatorProvider()
                                                        .apply(localReActorSystem)
                                                        .toCompletableFuture()
                                                        .join()
                                                        .orElseSneakyThrow());
            }

            for(Entry<String, ReActorRef> stageOperator : flowOperatorByStageName.entrySet()) {
                linkStage(stageOperator.getValue(),
                          flowStagesByName.get(stageOperator.getKey())
                                          .getOutputStagesNames()
                                          .stream()
                                          .map(flowOperatorByStageName::get)
                                          .collect(Collectors.toUnmodifiableSet())).orElseSneakyThrow();
            }

            flowStagesByName.entrySet().stream()
                            .filter(stageEntry -> hasSourceStreams(stageEntry.getValue()))
                            .forEach(stageEntry -> stageEntry.getValue().getSourceStreams()
                                                             .forEach(inputStream -> spawnNewStreamConsumer(flowOperatorByStageName.get(stageEntry.getKey()),
                                                                                                            inputStream, localReActorSystem, flowName,
                                                                                                            stageEntry.getKey())));
        } catch (Exception operatorsInitError) {
            destroyGraph(localReActorSystem, flowOperatorByStageName.values());
            return Try.ofFailure(operatorsInitError);
        }
        return Try.VOID;
    }
    public static Builder newBuilder() { return new Builder(); }

    private static Try<Void> linkStage(ReActorRef operator, Set<ReActorRef> outputOperators) {
        return Try.of(() -> operator.atell(operator, new SetNextStagesRequest(outputOperators))
                                    .toCompletableFuture()
                                    .join())
                  .flatMap(Try::identity)
                  .filter(DeliveryStatus::isDelivered)
                  .flatMap(delivered -> Try.VOID);
    }

    private static boolean hasSourceStreams(Stage stage) {
        return !stage.getSourceStreams().isEmpty();
    }
    private static void spawnNewStreamConsumer(ReActorRef operator,
                                               Stream<? extends Serializable> inputStream,
                                               ReActorSystem localReActorSystem,
                                               String flowName, String stageName) {
        ExecutorService streamConsumerExecutor = spawnNewStageInputStreamExecutor(localReActorSystem,
                                                                                  flowName,
                                                                                  stageName);

        streamConsumerExecutor.submit(() -> { inputStream.forEach(operator::tell);
                                              streamConsumerExecutor.shutdown(); });
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
        private final Collection<Stage> flowStages;
        private String flowName;
        private Builder() { this.flowStages = new HashSet<>(); }

        public Builder setFlowName(String flowName) {
            this.flowName = flowName;
            return this;
        }

        public Builder addStage(Collection<Stage> flowStages) {
            this.flowStages.addAll(flowStages);
            return this;
        }

        public Builder addStage(Stage flowStage) {
            this.flowStages.add(flowStage);
            return this;
        }

        /**
         * Build a runnable graph
         * @return A runnable FlowGraph
         * @throws IllegalArgumentException is stage names are not unique or there are missing stages
         */
        public ReActedGraph build() { return new ReActedGraph(this); }
    }

    private static Collection<Stage> validateFlowStages(Collection<Stage> flowStages) {
        if (areStagesNamesNotUnique(flowStages)) {
            throw new IllegalArgumentException("Stages names are not unique");
        }

        if (areThereOutputStagesNotDefined(flowStages)) {
            throw new IllegalArgumentException("Output stages are not fully defined");
        }
        return flowStages;
    }
    private static boolean areStagesNamesNotUnique(Collection<Stage> flowStages) {
        return flowStages.size() != flowStages.stream()
                                              .map(Stage::getStageName)
                                              .distinct()
                                              .count();
    }
    private static boolean areThereOutputStagesNotDefined(Collection<Stage> flowStages) {
        var stageNames = flowStages.stream()
                                   .map(Stage::getStageName)
                                   .collect(Collectors.toUnmodifiableSet());
        return flowStages.stream()
                      .flatMap(stage -> stage.getOutputStagesNames().stream())
                      .anyMatch(Predicate.not(stageNames::contains));
    }
}
