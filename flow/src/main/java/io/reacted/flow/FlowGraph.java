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
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NonNullByDefault
public class FlowGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowGraph.class);
    private final String flowName;
    private final Collection<Stage> pipelineStages;
    private Map<String, CompletionStage<ReActorRef>> pipelineOperators;
    private FlowGraph(Builder builder) {
        this.pipelineStages = validatePipelineStages(Objects.requireNonNull(builder.pipelineStages,
                                                                            "Pipeline Stages cannot be null"));
        this.flowName = Objects.requireNonNull(builder.flowName,
                                               "Flow name cannot be null");
    }

    public Collection<Stage> getPipelineStages() { return pipelineStages; }

    public void run(ReActorSystem localReActorSystem) {
        Map<String, Stage> stageMap = pipelineStages.stream()
                                                    .collect(Collectors.toUnmodifiableMap(Stage::getStageName,
                                                             Function.identity()));
        this.pipelineOperators = pipelineStages.stream()
                      .collect(Collectors.toUnmodifiableMap(Stage::getStageName,
                                                            stage -> stage.getOperatorProvider()
                                                                           .apply(localReActorSystem)));
        for(var entry : stageMap.entrySet()) {
            String stageName = entry.getKey();
            Collection<Stream<? extends Serializable>> stageInputs = entry.getValue().getSourceStreams();
            if (stageInputs.isEmpty()) {
                continue;
            }
            pipelineOperators.get(stageName)
                             .thenAccept(operator -> stageInputs.forEach(input -> spawnNewStreamConsumer(operator, input, localReActorSystem, flowName, stageName)));
        }
        
    }
    public static Builder newBuilder() { return new Builder(); }

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
        ThreadFactoryBuilder inputStreamThreadFactory = new ThreadFactoryBuilder();
        inputStreamThreadFactory.setNameFormat(String.format("InputStreamExecutor-Flow[%s]-Stage[%s]-",
                                                             flowName, stageName))
                                .setUncaughtExceptionHandler((thread, error) -> localReActorSystem.logError("Uncaught exception in {}", thread.getName(), error));

        return Executors.newSingleThreadExecutor(inputStreamThreadFactory.build());
    }

    public static class Builder {
        private Collection<Stage> pipelineStages;
        private String flowName;
        private Builder() { this.pipelineStages = new HashSet<>(); }

        public Builder setFlowName(String flowName) {
            this.flowName = flowName;
            return this;
        }

        public Builder addStage(Collection<Stage> pipelineStages) {
            this.pipelineStages.addAll(pipelineStages);
            return this;
        }

        public Builder addStage(Stage pipelineStage) {
            this.pipelineStages.add(pipelineStage);
            return this;
        }

        public FlowGraph build() { return new FlowGraph(this); }
    }

    private static Collection<Stage> validatePipelineStages(Collection<Stage> pipelineStages) {
        if (areStagesNamesNotUnique(pipelineStages)) {
            throw new IllegalArgumentException("Stages names are not unique");
        }

        if (areThereOutputStagesNotDefined(pipelineStages)) {
            throw new IllegalArgumentException("Output stages are not fully defined");
        }
        return pipelineStages;
    }
    private static boolean areStagesNamesNotUnique(Collection<Stage> pipelineStages) {
        return pipelineStages.size() != pipelineStages.stream()
                                                      .map(Stage::getStageName)
                                                      .distinct()
                                                      .count();
    }

    private static boolean areThereOutputStagesNotDefined(Collection<Stage> pipelineStages) {
        var stageNames = pipelineStages.stream()
                                       .map(Stage::getStageName)
                                       .collect(Collectors.toUnmodifiableSet());
        return pipelineStages.stream()
                      .flatMap(stage -> stage.getOutputStagesNames().stream())
                      .anyMatch(Predicate.not(stageNames::contains));
    }
}
