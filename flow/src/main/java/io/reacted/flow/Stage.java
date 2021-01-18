/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

@NonNullByDefault
public class Stage {
    private final String stageName;
    private final Function<ReActorSystem, CompletionStage<ReActorRef>> operatorProvider;
    private final Collection<String> outputLevelNames;
    private final Collection<Stream<? extends Serializable>> sourceStreams;
    private Stage(Builder builder) {
        this.stageName = Objects.requireNonNull(builder.stageName, "Stage name cannot be null");
        this.operatorProvider = Objects.requireNonNull(builder.operatorProvider,
                                                       "Stage operator cannot be null");
        this.outputLevelNames = Objects.requireNonNull(builder.outputStageNames,
                                                       "Stage output stages canno be null");
        this.sourceStreams = Objects.requireNonNull(builder.sourceStreams,
                                                    "Source Streams cannot be null");
        outputLevelNames.forEach(stageName -> Objects.requireNonNull(stageName,
                                                                     "Output stage name cannot be null"));
        sourceStreams.forEach(sourceStream -> Objects.requireNonNull(sourceStream,
                                                                     "Source Stream cannot be null"));
    }

    public String getStageName() { return stageName; }

    public Function<ReActorSystem, CompletionStage<ReActorRef>> getOperatorProvider() {
        return operatorProvider;
    }

    public Collection<Stream<? extends Serializable>> getSourceStreams() { return sourceStreams; }

    public Collection<String> getOutputStagesNames() { return outputLevelNames; }

    public static Builder newBuilder() { return new Builder(); }

    @SuppressWarnings("NotNullFieldNotInitialized")
    public static class Builder {
        private String stageName;
        private Function<ReActorSystem, CompletionStage<ReActorRef>> operatorProvider;
        private Collection<String> outputStageNames;

        private Collection<Stream<? extends Serializable>> sourceStreams;
        private Builder() {
            this.outputStageNames = List.of();
            this.sourceStreams = List.of();
        }

        public Builder setStageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public Builder setOperatorProvider(Function<ReActorSystem, CompletionStage<ReActorRef>>
                                           operatorProvider) {
            this.operatorProvider = operatorProvider;
            return this;
        }

        public Builder setOutputStagesNames(Collection<String> outputStagesNames) {
            this.outputStageNames = outputStagesNames;
            return this;
        }

        public Builder setOutputStagesNames(String outputStagesName) {
            this.outputStageNames = List.of(outputStagesName);
            return this;
        }

        public Builder setInputStreams(Collection<Stream<? extends Serializable>> sourceStreams) {
            this.sourceStreams = sourceStreams;
            return this;
        }

        public Builder setInputStream(Stream<? extends Serializable> sourceStream) {
            this.sourceStreams = List.of(sourceStream);
            return this;
        }

        public Stage build() { return new Stage(this); }
    }
}
