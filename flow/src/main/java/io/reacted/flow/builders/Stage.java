/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.builders;

import io.reacted.flow.operators.PipelineStage;
import io.reacted.patterns.NonNullByDefault;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

@NonNullByDefault
public class Stage {
    private final String stageName;
    private final PipelineStage operator;
    private final Collection<String> outputLevelNames;
    private final Collection<SourceStream> sourceStreams;
    private Stage(Builder builder) {
        this.stageName = Objects.requireNonNull(builder.stageName, "Stage name cannot be null");
        this.operator = Objects.requireNonNull(builder.operator, "Stage operator cannot be null");
        this.outputLevelNames = Objects.requireNonNull(builder.outputStageNames, "Stage output stages canno be null");
        this.sourceStreams = Objects.requireNonNull(builder.sourceStreams, "Source Streams cannot be null");
        outputLevelNames.forEach(stageName -> Objects.requireNonNull(stageName, "Output stage name cannot be null"));
        sourceStreams.forEach(sourceStream -> Objects.requireNonNull(sourceStream, "Source Stream cannot be null"));
    }

    public String getStageName() { return stageName; }

    public PipelineStage getOperator() { return operator; }

    public Collection<String> getOutputStagesNames() { return outputLevelNames; }

    public static class Builder {
        private String stageName;
        private PipelineStage operator;
        private Collection<String> outputStageNames;

        private Collection<SourceStream> sourceStreams;
        private Builder() {
            this.outputStageNames = List.of();
            this.sourceStreams = List.of();
        }

        public Builder setStageName(String stageName) {
            this.stageName = stageName;
            return this;
        }

        public Builder setOperator(PipelineStage operator) {
            this.operator = operator;
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

        public Builder setSourceStreams(Collection<SourceStream> sourceStreams) {
            this.sourceStreams = sourceStreams;
            return this;
        }

        public Builder setInputStreams(SourceStream sourceStream) {
            this.sourceStreams = List.of(sourceStream);
            return this;
        }

        public Stage build() { return new Stage(this); }
    }
}
