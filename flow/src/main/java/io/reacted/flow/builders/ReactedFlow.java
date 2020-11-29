/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.builders;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ReactedFlow {
    private final Collection<Stage> pipelineStages;
    private ReactedFlow(Builder builder) {
        this.pipelineStages = validatePipelineStages(Objects.requireNonNull(builder.pipelineStages));
    }

    public Collection<Stage> getPipelineStages() { return pipelineStages; }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder {
        private Collection<Stage> pipelineStages;
        private Builder() { this.pipelineStages = new HashSet<>(); }

        public Builder addStage(Collection<Stage> pipelineStages) {
            this.pipelineStages.addAll(pipelineStages);
            return this;
        }

        public Builder addStage(Stage pipelineStage) {
            this.pipelineStages.add(pipelineStage);
            return this;
        }

        public ReactedFlow build() { return new ReactedFlow(this); }
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
