/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.reduce.ReduceOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ReduceOperatorConfig extends FlowOperatorConfig<Builder, ReduceOperatorConfig> {
  private final Map<Class<? extends Serializable>,
                    Function<Serializable, ReduceKey>> keyExtractors;
  private final Function<Map<Class<? extends Serializable>,
                             List<? extends Serializable>>,
                         Collection<? extends Serializable>> reducer;
  private final Map<Class<? extends Serializable>, Long> reductionRules;
  private final Builder builder;
  private ReduceOperatorConfig(Builder builder) {
    super(builder);
    this.keyExtractors = Objects.requireNonNull(builder.keyExtractors,
                                                "Key extractors cannot be null");
    this.reducer = Objects.requireNonNull(builder.reducer, "Reducer function cannot be null");
    this.reductionRules = Objects.requireNonNull(builder.reductionRules,
                                                 "Reduction rules cannot be null");
    if (!Objects.equals(keyExtractors.keySet(), reductionRules.keySet())) {
      throw new IllegalArgumentException("Key Extractor and Reduction Rules must contain the same types");
    }
    if (reductionRules.values().stream()
                      .anyMatch(requiredForType -> requiredForType <= 0)) {
      throw new IllegalArgumentException("Reduction rules require a positive number of elements per type");
    }
    this.builder = builder;
  }

  public Map<Class<? extends Serializable>, Function<Serializable, ReduceKey>>
  getKeyExtractors() { return keyExtractors; }

  public Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                  Collection<? extends Serializable>> getReducer() { return reducer; }

  public Map<Class<? extends Serializable>, Long> getReductionRules() { return reductionRules; }
  @Override
  public Builder toBuilder() { return builder; }

  public static Builder newBuilder() { return new Builder(); }
  @SuppressWarnings("NotNullFieldNotInitialized")
  public static class Builder extends FlowOperatorConfig.Builder<Builder, ReduceOperatorConfig> {
    private Map<Class<? extends Serializable>,
                Function<Serializable, ReduceKey>> keyExtractors;
    private Function<Map<Class<? extends Serializable>,
                         List<? extends Serializable>>,
                     Collection<? extends Serializable>> reducer;
    private Map<Class<? extends Serializable>, Long> reductionRules;
    private Builder() { super.setRouteeProvider(ReduceOperator::new); }

    public Builder setReductionRules(Class<? extends Serializable> ...requiredTypes) {
      return setReductionRules(Arrays.stream(requiredTypes)
                                     .collect(Collectors.toUnmodifiableList()));
    }

    public Builder setReductionRules(List<Class<? extends Serializable>> reductionRules) {
      var rules = reductionRules.stream()
                                .collect(Collectors.collectingAndThen(Collectors.groupingBy(Function.identity(),
                                                                                            Collectors.counting()),
                                                                      Map::copyOf));
      return setReductionRules(rules);
    }

    public Builder setReductionRules(Map<Class<? extends Serializable>, Long> typeToRequiredInstances) {
      this.reductionRules = typeToRequiredInstances;
      return this;
    }

    public Builder setKeyExtractors(Map<Class<? extends Serializable>,
                                        Function<Serializable, ReduceKey>>
                                        keyExtractors) {
      this.keyExtractors = keyExtractors;
      return this;
    }

    public Builder setReducer(Function<Map<Class<? extends Serializable>,
                                           List<? extends Serializable>>,
                                       Collection<? extends Serializable>> reducer) {
      this.reducer = reducer;
      return this;
    }

    @Override
    public ReduceOperatorConfig build() {
      return new ReduceOperatorConfig(this);
    }
  }
}
