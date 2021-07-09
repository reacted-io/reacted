/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.annotations.unstable.Unstable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
@Unstable
public abstract class ReducingOperatorConfig<OperatorConfigBuilderT extends FlowOperatorConfig.Builder<OperatorConfigBuilderT,
                                                                                                              OperatorConfigT>,
                                                    OperatorConfigT extends FlowOperatorConfig<OperatorConfigBuilderT, OperatorConfigT>>
    extends FlowOperatorConfig<OperatorConfigBuilderT, OperatorConfigT> {
  private final Map<Class<? extends Serializable>,
                    Function<Serializable, ReduceKey>> keyExtractors;
  private final Function<Map<Class<? extends Serializable>,
                             List<? extends Serializable>>,
                         Collection<? extends Serializable>> reducer;
  private final Map<Class<? extends Serializable>, Long> reductionRules;
  protected ReducingOperatorConfig(Builder<OperatorConfigBuilderT, OperatorConfigT> builder) {
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
  }

  public Map<Class<? extends Serializable>, Function<Serializable, ReduceKey>>
  getKeyExtractors() { return keyExtractors; }

  public Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                  Collection<? extends Serializable>> getReducer() { return reducer; }

  public Map<Class<? extends Serializable>, Long> getReductionRules() { return reductionRules; }
  @SuppressWarnings("NotNullFieldNotInitialized")
  protected abstract static class Builder<BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
                                          BuiltT extends FlowOperatorConfig<BuilderT, BuiltT>>
      extends FlowOperatorConfig.Builder<BuilderT, BuiltT> {
    private Map<Class<? extends Serializable>,
                Function<Serializable, ReduceKey>> keyExtractors;
    private Function<Map<Class<? extends Serializable>,
                         List<? extends Serializable>>,
                     Collection<? extends Serializable>> reducer;
    private Map<Class<? extends Serializable>, Long> reductionRules;

    protected Builder() { }

    @SafeVarargs
    public final BuilderT setReductionRules(Class<? extends Serializable> ...requiredTypes) {
      return setReductionRules(Arrays.stream(requiredTypes)
                                     .collect(Collectors.toUnmodifiableList()));
    }

    public final BuilderT setReductionRules(List<Class<? extends Serializable>> reductionRules) {
      var rules = reductionRules.stream()
                                .collect(Collectors.collectingAndThen(Collectors.groupingBy(Function.identity(),
                                                                                            Collectors.counting()),
                                                                      Map::copyOf));
      return setReductionRules(rules);
    }

    public final BuilderT setReductionRules(Map<Class<? extends Serializable>, Long> typeToRequiredInstances) {
      this.reductionRules = typeToRequiredInstances;
      return getThis();
    }

    public final BuilderT setKeyExtractors(Map<Class<? extends Serializable>,
                                        Function<Serializable, ReduceKey>>
                                        keyExtractors) {
      this.keyExtractors = keyExtractors;
      return getThis();
    }

    public final BuilderT setReducer(Function<Map<Class<? extends Serializable>,
                                        List<? extends Serializable>>,
                               Collection<? extends Serializable>> reducer) {
      this.reducer = reducer;
      return getThis();
    }
  }
}
