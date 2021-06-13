/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactors.ReActor;
import io.reacted.flow.operators.ReduceOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

@NonNullByDefault
public class ReduceOperatorConfig extends FlowOperatorConfig<Builder, ReduceOperatorConfig> {
  private final Map<Class<? extends Serializable>, Long> typeToRequiredForMerge;
  private final Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                         Collection<? extends Serializable>> reducer;
  private ReduceOperatorConfig(Builder builder) {
    super(builder);
    this.typeToRequiredForMerge = Objects.requireNonNull(builder.typeToRequiredForMerge,
                                                         "Merge types cannot be null")
                                         .stream()
                                         .collect(Collectors.groupingBy(Function.identity(),
                                                                        Collectors.counting()));
    this.reducer = Objects.requireNonNull(builder.reducer, "Reducer function cannot be null");
  }

  public Map<Class<? extends Serializable>, Long> getTypeToRequiredForMerge() {
    return typeToRequiredForMerge;
  }

  public Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                  Collection<? extends Serializable>> getReducer() {
    return reducer;
  }

  public static Builder newBuilder() { return new Builder(); }
  public static class Builder extends FlowOperatorConfig.Builder<Builder, ReduceOperatorConfig> {
    private Collection<Class<? extends Serializable>> typeToRequiredForMerge;
    private Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                     Collection<? extends Serializable>> reducer;
    private Builder() { super.setRouteeProvider(ReduceOperator::new); }
    public Builder setMergeRequiredTypes(Collection<Class<? extends Serializable>> typeToRequiredForMerge) {
      this.typeToRequiredForMerge = typeToRequiredForMerge;
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
