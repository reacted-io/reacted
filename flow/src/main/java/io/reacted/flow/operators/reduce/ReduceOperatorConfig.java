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
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

@NonNullByDefault
public class ReduceOperatorConfig extends FlowOperatorConfig<Builder, ReduceOperatorConfig> {
  private final Map<Class<? extends Serializable>,
                    Function<Serializable, ReduceKey>> keyExtractors;
  private final Function<Map<Class<? extends Serializable>, Serializable>,
                         Collection<? extends Serializable>> reducer;
  private final Builder builder;
  private ReduceOperatorConfig(Builder builder) {
    super(builder);
    this.keyExtractors = Objects.requireNonNull(builder.keyExtractors);
    this.reducer = Objects.requireNonNull(builder.reducer, "Reducer function cannot be null");
    this.builder = builder;
  }

  public Map<Class<? extends Serializable>, Function<Serializable, ReduceKey>>
  getKeyExtractors() { return keyExtractors; }

  public Function<Map<Class<? extends Serializable>, Serializable>,
                  Collection<? extends Serializable>> getReducer() { return reducer; }
  @Override
  public Builder toBuilder() { return builder; }

  public static Builder newBuilder() { return new Builder(); }
  @SuppressWarnings("NotNullFieldNotInitialized")
  public static class Builder extends FlowOperatorConfig.Builder<Builder, ReduceOperatorConfig> {
    private Map<Class<? extends Serializable>,
                Function<Serializable, ReduceKey>> keyExtractors;
    private Function<Map<Class<? extends Serializable>, Serializable>,
                     Collection<? extends Serializable>> reducer;
    private Builder() { super.setRouteeProvider(ReduceOperator::new); }

    public Builder setKeyExtractors(Map<Class<? extends Serializable>,
                                        Function<Serializable, ReduceKey>>
                                        keyExtractors) {
      this.keyExtractors = keyExtractors;
      return this;
    }

    public Builder setReducer(Function<Map<Class<? extends Serializable>, Serializable>,
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
