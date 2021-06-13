/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.reactors.ReActor;
import io.reacted.flow.operators.FlowOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.CheckedFunction;
import io.reacted.patterns.UnChecked.CheckedSupplier;
import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

@NonNullByDefault
public class MapOperatorConfig extends FlowOperatorConfig<MapOperatorConfig.Builder,
                                                          MapOperatorConfig> {
  private final Function<Object, Collection<? extends Serializable>> mappingFunction;
  private MapOperatorConfig(Builder builder) {
    super(builder);
    this.mappingFunction = Objects.requireNonNull(builder.mappingFunction,
                                                  "Mapping function cannot be null");
  }

  public Function<Object, Collection<? extends Serializable>> getMappingFunction() {
    return mappingFunction;
  }

  public static Builder newBuilder() { return new Builder(); }
  public static class Builder extends FlowOperatorConfig.Builder<Builder, MapOperatorConfig> {
    private Function<Object, Collection<? extends Serializable>> mappingFunction;
    private Builder() { super.setRouteeProvider(MapOperator::new); }
    public Builder setMappingFunction(Function<Object, Collection<? extends Serializable>> mappingFunction) {
      this.mappingFunction = mappingFunction;
      return this;
    }

    @Override
    public MapOperatorConfig build() {
      return new MapOperatorConfig(this);
    }
  }
}
