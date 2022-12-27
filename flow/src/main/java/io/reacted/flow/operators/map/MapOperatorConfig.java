/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.map;

import io.reacted.core.serialization.ReActedMessage;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.map.MapOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@NonNullByDefault
public class MapOperatorConfig extends FlowOperatorConfig<Builder,
                                                          MapOperatorConfig> {
  private final Function<Object, Collection<? extends ReActedMessage>> mapper;
  private final Builder builder;
  private MapOperatorConfig(Builder builder) {
    super(builder);
    this.mapper = Objects.requireNonNull(builder.mapper,
                                         "Mapping function cannot be null");
    this.builder = builder;
  }

  public Function<Object, Collection<? extends ReActedMessage>> getMapper() {
    return mapper;
  }

  @Override
  public Builder toBuilder() { return builder; }
  public static Builder newBuilder() { return new Builder(); }
  public static class Builder extends FlowOperatorConfig.Builder<Builder, MapOperatorConfig> {
    @SuppressWarnings("NotNullFieldNotInitialized")
    private Function<Object, Collection<? extends ReActedMessage>> mapper;
    private Builder() {
      super.setRouteeProvider(MapOperator::new);
    }
    public final Builder setMapper(Function<Object, Collection<? extends ReActedMessage>> mapper) {
      this.mapper = mapper;
      return this;
    }

    public final Builder setConsumer(Consumer<Object> consumer) {
      this.mapper = input -> { consumer.accept(input); return List.of(); };
      return this;
    }

    @Override
    public MapOperatorConfig build() {
      return new MapOperatorConfig(this);
    }
  }
}
