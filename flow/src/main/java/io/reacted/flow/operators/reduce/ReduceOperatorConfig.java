/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

import io.reacted.flow.operators.reduce.ReduceOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class ReduceOperatorConfig extends ReducingOperatorConfig<Builder, ReduceOperatorConfig> {
  private final Builder builder;
  private ReduceOperatorConfig(Builder builder) {
    super(builder);
    this.builder = builder;
  }
  public static Builder newBuilder() { return new Builder(); }

  @Override
  public Builder toBuilder() { return builder; }

  public static class Builder extends ReducingOperatorConfig.Builder<Builder, ReduceOperatorConfig> {
    private Builder() { super.setRouteeProvider(ReduceOperator::new); }

    @Override
    public ReduceOperatorConfig build() {
      return new ReduceOperatorConfig(this);
    }
  }
}
