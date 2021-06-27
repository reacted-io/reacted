/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZipOperatorConfig extends FlowOperatorConfig<Builder, ZipOperatorConfig> {
  private final Map<Class<? extends Serializable>, Long> zipperRequiredTypes;
  private final Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                         Collection<? extends Serializable>> zipper;
  private final Builder builder;
  private ZipOperatorConfig(Builder builder) {
    super(builder);
    this.zipperRequiredTypes = Objects.requireNonNull(builder.zipperRequiredTypes,
                                                      "Zip types cannot be null")
                                      .stream()
                                      .collect(Collectors.groupingBy(Function.identity(),
                                                                     Collectors.counting()));
    this.zipper = Objects.requireNonNull(builder.zipper, "Zipper function cannot be null");
    this.builder = builder;
  }

  public Map<Class<? extends Serializable>, Long> getZipperRequiredTypes() {
    return zipperRequiredTypes;
  }

  public Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                  Collection<? extends Serializable>> getZipper() {
    return zipper;
  }

  @Override
  public Builder toBuilder() { return builder; }

  public static Builder newBuilder() { return new Builder(); }
  @SuppressWarnings("NotNullFieldNotInitialized")
  public static class Builder extends FlowOperatorConfig.Builder<Builder, ZipOperatorConfig> {
    private Collection<Class<? extends Serializable>> zipperRequiredTypes;
    private Function<Map<Class<? extends Serializable>, List<? extends Serializable>>,
                     Collection<? extends Serializable>> zipper;
    private Builder() { super.setRouteeProvider(ZipOperator::new); }
    public Builder setZipRequiredTypes(Collection<Class<? extends Serializable>> zipperRequiredTypes) {
      this.zipperRequiredTypes = zipperRequiredTypes;
      return this;
    }

    public Builder setZipper(Function<Map<Class<? extends Serializable>,
                                       List<? extends Serializable>>,
                                       Collection<? extends Serializable>> zipper) {
      this.zipper = zipper;
      return this;
    }

    @Override
    public ZipOperatorConfig build() {
      return new ZipOperatorConfig(this);
    }
  }
}
