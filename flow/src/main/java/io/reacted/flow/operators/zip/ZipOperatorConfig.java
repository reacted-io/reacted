/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.flow.operators.reduce.ReducingOperatorConfig;
import io.reacted.flow.operators.reduce.ReduceKey;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZipOperatorConfig extends ReducingOperatorConfig<Builder, ZipOperatorConfig> {
  private static final ReduceKey NO_KEY = new ZipKey();
  private final Builder builder;
  private ZipOperatorConfig(Builder builder) {
    super(builder);
    this.builder = builder;
  }
  @Override
  public Builder toBuilder() { return builder; }
  public static Builder newBuilder() { return new Builder(); }

  public static class Builder extends ReducingOperatorConfig.Builder<Builder, ZipOperatorConfig> {
    private Builder() { super.setRouteeProvider(ZipOperator::new); }

    @SafeVarargs
    public final Builder setZipTypes(Class<? extends Serializable>... zipTypes) {
      return setZipTypes(Arrays.stream(zipTypes).collect(Collectors.toUnmodifiableList()));
    }
    public final Builder setZipTypes(Collection<Class<? extends Serializable>> zipTypes) {
      var zipRequiredTypes = zipTypes.stream()
                                     .collect(Collectors.collectingAndThen(Collectors.groupingBy(Function.identity(),
                                                                                                 Collectors.counting()),
                                                                           Map::copyOf));
      setZipTypes(zipRequiredTypes);
      return this;
    }

    public final Builder setZipTypes(Map<Class<? extends Serializable>, Long> setZipTypes) {
      setReductionRules(setZipTypes);
      setKeyExtractors(setZipTypes.keySet().stream()
                                  .collect(Collectors.toUnmodifiableMap(Function.identity(),
                                                                        element -> value -> NO_KEY)));
      return this;
    }
    public final Builder setZipper(Function<Map<Class<? extends Serializable>,
                                          List<? extends Serializable>>,
                             Collection<? extends Serializable>> zipper) {
      return setReducer(zipper);
    }

    public final Builder setZippingConsumer(Consumer<Map<Class<? extends Serializable>,
                                                   List<? extends Serializable>>> zippingConsumer) {
      return setReducer(map -> { zippingConsumer.accept(map); return List.of(); });
    }

    @Override
    public ZipOperatorConfig build() {
      return new ZipOperatorConfig(this);
    }
  }

  private static class ZipKey implements ReduceKey {
    @Override
    public int hashCode() { return 0; }

    @Override
    public boolean equals(Object obj) { return obj instanceof ZipKey; }

    @Override
    public int compareTo(ReduceKey o) {
      return 0;
    }
  }
}
