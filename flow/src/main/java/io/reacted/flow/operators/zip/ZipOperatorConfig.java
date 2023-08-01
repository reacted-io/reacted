/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.zip;

import io.reacted.core.serialization.ReActedMessage;
import io.reacted.flow.operators.reduce.ReducingOperatorConfig;
import io.reacted.flow.operators.zip.ZipOperatorConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZipOperatorConfig extends ReducingOperatorConfig<Builder, ZipOperatorConfig> {
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
    public final Builder setZipTypes(Class<? extends ReActedMessage>... zipTypes) {
      return setZipTypes(Arrays.stream(zipTypes).collect(Collectors.toUnmodifiableList()));
    }
    public final Builder setZipTypes(Collection<Class<? extends ReActedMessage>> zipTypes) {
      var zipRequiredTypes = zipTypes.stream()
                                     .collect(Collectors.collectingAndThen(Collectors.groupingBy(Function.identity(),
                                                                                                 Collectors.counting()),
                                                                           Map::copyOf));
      setZipTypes(zipRequiredTypes);
      return this;
    }

    public final Builder setZipTypes(Map<Class<? extends ReActedMessage>, Long> setZipTypes) {
      setReductionRules(setZipTypes);
      return this;
    }
    public final Builder setZipper(Function<Map<Class<? extends ReActedMessage>, List<? extends ReActedMessage>>,
                                            Collection<? extends ReActedMessage>> zipper) {
      return setReducer(zipper);
    }

    public final Builder setZippingConsumer(Consumer<Map<Class<? extends ReActedMessage>,
                                            List<? extends ReActedMessage>>> zippingConsumer) {
      return setReducer(map -> { zippingConsumer.accept(map); return List.of(); });
    }

    @Override
    public ZipOperatorConfig build() {
      return new ZipOperatorConfig(this);
    }
  }
}
