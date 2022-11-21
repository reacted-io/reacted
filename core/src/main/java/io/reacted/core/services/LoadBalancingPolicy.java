/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface LoadBalancingPolicy {
  @Nullable
  <ServiceConfigBuilderT extends ReActorServiceConfig.Builder<ServiceConfigBuilderT, ServiceConfigT>,
   ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
  ReActorRef selectRoutee(@Nonnull ReActorContext routerCtx,
                          @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                          long msgNum, @Nonnull Serializable message);
}
