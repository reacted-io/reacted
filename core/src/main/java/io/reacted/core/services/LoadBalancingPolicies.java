/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.config.reactors.ReActorServiceConfig.Builder;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
@NonNullByDefault
public final class LoadBalancingPolicies {
  public static final LoadBalancingPolicy ROUND_ROBIN = new LoadBalancingPolicy() {
    @Nonnull
    @Override
    public <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
            ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
    Optional<ReActorRef> selectRoutee(@Nonnull ReActorContext routerCtx,
                                      @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                                      long msgNum, @Nonnull Serializable message) {
      List<ReActorRef> routees = thisService.getRouteesMap();
      int routeeIdx = (int) ((msgNum % Integer.MAX_VALUE) % routees.size());
      return Try.of(() -> routees.get(routeeIdx))
                .toOptional();

    }
  };

  public static final LoadBalancingPolicy LOWEST_LOAD = new LoadBalancingPolicy() {
    @Nonnull
    @Override
    public
    <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
     ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
    Optional<ReActorRef> selectRoutee(@Nonnull ReActorContext routerCtx,
                                      @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                                      long msgNum, @Nonnull Serializable message) {
      return routerCtx.getChildren().stream()
                      .map(ReActorRef::getReActorId)
                      .map(routerCtx.getReActorSystem()::getReActor)
                      .flatMap(Optional::stream)
                      .min(Comparator.comparingLong(reActorCtx -> reActorCtx.getMbox().getMsgNum()))
                      .map(ReActorContext::getSelf);
    }
  };

  private LoadBalancingPolicies() { /* No implementation required */ }
  @Nonnull
  public static LoadBalancingPolicy partitionBy(ToIntFunction<Serializable> partitioner) {
    return new LoadBalancingPolicy() {
      @Nonnull
      @Override
      public <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
              ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
      Optional<ReActorRef> selectRoutee(@Nonnull ReActorContext routerCtx,
                                        @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                                        long msgNum, @Nonnull Serializable message) {
        return Try.of(() -> Math.abs(partitioner.applyAsInt(message)))
                  .peekFailure(error -> routerCtx.logError("Error partitioning message {}",
                                                           message, error))
                  .map(key -> ROUND_ROBIN.selectRoutee(routerCtx, thisService, key, message))
                  .orElse(Optional.empty());
      }
    };
  }
}
