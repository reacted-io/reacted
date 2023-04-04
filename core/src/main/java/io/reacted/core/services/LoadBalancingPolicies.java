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
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.function.ToIntFunction;

@Immutable
@NonNullByDefault
public final class LoadBalancingPolicies {
  public static final LoadBalancingPolicy ROUND_ROBIN = new LoadBalancingPolicy() {
    @Nullable
    @Override
    public <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
            ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
    ReActorRef selectRoutee(@Nonnull ReActorContext routerCtx,
                            @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                            long msgNum, @Nonnull ReActedMessage message) {
      List<ReActorRef> routees = thisService.getRouteesMap();
      if (routees.size() == 0) {
        return null;
      }
      int routeeIdx = (int) ((msgNum % Integer.MAX_VALUE) % routees.size());
      return routeeIdx < routees.size()
             ? routees.get(routeeIdx)
             : null;
    }
  };

  public static final LoadBalancingPolicy LOWEST_LOAD = new LoadBalancingPolicy() {
    @Nullable
    @Override
    public
    <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
     ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
    ReActorRef selectRoutee(@Nonnull ReActorContext routerCtx,
                                      @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                                      long msgNum, @Nonnull ReActedMessage message) {
      ReActorRef minLoadRoutee = null;
      long minLoad = Long.MAX_VALUE;
      for (ReActorRef routee : routerCtx.getChildren()) {
        ReActorContext ctx = routerCtx.getReActorSystem().getReActorCtx(routee.getReActorId());
        if (ctx != null && ctx.getMbox().getMsgNum() < minLoad) {
          minLoadRoutee = routee;
          minLoad = ctx.getMbox().getMsgNum();
        }
      }
      return minLoadRoutee;
    }
  };

  private LoadBalancingPolicies() { /* No implementation required */ }
  public static LoadBalancingPolicy
  partitionBy(ToIntFunction<ReActedMessage> partitioner) {
    return new LoadBalancingPolicy() {
      @Nullable
      @Override
      public <ServiceConfigBuilderT extends Builder<ServiceConfigBuilderT, ServiceConfigT>,
              ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
      ReActorRef selectRoutee(@Nonnull ReActorContext routerCtx,
                              @Nonnull Service<ServiceConfigBuilderT, ServiceConfigT> thisService,
                              long msgNum, @Nonnull ReActedMessage message) {
        ReActorRef routee = null;
        try {
          int partitionKey = Math.abs(partitioner.applyAsInt(message));
          routee = ROUND_ROBIN.selectRoutee(routerCtx, thisService, partitionKey, message);
        } catch (Exception anyException) {
          routerCtx.logError("Error partitioning message {}", message, anyException);
        }
        return routee;
      }
    };
  }
}
