/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.service;

import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.utils.ReActedUtils;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.service.ServiceOperatorConfig.Builder;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public class ServiceOperator extends FlowOperator<Builder, ServiceOperatorConfig> {
  private final ReActions reActions;
  private final ExecutorService executorService;
  private final boolean shallStopExecutorService;
  @Nullable
  private ScheduledFuture<?> serviceRefreshTask;
  @SuppressWarnings("NotNullFieldNotInitialized")
  private ReActorRef service;
  private boolean serviceInitializationMissing;

  protected ServiceOperator(ServiceOperatorConfig config) {
    super(config);
    this.serviceInitializationMissing = true;
    this.executorService = config.getExecutorService()
                                 .orElseGet(Executors::newSingleThreadExecutor);
    this.shallStopExecutorService = config.getExecutorService().isEmpty();
    this.reActions = ReActions.newBuilder()
                              .from(super.getReActions())
                              .reAct(ReActorInit.class, this::onServiceOperatorInit)
                              .reAct(ReActorStop.class, this::onServiceOperatorStop)
                              .reAct(RefreshServiceRequest.class,
                                     (ctx, refreshServiceRequest) -> onRefreshServiceRequest(ctx))
                              .reAct(RefreshServiceUpdate.class, this::onRefreshServiceUpdate)
                              .reAct(config.getServiceReplyType(), this::onReply)
                              .build();
  }

  @Nonnull
  @Override
  public ReActions getReActions() { return reActions; }

  @Override
  protected final CompletionStage<Collection<? extends ReActedMessage>>
  onNext(ReActedMessage input, ReActorContext ctx) {

    return AsyncUtils.asyncForeach(request -> service.apublish(ctx.getSelf(), request),
                                   getOperatorCfg().getToServiceRequests().apply(input).iterator(),
                                   error -> onFailedDelivery(error, ctx, input), executorService)
                     .thenAccept(noVal -> ctx.getMbox().request(1))
                     .thenApply(noVal -> FlowOperator.NO_OUTPUT);
  }

  @Override
  protected void broadcastOperatorInitializationComplete(ReActorContext ctx) {
    if (!serviceInitializationMissing) {
      super.broadcastOperatorInitializationComplete(ctx);
    }
  }
  private void onServiceOperatorInit(ReActorContext ctx, ReActorInit init) {
    super.onInit(ctx, init);
    BackpressuringMbox.toBackpressuringMailbox(ctx.getMbox())
                      .ifPresent(mbox -> mbox.addNonDelayableTypes(Set.of(RefreshServiceRequest.class)));
    this.serviceRefreshTask = ctx.getReActorSystem()
         .getSystemSchedulingService()
         .scheduleWithFixedDelay(() -> {
                                   if (!ctx.selfPublish(new RefreshServiceRequest()).isSent()) {
                                       ctx.logError("Unable to request refresh of service operators");
                                   }
                                 },
                                 0, getOperatorCfg().getServiceRefreshPeriod()
                                                    .toNanos(), TimeUnit.NANOSECONDS);
  }

  private void onServiceOperatorStop(ReActorContext ctx, ReActorStop stop) {
    super.onStop(ctx, stop);
    if (serviceRefreshTask != null) {
      serviceRefreshTask.cancel(true);
    }
    if (shallStopExecutorService) {
      executorService.shutdownNow();
    }
  }

  private void onRefreshServiceRequest(ReActorContext ctx) {
    ReActedUtils.resolveServices(List.of(getOperatorCfg().getServiceSearchFilter()),
                                 ctx.getReActorSystem(),
                                 getOperatorCfg().getGateSelector(),
                                 ctx.getSelf().getReActorId().toString())
                .thenAccept(selectedService -> {
                  if (!selectedService.isEmpty()) {
                    ctx.selfPublish(new RefreshServiceUpdate(selectedService));
                  }
                });

  }
  private void onRefreshServiceUpdate(ReActorContext reActorContext,
                                      RefreshServiceUpdate refreshServiceUpdate) {
    this.service = refreshServiceUpdate.serviceGates.iterator().next();

    if (serviceInitializationMissing) {
      this.serviceInitializationMissing = false;
      if (isShallAwakeInputStreams()) {
        super.broadcastOperatorInitializationComplete(reActorContext);
      }
    }
  }
  private <PayloadT extends ReActedMessage> void onReply(ReActorContext ctx, PayloadT reply) {
    propagate(CompletableFuture.supplyAsync(() -> getOperatorCfg().getFromServiceResponse()
                                                                  .apply(reply), executorService),
              reply, ctx);
  }

  private static class RefreshServiceRequest implements ReActedMessage {
    @Override
    public String toString() {
      return "RefreshServiceRequest{}";
    }
  }

  private record RefreshServiceUpdate(Collection<ReActorRef> serviceGates) implements ReActedMessage { }
}
