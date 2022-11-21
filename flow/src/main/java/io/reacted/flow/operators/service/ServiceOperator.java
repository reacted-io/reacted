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
import io.reacted.core.utils.ReActedUtils;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.service.ServiceOperatorConfig.Builder;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

@NonNullByDefault
public class ServiceOperator extends FlowOperator<Builder,
                                                  ServiceOperatorConfig> {
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
                                     (raCtx, refreshServiceRequest) -> onRefreshServiceRequest(raCtx))
                              .reAct(RefreshServiceUpdate.class, this::onRefreshServiceUpdate)
                              .reAct(config.getServiceReplyType(), this::onReply)
                              .build();
  }

  @Nonnull
  @Override
  public ReActions getReActions() { return reActions; }

  @Override
  protected final CompletionStage<Collection<? extends Serializable>>
  onNext(Serializable input, ReActorContext raCtx) {

    return AsyncUtils.asyncForeach(request -> service.atell(raCtx.getSelf(), request),
                                   getOperatorCfg().getToServiceRequests().apply(input).iterator(),
                                   error -> onFailedDelivery(error, raCtx, input), executorService)
                     .thenAccept(noVal -> raCtx.getMbox().request(1))
                     .thenApply(noVal -> FlowOperator.NO_OUTPUT);
  }

  @Override
  protected void broadcastOperatorInitializationComplete(ReActorContext raCtx) {
    if (!serviceInitializationMissing) {
      super.broadcastOperatorInitializationComplete(raCtx);
    }
  }
  private void onServiceOperatorInit(ReActorContext raCtx, ReActorInit init) {
    super.onInit(raCtx, init);
    BackpressuringMbox.toBackpressuringMailbox(raCtx.getMbox())
                      .ifPresent(mbox -> mbox.addNonDelayableTypes(Set.of(RefreshServiceRequest.class)));
    this.serviceRefreshTask = raCtx.getReActorSystem()
         .getSystemSchedulingService()
         .scheduleWithFixedDelay(() -> {
                                   if (!raCtx.selfTell(new RefreshServiceRequest()).isSent()) {
                                       raCtx.logError("Unable to request refresh of service operators");
                                   }
                                 },
                                 0, getOperatorCfg().getServiceRefreshPeriod()
                                                    .toNanos(), TimeUnit.NANOSECONDS);
  }

  private void onServiceOperatorStop(ReActorContext raCtx, ReActorStop stop) {
    super.onStop(raCtx, stop);
    if (serviceRefreshTask != null) {
      serviceRefreshTask.cancel(true);
    }
    if (shallStopExecutorService) {
      executorService.shutdownNow();
    }
  }

  private void onRefreshServiceRequest(ReActorContext raCtx) {
    ReActedUtils.resolveServices(List.of(getOperatorCfg().getServiceSearchFilter()),
                                 raCtx.getReActorSystem(),
                                 getOperatorCfg().getGateSelector(),
                                 raCtx.getSelf().getReActorId().toString())
                .thenAccept(selectedService -> {
                  if (!selectedService.isEmpty()) {
                    raCtx.selfTell(new RefreshServiceUpdate(selectedService));
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
  private <PayloadT extends Serializable> void onReply(ReActorContext raCtx, PayloadT reply) {
    propagate(CompletableFuture.supplyAsync(() -> getOperatorCfg().getFromServiceResponse()
                                                                  .apply(reply), executorService),
              reply, raCtx);
  }

  private static class RefreshServiceRequest implements Serializable {
    @Override
    public String toString() {
      return "RefreshServiceRequest{}";
    }
  }

  private record RefreshServiceUpdate(Collection<ReActorRef> serviceGates) implements Serializable {
  }
}
