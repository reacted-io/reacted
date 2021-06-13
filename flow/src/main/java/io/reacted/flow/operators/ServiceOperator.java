/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.flow.operators.messages.RefreshOperatorRequest;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

@NonNullByDefault
public class ServiceOperator extends FlowOperator<ServiceOperatorConfig.Builder,
                                                  ServiceOperatorConfig> {
  private final Function<Collection<ReActorRef>, Optional<ReActorRef>> gateSelector;
  private final Function<Serializable, Collection<? extends Serializable>> toServiceRequests;
  private final Function<Serializable, Collection<? extends Serializable>> fromServiceResponse;
  private final ServiceDiscoverySearchFilter serviceSearchFilter;
  private final ReActions reActions;
  private final ExecutorService executorService;
  private final boolean shallStopExecutorService;
  private ReActorRef service;

  protected ServiceOperator(ServiceOperatorConfig config) {
    super(config);
    this.gateSelector = config.getGateSelector();
    this.toServiceRequests = config.getToServiceRequests();
    this.fromServiceResponse = config.getFromServiceResponse();
    this.serviceSearchFilter = config.getServiceSearchFilter();
    this.executorService = config.getExecutorService()
                                 .orElseGet(Executors::newSingleThreadExecutor);
    this.shallStopExecutorService = config.getExecutorService().isEmpty();
    this.reActions = ReActions.newBuilder()
                              .from(super.getReActions())
                              .reAct(ReActorInit.class, this::onServiceOperatorInit)
                              .reAct(ReActorStop.class, this::onServiceOperatorStop)
                              .reAct(RefreshOperatorRequest.class,
                                     this::onRefreshServiceOperatorRequest)
                              .reAct(config.getServiceReplyType(), this::onReply)
                              .build();
  }

  @Override
  public ReActions getReActions() { return reActions; }

  @Override
  protected final CompletionStage<Collection<? extends Serializable>>
  onNext(Serializable input, ReActorContext raCtx) {

    return AsyncUtils.asyncForeach(request -> service.atell(raCtx.getSelf(), request),
                                   toServiceRequests.apply(input).iterator(),
                                   error -> onFailedDelivery(error, raCtx, input), executorService)
                     .thenAccept(noVal -> raCtx.getMbox().request(1))
                     .thenApply(noVal -> FlowOperator.NO_OUTPUT);
  }

  private void onServiceOperatorInit(ReActorContext raCtx, ReActorInit init) {
    super.onInit(raCtx, init);
  }

  private void onServiceOperatorStop(ReActorContext raCtx, ReActorStop stop) {
    super.onStop(raCtx, stop);
    if (shallStopExecutorService) {
      executorService.shutdownNow();
    }
  }

  private void onRefreshServiceOperatorRequest(ReActorContext raCtx,
                                               RefreshOperatorRequest request) {
    super.onRefreshOperatorRequest(raCtx);

  }

  private <PayloadT extends Serializable> void onReply(ReActorContext raCtx, PayloadT reply) {
    propagate(CompletableFuture.supplyAsync(() -> fromServiceResponse.apply(reply), executorService),
              reply, raCtx);
  }
}
