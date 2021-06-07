/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.exceptions.ServiceNotFoundException;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

@NonNullByDefault
public class ServiceOperator extends FlowOperator {
  public static final Function<Collection<ReActorRef>, ReActorRef> GET_FIRST_GATE = gates -> gates.iterator().next();
  public static final Function<Collection<ReActorRef>, ReActorRef> GET_RANDOM_GATE = gates -> gates.stream()
                                                                                                   .skip(ThreadLocalRandom.current().nextInt(0, gates.size() - 1))
                                                                                                   .findAny()
                                                                                                   .get();
  private static final CompletionStage<Collection<? extends Serializable>> NO_OUTPUT = CompletableFuture.completedFuture(List.of());
  private final Function<Serializable, Collection<? extends Serializable>> toServiceRequests;
  private final Function<Serializable, Collection<? extends Serializable>> fromServiceResponse;
  private final Class<? extends Serializable> serviceReplyType;
  private final ReActorRef service;
  private final ReActions reActions;
  private long requestCounter;

  private ServiceOperator(Class<? extends Serializable> serviceReplyType,
                          Function<Serializable, Collection<? extends Serializable>> toServiceRequests,
                          Function<Serializable, Collection<? extends Serializable>> fromServiceResponse,
                          ReActorRef service) {
    this.serviceReplyType = Objects.requireNonNull(serviceReplyType,
                                                   "Reply type cannot be null");
    this.toServiceRequests = Objects.requireNonNull(toServiceRequests,
                                                    "Input mapper cannot be null");
    this.fromServiceResponse = Objects.requireNonNull(fromServiceResponse,
                                                      "Output mapper cannot be null");
    this.service = service;
    this.reActions = ReActions.newBuilder()
                              .from(super.getReActions())
                              .reAct(serviceReplyType, this::onReply)
                              .build();
  }

  public static CompletionStage<Try<ReActorRef>>
  of(ReActorSystem localReActorSystem, ReActorConfig operatorCfg,
     ServiceDiscoverySearchFilter serviceSearchFilter,
     Class<? extends Serializable> replyT,
     Function<Serializable, Collection<? extends Serializable>> toServiceRequests,
     Function<Serializable, Collection<? extends Serializable>> fromServiceResponse) {
    return of(localReActorSystem, operatorCfg, serviceSearchFilter, GET_RANDOM_GATE,
              replyT, toServiceRequests, fromServiceResponse);
  }

  public static CompletionStage<Try<ReActorRef>>
  of(ReActorSystem localReActorSystem, ReActorConfig operatorCfg,
     ServiceDiscoverySearchFilter serviceSearchFilter,
     Function<Collection<ReActorRef>, ReActorRef> gateSelector,
     Class<? extends Serializable> serviceReplyType,
     Function<Serializable, Collection<? extends Serializable>> toServiceRequests,
     Function<Serializable, Collection<? extends Serializable>> fromServiceResponse) {
    return localReActorSystem.serviceDiscovery(serviceSearchFilter)
                             .thenApply(tryReply -> tryReply.filter(reply -> reply.getServiceGates().isEmpty(),
                                                                    ServiceNotFoundException::new)
                                                            .map(ServiceDiscoveryReply::getServiceGates)
                                                            .map(gateSelector::apply))
                             .thenApply(service -> service.flatMap(serviceRef -> localReActorSystem.spawn(new ServiceOperator(serviceReplyType,
                                                                                                                              toServiceRequests,
                                                                                                                              fromServiceResponse,
                                                                                                                              serviceRef),
                                                                                                          operatorCfg)));
  }

  @Override
  public ReActions getReActions() { return reActions; }

  @Override
  protected final CompletionStage<Collection<? extends Serializable>>
  onNext(Serializable input, ReActorContext raCtx) {
    var requestsForService = toServiceRequests.apply(input).stream();
    requestsForService.map(request -> service.atell(raCtx.getSelf(), request))
                      .forEach(request -> request.thenAccept(delivery -> delivery.ifError(error -> onFailedDelivery(error, raCtx, input))));
    return NO_OUTPUT;
  }

  private <PayloadT extends Serializable> void onReply(ReActorContext raCtx, PayloadT reply) {
    forwardToNextStages(reply, fromServiceResponse.apply(reply), raCtx, getNextStages());
  }
}
