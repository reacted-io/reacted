/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.exceptions.ServiceNotFoundException;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.flow.operators.messages.SetNextStagesRequest;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ObjectUtils;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;

@NonNullByDefault
public abstract class PipelineStage implements ReActiveEntity {
    private Set<ReActorRef> nextStages;
    private final ReActions stageReActions;
    protected PipelineStage() {
        this.nextStages = Set.of();
        this.stageReActions = ReActions.newBuilder()
                                       .reAct(SetNextStagesRequest.class, this::setNextStages)
                                       .reAct(ReActorInit.class, this::onInit)
                                       .reAct(ReActorStop.class, this::onStop)
                                       .reAct(this::onNext)
                                       .build();
    }

    public CompletionStage<Try<ReActorRef>> of(ReActorSystem localReActorSystem,
                                               ServiceDiscoverySearchFilter serviceSearchFilter) {
        return localReActorSystem.serviceDiscovery(serviceSearchFilter)
                                 .thenApply(tryReply -> tryReply.filter(reply -> reply.getServiceGates()
                                                                                      .isEmpty(),
                                                                        ServiceNotFoundException::new)
                                                                .map(reply -> reply.getServiceGates()
                                                                                   .iterator()
                                                                                   .next()));
    }
    @Override
    public final ReActions getReActions() { return stageReActions; }

    public void addNextStage(ReActorRef nextStage) {
        this.nextStages = Stream.concat(Stream.of(nextStage), nextStages.stream())
                                .collect(Collectors.toUnmodifiableSet());
    }
    public void setNextStages(ReActorContext raCtx, SetNextStagesRequest nextStagesRequest) {
        this.nextStages = ObjectUtils.defaultIfNull(nextStagesRequest.getNextStages(), Set.of());
    }

    protected abstract Collection<? extends Serializable> onNext(Serializable input,
                                                                 ReActorContext raCtx);
    protected void onLinkError(Throwable error, ReActorContext raCtx, Serializable input) {
        raCtx.logError("Unable to pass {} to the next stage", input, error);
    }
    private void onInit(ReActorContext raCtx, ReActorInit init) {
        /* No default implementation required */
    }
    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        /* No default implementation required */
    }
    private void onNext(ReActorContext raCtx, Serializable message) {
        var fanOut = onNext(message, raCtx).stream()
                .flatMap(output -> nextStages.stream()
                                             .map(dst -> dst.atell(raCtx.getSelf(), output)))
                  .collect(Collectors.toList());
        var lastDelivery = fanOut.stream()
              .reduce((first, second) -> composeDeliveries(first, second,
                                                           error -> onFailedDelivery(error, raCtx,
                                                                                     message)))
              .orElseGet(() -> CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.DELIVERED)));
        lastDelivery.thenAccept(lastOutcome -> raCtx.getMbox().request(1));
    }

    @SuppressWarnings("SameReturnValue")
    private <InputT extends Serializable>
    DeliveryStatus onFailedDelivery(Throwable error, ReActorContext raCtx, InputT message) {
        onLinkError(error, raCtx, message);
        return DeliveryStatus.NOT_DELIVERED;
    }
}
