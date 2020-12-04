/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;

@NonNullByDefault
public abstract class PipelineStage implements ReActiveEntity {
    private final Collection<ReActorRef> nextStages;
    private final ReActions stageReActions;
    protected PipelineStage(Collection<ReActorRef> nextStages) {
        this.nextStages = Objects.requireNonNull(nextStages, "Next stages cannot be null");
        this.stageReActions = ReActions.newBuilder()
                                       .reAct(ReActorInit.class, this::onInit)
                                       .reAct(ReActorStop.class, this::onStop)
                                       .reAct(this::onNext).build();
    }
    @Override
    public final ReActions getReActions() { return stageReActions; }
    protected abstract Collection<? extends Serializable> onNext(Serializable input, ReActorContext raCtx);
    protected void onLinkError(Throwable error, ReActorContext raCtx, Serializable input) {
        raCtx.logError("Unable to pass {} to the next stage", error);
    }

    protected void onInit(ReActorContext raCtx, ReActorInit init) { }
    protected void onStop(ReActorContext raCtx, ReActorStop stop) { }

    protected static <InputT extends Serializable> InputT toExpectedType(Serializable message) {
        return (InputT)message;
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

    private <InputT extends Serializable>
    DeliveryStatus onFailedDelivery(Throwable error, ReActorContext raCtx, InputT message) {
        onLinkError(error, raCtx, message);
        return DeliveryStatus.NOT_DELIVERED;
    }
}
