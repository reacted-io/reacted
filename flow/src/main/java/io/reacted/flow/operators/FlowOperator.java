/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ServiceConfig;
import io.reacted.core.exceptions.ServiceNotFoundException;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.flow.operators.messages.InitOperator;
import io.reacted.flow.operators.messages.SetNextStagesRequest;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;

@NonNullByDefault
public abstract class FlowOperator
    implements ReActiveEntity {
    private final ReActions stageReActions;
    private final FlowOperatorConfig<?, ?> operatorConfig;
    private Collection<ReActorRef> ifPredicateOutputOperators;
    private Collection<ReActorRef> thenElseOutputOperators;

    protected FlowOperator(FlowOperatorConfig<?, ?> genericOperatorConfig) {
        this.operatorConfig = Objects.requireNonNull(genericOperatorConfig,
                                                     "Operator config cannot be null");
        this.ifPredicateOutputOperators = List.of();
        this.thenElseOutputOperators = List.of();
        this.stageReActions = ReActions.newBuilder()
                                       .reAct(InitOperator.class, (raCtx, payload) -> onOperatorInit(raCtx))
                                       .reAct(ReActorInit.class, this::onInit)
                                       .reAct(ReActorStop.class, this::onStop)
                                       .reAct(this::onNext)
                                       .build();
    }
    @Nonnull
    @Override
    public ReActions getReActions() { return stageReActions; }

    protected static Try<ReActorRef> of(ReActorSystem localReActorSystem, ServiceConfig config) {
        return localReActorSystem.spawnService(config);
    }
    protected abstract CompletionStage<Collection<? extends Serializable>>
    onNext(Serializable input, ReActorContext raCtx);
    protected void onLinkError(Throwable error, ReActorContext raCtx, Serializable input) {
        raCtx.logError("Unable to pass {} to the next stage", input, error);
    }
    @SuppressWarnings("EmptyMethod")
    protected void onInit(ReActorContext raCtx, ReActorInit init) {
        /* No default implementation required */
    }
    @SuppressWarnings("EmptyMethod")
    protected void onStop(ReActorContext raCtx, ReActorStop stop) {
        /* No default implementation required */
    }

    private void onOperatorInit(ReActorContext raCtx) {
        operatorConfig.getIfPredicateOutputOperators().stream()
                      .map(filter -> raCtx.getReActorSystem().serviceDiscovery(filter))

    }

    private void onNext(ReActorContext raCtx, Serializable message) {
        onNext(message, raCtx).thenAccept(outMessages -> routeOutputMessageAfterFiltering(outMessages)
                                          .forEach((operators, outputs) -> forwardToNextStages(message,
                                                                                               operators,
                                                                                               raCtx,
                                                                                               (Collection<ReActorRef>) outputs)));
    }

    protected Map<Collection<ReActorRef>, ? extends Collection<? extends Serializable>>
    routeOutputMessageAfterFiltering(Collection<? extends Serializable> outputMessages) {
        return outputMessages.stream()
                      .collect(Collectors.groupingBy(message -> operatorConfig.getIfPredicate()
                                                                              .test(message)
                                                                                ? ifPredicateOutputOperators
                                                                                : thenElseOutputOperators));
    }
    protected void forwardToNextStages(Serializable input,
                                       Collection<? extends Serializable> outMsgs,
                                       ReActorContext raCtx,
                                       Collection<ReActorRef> nextStages) {
        var fanOut = outMsgs.stream()
                            .flatMap(output -> nextStages.stream()
                                                         .map(dst -> dst.atell(raCtx.getSelf(),
                                                                               output)))
                            .collect(Collectors.toList());
        fanOut.stream()
              .reduce((first, second) -> composeDeliveries(first, second,
                                                           error -> onFailedDelivery(error, raCtx,
                                                                                     input)))
              .orElseGet(() -> CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.DELIVERED)))
              .thenAccept(lastDelivery -> raCtx.getMbox().request(1));
    }
    @SuppressWarnings("SameReturnValue")
    protected  <InputT extends Serializable>
    DeliveryStatus onFailedDelivery(Throwable error, ReActorContext raCtx, InputT message) {
        onLinkError(error, raCtx, message);
        return DeliveryStatus.NOT_DELIVERED;
    }
}
