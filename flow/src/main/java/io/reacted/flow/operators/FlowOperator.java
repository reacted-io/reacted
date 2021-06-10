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
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.GateSelectorPolicies;
import io.reacted.core.utils.ReActedUtils;
import io.reacted.flow.operators.messages.InitOperatorReply;
import io.reacted.flow.operators.messages.InitOperatorRequest;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;

@NonNullByDefault
public abstract class FlowOperator<BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
                                   BuiltT extends FlowOperatorConfig<BuilderT, BuiltT>>
    implements ReActor {
    private final ReActions stageReActions;
    private final BuiltT operatorCfg;
    private final ReActorConfig routeeCfg;
    private Collection<ReActorRef> ifPredicateOutputOperatorsRefs;
    private Collection<ReActorRef> thenElseOutputOperatorsRefs;
    private ScheduledFuture<?> operatorsRefreshTask;

    protected FlowOperator(BuiltT operatorCfg) {
        this.operatorCfg = Objects.requireNonNull(operatorCfg, "Operator Config cannot be null");
        this.routeeCfg = Objects.requireNonNull(operatorCfg.getRouteeConfig());
        this.ifPredicateOutputOperatorsRefs = List.of();
        this.thenElseOutputOperatorsRefs = List.of();
        this.stageReActions = ReActions.newBuilder()
                                       .reAct(InitOperatorRequest.class, (raCtx, payload) -> onOperatorInit(raCtx))
                                       .reAct(ReActorInit.class, this::onInit)
                                       .reAct(ReActorStop.class, this::onStop)
                                       .reAct(ServicesGatesUpdate.class, this::onServiceGatesUpdate)
                                       .reAct(this::onNext)
                                       .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() { return routeeCfg; }

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
        // Constantly refresh the gates. The idea is to automatically discover new available operators
        this.operatorsRefreshTask = raCtx.getReActorSystem()
            .getSystemSchedulingService()
            .scheduleWithFixedDelay(() -> ReActedUtils.ifNotDelivered(raCtx.getSelf()
                                                                           .tell(raCtx.getReActorSystem()
                                                                                      .getSystemSink(),
                                                                                 new InitOperatorRequest()),
                                                                      error -> raCtx.logError("Unable to self init operator",
                                                                                              error)),
                                    0, 15, TimeUnit.SECONDS);

    }

    protected void onServiceGatesUpdate(ReActorContext raCtx, ServicesGatesUpdate newGates) {
        this.ifPredicateOutputOperatorsRefs = newGates.ifPredicateServices;
        this.thenElseOutputOperatorsRefs = newGates.thenElseServices;
        raCtx.getSender().tell(raCtx.getSelf(), new InitOperatorReply(operatorCfg.getReActorName(),
                                                                      this.getClass(), true));
    }
    protected void onStop(ReActorContext raCtx, ReActorStop stop) {
        operatorsRefreshTask.cancel(true);
    }

    private void onOperatorInit(ReActorContext raCtx) {
        var requester = raCtx.getSender();
        var ifServices = ReActedUtils.resolveServices(operatorCfg.getIfPredicateOutputOperators(),
                                                      raCtx.getReActorSystem(),
                                                      GateSelectorPolicies.RANDOM_GATE);
        var thenElseServices = ReActedUtils.resolveServices(operatorCfg.getThenElseOutputOperators(),
                                                            raCtx.getReActorSystem(),
                                                            GateSelectorPolicies.RANDOM_GATE);
        ifServices.thenCombine(thenElseServices, ServicesGatesUpdate::new)
                  .thenApply(servicesGatesUpdate -> servicesGatesUpdate.ifPredicateServices.size() !=
                                                    operatorCfg.getIfPredicateOutputOperators().size() ||
                                                    servicesGatesUpdate.thenElseServices.size() !=
                                                    operatorCfg.getThenElseOutputOperators().size()
                                                    ? requester.tell(new InitOperatorReply(getConfig().getReActorName(),
                                                                                           this.getClass(), false))
                                                    : raCtx.getSelf().tell(requester, servicesGatesUpdate));
    }

    private void onNext(ReActorContext raCtx, Serializable message) {
        onNext(message, raCtx).thenAccept(outMessages -> routeOutputMessageAfterFiltering(outMessages)
                                          .forEach((operators, outputs) -> forwardToNextStages(message,
                                                                                               outputs,
                                                                                               raCtx,
                                                                                               operators)));
    }

    protected Map<Collection<ReActorRef>, ? extends Collection<? extends Serializable>>
    routeOutputMessageAfterFiltering(Collection<? extends Serializable> outputMessages) {
        return outputMessages.stream()
                      .collect(Collectors.groupingBy(message -> operatorCfg.getIfPredicate()
                                                                           .test(message)
                                                                ? ifPredicateOutputOperatorsRefs
                                                                : thenElseOutputOperatorsRefs));
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

    private static class ServicesGatesUpdate implements Serializable {
        private final Collection<ReActorRef> ifPredicateServices;
        private final Collection<ReActorRef> thenElseServices;

        public ServicesGatesUpdate(Collection<ReActorRef> ifPredicateServices,
                                   Collection<ReActorRef> thenElseServices) {
            this.ifPredicateServices = ifPredicateServices;
            this.thenElseServices = thenElseServices;
        }
    }
}
