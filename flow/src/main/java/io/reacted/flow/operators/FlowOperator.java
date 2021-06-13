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
import io.reacted.flow.operators.messages.RefreshOperatorReply;
import io.reacted.flow.operators.messages.RefreshOperatorRequest;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;

@NonNullByDefault
public abstract class FlowOperator<BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
                                   BuiltT extends FlowOperatorConfig<BuilderT, BuiltT>>
    implements ReActor {
    public static final Collection<? extends Serializable> NO_OUTPUT = List.of();
    private final ReActions operatorReactions;
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
        this.operatorReactions = ReActions.newBuilder()
                                          .reAct(RefreshOperatorRequest.class, (raCtx, payload) -> onRefreshOperatorRequest(raCtx))
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
    public ReActions getReActions() { return operatorReactions; }

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
                                                                                 new RefreshOperatorRequest()),
                                                                      error -> raCtx.logError("Unable to self init operator",
                                                                                              error)),
                                    0, 15, TimeUnit.SECONDS);
    }

    protected void onServiceGatesUpdate(ReActorContext raCtx, ServicesGatesUpdate newGates) {
        this.ifPredicateOutputOperatorsRefs = newGates.ifPredicateServices;
        this.thenElseOutputOperatorsRefs = newGates.thenElseServices;
        raCtx.getSender().tell(raCtx.getSelf(), new RefreshOperatorReply(operatorCfg.getReActorName(),
                                                                         this.getClass(), true));
    }
    protected void onStop(ReActorContext raCtx, ReActorStop stop) {
        operatorsRefreshTask.cancel(true);
    }

    protected void onRefreshOperatorRequest(ReActorContext raCtx) {
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
                                                    ? requester.tell(new RefreshOperatorReply(getConfig().getReActorName(),
                                                                                              this.getClass(), false))
                                                    : raCtx.getSelf().tell(requester, servicesGatesUpdate));
    }

    private void onNext(ReActorContext raCtx, Serializable message) {
        backpressuredPropagation(onNext(message, raCtx), message, raCtx);
    }

    protected CompletionStage<Void> backpressuredPropagation(CompletionStage<Collection<? extends Serializable>> operatorOutput,
                                                             Serializable inputMessage,
                                                             ReActorContext raCtx) {

        return propagate(operatorOutput, inputMessage, raCtx)
            .thenAccept(lastDelivery -> raCtx.getMbox().request(1));
    }

    protected CompletionStage<Try<DeliveryStatus>>
    propagate(CompletionStage<Collection<? extends Serializable>> operatorOutput,
              Serializable inputMessage, ReActorContext raCtx) {
        Consumer<Throwable> onDeliveryError = error -> onFailedDelivery(error, raCtx, inputMessage);
        return operatorOutput.thenCompose(messages -> routeOutputMessageAfterFiltering(messages).entrySet().stream()
                                                                                                .map(msgToDst -> forwardToOperators(onDeliveryError,
                                                                                                                                    msgToDst.getValue(),
                                                                                                                                    raCtx, msgToDst.getKey()))
                                                                                                .reduce((first, second) -> ReActedUtils.composeDeliveries(first, second, onDeliveryError))
                                                                                                .orElse(CompletableFuture.completedStage(Try.ofSuccess(DeliveryStatus.DELIVERED))));
    }
    protected Map<Collection<ReActorRef>, ? extends Collection<? extends Serializable>>
    routeOutputMessageAfterFiltering(Collection<? extends Serializable> outputMessages) {
        return outputMessages.stream()
                      .collect(Collectors.groupingBy(message -> operatorCfg.getIfPredicate()
                                                                           .test(message)
                                                                ? ifPredicateOutputOperatorsRefs
                                                                : thenElseOutputOperatorsRefs));
    }
    protected CompletionStage<Try<DeliveryStatus>>
    forwardToOperators(Consumer<Throwable> onDeliveryError,
                       Collection<? extends Serializable> messages,
                       ReActorContext raCtx, Collection<ReActorRef> nextStages) {
        return messages.stream()
                       .flatMap(output -> nextStages.stream()
                                                    .map(dst -> dst.atell(raCtx.getSelf(), output)))
                       .reduce((first, second) -> composeDeliveries(first, second, onDeliveryError))
                       .orElseGet(() -> CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.DELIVERED)));
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
