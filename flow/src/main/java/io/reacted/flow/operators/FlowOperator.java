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
import io.reacted.core.mailboxes.BackpressuringMbox;
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
import io.reacted.flow.operators.messages.OperatorInitComplete;
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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;
@NonNullByDefault
public abstract class FlowOperator<CfgBuilderT extends FlowOperatorConfig.Builder<CfgBuilderT, CfgT>,
                                   CfgT extends FlowOperatorConfig<CfgBuilderT, CfgT>>
    implements ReActor {
    public static final Collection<? extends Serializable> NO_OUTPUT = List.of();
    private final ReActions operatorReactions;
    private final CfgT operatorCfg;
    private final ReActorConfig routeeCfg;
    private Collection<ReActorRef> ifPredicateOutputOperatorsRefs;
    private Collection<ReActorRef> thenElseOutputOperatorsRefs;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private ScheduledFuture<?> operatorsRefreshTask;
    private boolean shallAwakeInputStreams = true;

    protected FlowOperator(CfgT operatorCfg) {
        this.operatorCfg = Objects.requireNonNull(operatorCfg, "Operator Config cannot be null");
        this.routeeCfg = Objects.requireNonNull(operatorCfg.getRouteeConfig());
        this.ifPredicateOutputOperatorsRefs = List.of();
        this.thenElseOutputOperatorsRefs = List.of();
        this.operatorReactions = ReActions.newBuilder()
                                          .reAct(RefreshOperatorRequest.class, (raCtx, payload) -> onRefreshOperatorRequest(raCtx))
                                          .reAct(ReActorInit.class, this::onInit)
                                          .reAct(ReActorStop.class, this::onStop)
                                          .reAct(OperatorOutputGatesUpdate.class, this::onServiceGatesUpdate)
                                          .reAct(this::onNext)
                                          .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() { return routeeCfg; }

    @Nonnull
    @Override
    public ReActions getReActions() { return operatorReactions; }

    protected boolean isShallAwakeInputStreams() { return shallAwakeInputStreams; }

    protected CfgT getOperatorCfg() { return operatorCfg; }

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
        BackpressuringMbox.toBackpressuringMailbox(raCtx.getMbox())
                          .map(mbox -> mbox.addNonDelayedMessageTypes(Set.of(RefreshOperatorRequest.class,
                                                                             OperatorOutputGatesUpdate.class)))
                          /* If this init is not delayed, an slot of the backpressuring buffer size
                             has been consumed to deliver init itself, so we must make it available
                             otherwise we will permanently leak 1 from buffer size
                          */
                          .filter(mbox -> !mbox.getNotDelayedMessageTypes().contains(ReActorInit.class))
                          .ifPresent(mbox -> mbox.request(1));
        // Constantly refresh the gates. The idea is to automatically discover new available operators
        this.operatorsRefreshTask = raCtx.getReActorSystem()
            .getSystemSchedulingService()
            .scheduleWithFixedDelay(() -> ReActedUtils.ifNotDelivered(raCtx.selfTell(new RefreshOperatorRequest()),
                                                                      error -> raCtx.logError("Unable to request refresh of operator outputs",
                                                                                              error)),
                                    0, operatorCfg.getOutputOperatorsRefreshPeriod()
                                                  .toNanos(), TimeUnit.NANOSECONDS);
    }

    protected void onServiceGatesUpdate(ReActorContext raCtx, OperatorOutputGatesUpdate newGates) {
        this.ifPredicateOutputOperatorsRefs = newGates.ifPredicateServices;
        this.thenElseOutputOperatorsRefs = newGates.thenElseServices;
        if (isShallAwakeInputStreams()) {
            this.shallAwakeInputStreams = false;
            broadcastOperatorInitializationComplete(raCtx);
        }
    }
    protected void broadcastOperatorInitializationComplete(ReActorContext raCtx) {
        raCtx.getReActorSystem()
             .broadcastToLocalSubscribers(raCtx.getSelf(),
                                          new OperatorInitComplete(operatorCfg.getFlowName(),
                                                                   operatorCfg.getReActorName(),
                                                                   raCtx.getSelf()
                                                                        .getReActorId()
                                                                        .getReActorName()));
    }

    protected void onStop(ReActorContext raCtx, ReActorStop stop) {
        operatorsRefreshTask.cancel(true);
    }

    protected void onRefreshOperatorRequest(ReActorContext raCtx) {
        var ifServices = ReActedUtils.resolveServices(operatorCfg.getIfPredicateOutputOperators(),
                                                      raCtx.getReActorSystem(),
                                                      GateSelectorPolicies.RANDOM_GATE,
                                                      raCtx.getSelf().getReActorId().toString());
        var thenElseServices = ReActedUtils.resolveServices(operatorCfg.getThenElseOutputOperators(),
                                                            raCtx.getReActorSystem(),
                                                            GateSelectorPolicies.RANDOM_GATE,
                                                            raCtx.getSelf().getReActorId().toString());
        ifServices.thenCombine(thenElseServices, OperatorOutputGatesUpdate::new)
                  .thenAccept(operatorOutputGatesUpdate -> {
                      if (isUpdateMatchingRequest(operatorCfg.getIfPredicateOutputOperators().size(),
                                                  operatorCfg.getThenElseOutputOperators().size(),
                                                  operatorOutputGatesUpdate)) {
                          raCtx.selfTell(operatorOutputGatesUpdate);
                      }
                  });
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

    private static boolean isUpdateMatchingRequest(int expectedIfServices,
                                                   int expectedThenElseServices,
                                                   OperatorOutputGatesUpdate update) {
        return update.ifPredicateServices.size() == expectedIfServices &&
               update.thenElseServices.size() == expectedThenElseServices;
    }

    private static class RefreshOperatorRequest implements Serializable {

        @Override
        public String toString() {
            return "RefreshOperatorRequest{}";
        }
    }

    private static class OperatorOutputGatesUpdate implements Serializable {
        private final Collection<ReActorRef> ifPredicateServices;
        private final Collection<ReActorRef> thenElseServices;

        public OperatorOutputGatesUpdate(Collection<ReActorRef> ifPredicateServices,
                                         Collection<ReActorRef> thenElseServices) {
            this.ifPredicateServices = ifPredicateServices;
            this.thenElseServices = thenElseServices;
        }
        @Override
        public String toString() {
            return "OperatorOutputGatesUpdate{" +
                   "ifPredicateServices=" + ifPredicateServices +
                   ", thenElseServices=" + thenElseServices +
                   '}';
        }
    }
}
