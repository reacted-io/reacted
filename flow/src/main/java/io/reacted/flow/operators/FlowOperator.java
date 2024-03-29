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
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.services.GateSelectorPolicies;
import io.reacted.core.utils.ReActedUtils;
import io.reacted.flow.operators.messages.OperatorInitComplete;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.annotations.unstable.Unstable;

import javax.annotation.Nonnull;
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

import static io.reacted.core.utils.ReActedUtils.composeDeliveries;
@NonNullByDefault
@Unstable
public abstract class FlowOperator<CfgBuilderT extends FlowOperatorConfig.Builder<CfgBuilderT, CfgT>,
                                   CfgT extends FlowOperatorConfig<CfgBuilderT, CfgT>> implements ReActor {
    public static final Collection<? extends ReActedMessage> NO_OUTPUT = List.of();
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
                                          .reAct(RefreshOperatorRequest.class, (ctx, payload) -> onRefreshOperatorRequest(ctx))
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
    protected abstract CompletionStage<Collection<? extends ReActedMessage>>
    onNext(ReActedMessage input, ReActorContext ctx);
    protected void onLinkError(Throwable error, ReActorContext ctx, ReActedMessage input) {
        ctx.logError("Unable to pass {} to the next stage", input, error);
    }
    @SuppressWarnings("EmptyMethod")
    protected void onInit(ReActorContext ctx, ReActorInit init) {
        BackpressuringMbox.toBackpressuringMailbox(ctx.getMbox())
                          .map(mbox -> mbox.addNonDelayableTypes(Set.of(RefreshOperatorRequest.class,
                                                                        OperatorOutputGatesUpdate.class)))
                          /* If this init is not delayed, an slot of the backpressuring buffer size
                             has been consumed to deliver init itself, so we must make it available
                             otherwise we will permanently leak 1 from buffer size
                          */
                          .filter(mbox -> mbox.isDelayable(ReActorInit.class))
                          .ifPresent(mbox -> mbox.request(1));
        // Constantly refresh the gates. The idea is to automatically discover new available operators
        this.operatorsRefreshTask = ctx.getReActorSystem()
            .getSystemSchedulingService()
            .scheduleWithFixedDelay(() -> {
                if (!ctx.selfPublish(new RefreshOperatorRequest()).isSent()) {
                    ctx.logError("Unable to request refresh of operator outputs");
                }},
                                    0, operatorCfg.getOutputOperatorsRefreshPeriod()
                                                  .toNanos(), TimeUnit.NANOSECONDS);
    }

    protected void onServiceGatesUpdate(ReActorContext ctx, OperatorOutputGatesUpdate newGates) {
        this.ifPredicateOutputOperatorsRefs = newGates.ifPredicateServices;
        this.thenElseOutputOperatorsRefs = newGates.thenElseServices;
        if (isShallAwakeInputStreams()) {
            this.shallAwakeInputStreams = false;
            broadcastOperatorInitializationComplete(ctx);
        }
    }
    protected void broadcastOperatorInitializationComplete(ReActorContext ctx) {
        ctx.getReActorSystem()
             .broadcastToLocalSubscribers(ctx.getSelf(),
                                          new OperatorInitComplete(operatorCfg.getFlowName(),
                                                                   operatorCfg.getReActorName(),
                                                                   ctx.getSelf()
                                                                        .getReActorId()
                                                                        .getReActorName()));
    }

    protected void onStop(ReActorContext ctx, ReActorStop stop) {
        operatorsRefreshTask.cancel(true);
    }

    protected void onRefreshOperatorRequest(ReActorContext ctx) {
        var ifServices = ReActedUtils.resolveServices(operatorCfg.getIfPredicateOutputOperators(),
                                                      ctx.getReActorSystem(),
                                                      GateSelectorPolicies.RANDOM_GATE,
                                                      ctx.getSelf().getReActorId().toString());
        var thenElseServices = ReActedUtils.resolveServices(operatorCfg.getThenElseOutputOperators(),
                                                            ctx.getReActorSystem(),
                                                            GateSelectorPolicies.RANDOM_GATE,
                                                            ctx.getSelf().getReActorId().toString());
        ifServices.thenCombine(thenElseServices, OperatorOutputGatesUpdate::new)
                  .thenAccept(operatorOutputGatesUpdate -> {
                      if (isUpdateMatchingRequest(operatorCfg.getIfPredicateOutputOperators().size(),
                                                  operatorCfg.getThenElseOutputOperators().size(),
                                                  operatorOutputGatesUpdate)) {
                          ctx.selfPublish(operatorOutputGatesUpdate);
                      }
                  });
    }

    private void onNext(ReActorContext ctx, ReActedMessage message) {
        backpressuredPropagation(onNext(message, ctx), message, ctx);
    }

    protected CompletionStage<Void> backpressuredPropagation(CompletionStage<Collection<? extends ReActedMessage>> operatorOutput,
                                                             ReActedMessage inputMessage,
                                                             ReActorContext ctx) {
        return propagate(operatorOutput, inputMessage, ctx)
            .thenAccept(lastDelivery -> ctx.getMbox().request(1));
    }

    protected CompletionStage<DeliveryStatus>
    propagate(CompletionStage<Collection<? extends ReActedMessage>> operatorOutput,
              ReActedMessage inputMessage, ReActorContext ctx) {
        Consumer<Throwable> onDeliveryError = error -> onFailedDelivery(error, ctx, inputMessage);
        return operatorOutput.thenCompose(messages -> routeOutputMessageAfterFiltering(messages).entrySet().stream()
                                                                                                .map(msgToDst -> forwardToOperators(onDeliveryError,
                                                                                                                                    msgToDst.getValue(),
                                                                                                                                    ctx, msgToDst.getKey()))
                                                                                                .reduce((first, second) -> ReActedUtils.composeDeliveries(first, second, onDeliveryError))
                                                                                                .orElse(CompletableFuture.completedStage(DeliveryStatus.DELIVERED)));
    }
    protected Map<Collection<ReActorRef>, ? extends Collection<? extends ReActedMessage>>
    routeOutputMessageAfterFiltering(Collection<? extends ReActedMessage> outputMessages) {
        return outputMessages.stream()
                      .collect(Collectors.groupingBy(message -> operatorCfg.getIfPredicate()
                                                                           .test(message)
                                                                ? ifPredicateOutputOperatorsRefs
                                                                : thenElseOutputOperatorsRefs));
    }
    protected CompletionStage<DeliveryStatus>
    forwardToOperators(Consumer<Throwable> onDeliveryError,
                       Collection<? extends ReActedMessage> messages,
                       ReActorContext ctx, Collection<ReActorRef> nextStages) {
        return messages.stream()
                       .flatMap(output -> nextStages.stream()
                                                    .map(dst -> dst.apublish(ctx.getSelf(), output)))
                       .reduce((first, second) -> composeDeliveries(first, second, onDeliveryError))
                       .orElseGet(() -> CompletableFuture.completedFuture(DeliveryStatus.DELIVERED));
    }
    @SuppressWarnings("SameReturnValue")
    protected  <InputT extends ReActedMessage>
    DeliveryStatus onFailedDelivery(Throwable error, ReActorContext ctx, InputT message) {
        onLinkError(error, ctx, message);
        return DeliveryStatus.NOT_DELIVERED;
    }

    private static boolean isUpdateMatchingRequest(int expectedIfServices,
                                                   int expectedThenElseServices,
                                                   OperatorOutputGatesUpdate update) {
        return update.ifPredicateServices.size() == expectedIfServices &&
               update.thenElseServices.size() == expectedThenElseServices;
    }

    private static class RefreshOperatorRequest implements ReActedMessage {

        @Override
        public String toString() {
            return "RefreshOperatorRequest{}";
        }
    }

    private record OperatorOutputGatesUpdate(Collection<ReActorRef> ifPredicateServices,
                                             Collection<ReActorRef> thenElseServices) implements ReActedMessage {
        @Override
            public String toString() {
                return "OperatorOutputGatesUpdate{" +
                        "ifPredicateServices=" + ifPredicateServices +
                        ", thenElseServices=" + thenElseServices +
                        '}';
            }
        }
}
