/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.flow.operators.FlowOperatorConfig.Builder;
import io.reacted.flow.operators.messages.OperatorInitComplete;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked.TriConsumer;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@SuppressWarnings("unchecked")
@NonNullByDefault
class GraphController implements ReActiveEntity {
  private final Map<String,
                    ? extends FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?,?>,
                                                 ? extends FlowOperatorConfig<?, ?>>>
      operatorsCfgsByName;
  private final ReActions reActions;
  private final Map<String, ReActorRef> operatorNameToOperator;
  private final Map<String, Integer> operatorToInitedRoutees;
  private final List<ExecutorService> inputStreamProcessors;
  private final String flowName;
  private boolean inputStreamsHaveBeenInited = false;
  GraphController(String flowName,
                  Collection<? extends FlowOperatorConfig<? extends Builder<?,?>,
                             ? extends FlowOperatorConfig<?, ?>>> operatorsCfgs) {
    this.operatorsCfgsByName = ObjectUtils.requiredCondition(Objects.requireNonNull(operatorsCfgs),
                                                             ops -> !ops.isEmpty(),
                                                             () -> new IllegalArgumentException("Operators cannot be empty"))
                                          .stream()
                                          .collect(Collectors.toUnmodifiableMap(FlowOperatorConfig::getReActorName,
                                                                                Function.identity()));
    this.operatorNameToOperator = new ConcurrentHashMap<>(operatorsCfgs.size(), 0.5f);
    this.operatorToInitedRoutees = new HashMap<>();
    this.reActions = ReActions.newBuilder()
                              .reAct(ReActorInit.class, (raCtx, init) -> onInit(raCtx))
                              .reAct(InitInputStreams.class, (raCtx, initStreams) -> onInitInputStreams(raCtx))
                              .reAct(ReActorStop.class, (raCtx, stop) -> onStop(raCtx))
                              .reAct(OperatorInitComplete.class, this::onOperatorInitComplete)
                              .build();
    this.inputStreamProcessors = new LinkedList<>();
    this.flowName = flowName;
  }

  @Nonnull
  @Override
  public ReActions getReActions() { return reActions; }

  Map<String, ReActorRef> getOperatorsByName() { return operatorNameToOperator; }

  private void onOperatorInitComplete(ReActorContext raCtx,
                                      OperatorInitComplete operatorInitComplete) {
    if (!Objects.equals(flowName, operatorInitComplete.getFlowName())) {
      return; // Not Interesting, it's for another flow
    }
    operatorToInitedRoutees.compute(operatorInitComplete.getOperatorName(),
                                   (operator, inited) -> inited == null ? 1 : 1 + inited);
    if (operatorsCfgsByName.entrySet().stream()
                           .allMatch(entry -> operatorToInitedRoutees.getOrDefault(entry.getKey(), 0) >=
                                              entry.getValue().getRouteesNum()) &&
        !inputStreamsHaveBeenInited) {
      this.inputStreamsHaveBeenInited = true;
      raCtx.selfTell(new InitInputStreams());
    }
  }
  private void onInit(ReActorContext raCtx) {
    BackpressuringMbox.toBackpressuringMailbox(raCtx.getMbox())
                      .ifPresent(mbox -> mbox.addNonDelayedMessageTypes(OperatorInitComplete.class));
    for(var operatorCfg : operatorsCfgsByName.entrySet()) {
      operatorNameToOperator.put(operatorCfg.getKey(),
                                 spawnOperator(raCtx.getReActorSystem(),operatorCfg.getValue(),
                                               raCtx.getSelf()).orElseSneakyThrow());
    }
  }
  private void onInitInputStreams(ReActorContext raCtx) {
    for(var operatorCfg : operatorsCfgsByName.entrySet()) {
      for(var inputStream : operatorCfg.getValue().getInputStreams()) {
        spawnNewStreamConsumer(operatorNameToOperator.get(operatorCfg.getKey()),
                               inputStream, raCtx.getReActorSystem(),
                               raCtx.getSelf().getReActorId().getReActorName(),
                               operatorCfg.getValue());
      }
    }
  }
  private void onStop(ReActorContext raCtx) {
    inputStreamProcessors.forEach(ExecutorService::shutdownNow);
  }
  private void spawnNewStreamConsumer(ReActorRef operator,
                                      Stream<? extends Serializable> inputStream,
                                      ReActorSystem localReActorSystem, String flowName,
                              FlowOperatorConfig<?, ?> operatorCfg) {
    ExecutorService streamConsumerExecutor = spawnNewInputStreamExecutor(localReActorSystem,
                                                                         flowName,
                                                                         operatorCfg.getReActorName());
    inputStreamProcessors.add(streamConsumerExecutor);
    var errorHandler = (TriConsumer<ReActorSystem, Object, ? super Throwable>) operatorCfg.getInputStreamErrorHandler();
    AsyncUtils.asyncForeach(operator::atell, inputStream.iterator(),
                            error -> errorHandler.accept(localReActorSystem, operatorCfg,
                                                         error),
                            streamConsumerExecutor)
              .thenAccept(finished -> streamConsumerExecutor.shutdownNow());
  }

  private static <CfgBuilderT extends FlowOperatorConfig.Builder<CfgBuilderT, CfgT>,
      CfgT extends FlowOperatorConfig<CfgBuilderT, CfgT>>
  Try<ReActorRef> spawnOperator(ReActorSystem localReActorSystem,
                                ReActorServiceConfig<? extends ReActorServiceConfig.Builder<?, ?>,
                                    ? extends ReActorServiceConfig<?,?>> operatorConfig,
                                ReActorRef operatorFather) {
    return Try.of(() -> localReActorSystem.spawnService((CfgT)operatorConfig, operatorFather))
              .flatMap(Try::identity);
  }
  private static ExecutorService spawnNewInputStreamExecutor(ReActorSystem localReActorSystem,
                                                             String flowName, String stageName) {
    var inputStreamThreadFactory = new ThreadFactoryBuilder();
    inputStreamThreadFactory.setNameFormat(String.format("InputStreamExecutor-Flow[%s]-Stage[%s]-",
                                                         flowName, stageName))
                            .setUncaughtExceptionHandler((thread, error) -> localReActorSystem.logError("Uncaught exception in {}",
                                                                                                        thread.getName(), error));
    return Executors.newSingleThreadExecutor(inputStreamThreadFactory.build());
  }

  private final static class InitInputStreams implements Serializable {
    @Override
    public String toString() {
      return "InitInputStreams{}";
    }
  }
}
