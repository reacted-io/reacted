/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.UnChecked.TriConsumer;
import io.reacted.patterns.annotations.unstable.Unstable;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NonNullByDefault
@Immutable
@Unstable
public abstract class FlowOperatorConfig<BuilderT extends ReActorServiceConfig.Builder<BuilderT, BuiltT>,
                                         BuiltT extends ReActorServiceConfig<BuilderT, BuiltT>>
    extends ReActorServiceConfig<BuilderT, BuiltT> {
  public static final Duration DEFAULT_OUTPUT_OPERATORS_REFRESH = Duration.ofMinutes(1);
  public static final Predicate<Serializable> NO_FILTERING = element -> true;
  public static final Collection<ServiceDiscoverySearchFilter> NO_OUTPUT = List.of();
  public static final Collection<Stream<? extends Serializable>> NO_INPUT_STREAMS = List.of();
  public static final TriConsumer<ReActorSystem,
                                 ? extends FlowOperatorConfig<? extends ReActorServiceConfig.Builder<?, ?>,
                                                              ? extends ReActorServiceConfig<?, ?>>,
                                 ? super Throwable> DEFAULT_INPUT_STREAM_LOGGING_ERROR_HANDLER =
      (reActorSystem, operatorConfig, throwable) ->
          reActorSystem.logError("Error processing input stream for operator {}",
                                 operatorConfig.getReActorName(), throwable);
  private static final ReActorConfig DEFAULT_OPERATOR_ROUTEE_CONFIG = ReActorConfig.newBuilder()
                                                                                   .setReActorName("ROUTEE")
                                                                                   .build();
  private static final String FLOW_NAME_MISSING = "FLOW NAME MISSING";
  private final TriConsumer<ReActorSystem, BuiltT, ? super Throwable> inputStreamErrorHandler;
  private final Predicate<Serializable> ifPredicate;
  private final Collection<ServiceDiscoverySearchFilter> ifPredicateOutputOperators;
  private final Collection<ServiceDiscoverySearchFilter> thenElseOutputOperators;
  private final Collection<Stream<? extends Serializable>> inputStreams;
  private final ReActorConfig routeeConfig;
  private final String flowName;

  private final Duration outputOperatorsRefreshPeriod;
  protected FlowOperatorConfig(Builder<BuilderT, BuiltT> builder) {
    super(builder);
    this.routeeConfig = Objects.requireNonNull(builder.operatorRouteeCfg,
                                               "Operator routee config cannot be null") == DEFAULT_OPERATOR_ROUTEE_CONFIG
                        ? ReActorConfig.fromConfig(builder.operatorRouteeCfg)
                                       .setDispatcherName(getDispatcherName())
                                       .build()
                        : builder.operatorRouteeCfg;
    this.ifPredicate = Objects.requireNonNull(builder.ifPredicate, "If predicate cannot be null");
    this.ifPredicateOutputOperators = Objects.requireNonNull(builder.ifPredicateOutputOperators,
                                                             "Output filters for if predicate cannot be null");
    this.thenElseOutputOperators = Objects.requireNonNull(builder.thenElseOutputOperators,
                                                          "Output filters if predicate is false cannot be null");
    this.inputStreams = Objects.requireNonNull(builder.inputStreams, "Input Streams cannot be null");
    this.inputStreamErrorHandler = Objects.requireNonNull(builder.inputStreamErrorHandler,
                                                          "Input stream error handler cannot be null");
    this.flowName = Objects.requireNonNull(builder.flowName, "Flow Name cannot be null");
    this.outputOperatorsRefreshPeriod = ObjectUtils.checkNonNullPositiveTimeInterval(builder.outputOperatorsRefreshPeriod);
  }

  public Predicate<Serializable> getIfPredicate() {
    return ifPredicate;
  }

  public Collection<ServiceDiscoverySearchFilter> getIfPredicateOutputOperators() {
    return ifPredicateOutputOperators;
  }

  public Collection<ServiceDiscoverySearchFilter> getThenElseOutputOperators() {
    return thenElseOutputOperators;
  }

  public Collection<Stream<? extends Serializable>> getInputStreams() { return inputStreams; }

  public ReActorConfig getRouteeConfig() { return routeeConfig; }

  public TriConsumer<ReActorSystem, BuiltT, ? super Throwable> getInputStreamErrorHandler() {
    return inputStreamErrorHandler;
  }

  public String getFlowName() { return flowName; }

  public Duration getOutputOperatorsRefreshPeriod() { return outputOperatorsRefreshPeriod; }

  public abstract BuilderT toBuilder();
  public abstract static class Builder<BuilderT extends ReActorServiceConfig.Builder<BuilderT, BuiltT>,
                                       BuiltT extends ReActorServiceConfig<BuilderT, BuiltT>> extends ReActorServiceConfig.Builder<BuilderT, BuiltT> {
    protected Collection<ServiceDiscoverySearchFilter> ifPredicateOutputOperators = NO_OUTPUT;
    protected Collection<ServiceDiscoverySearchFilter> thenElseOutputOperators = NO_OUTPUT;
    protected Predicate<Serializable> ifPredicate = NO_FILTERING;
    protected Collection<Stream<? extends Serializable>> inputStreams = NO_INPUT_STREAMS;
    protected ReActorConfig operatorRouteeCfg = DEFAULT_OPERATOR_ROUTEE_CONFIG;
    protected Duration outputOperatorsRefreshPeriod = DEFAULT_OUTPUT_OPERATORS_REFRESH;
    @SuppressWarnings("unchecked")
    protected TriConsumer<ReActorSystem, BuiltT, ? super Throwable> inputStreamErrorHandler =
        (TriConsumer<ReActorSystem, BuiltT, ? super Throwable>) DEFAULT_INPUT_STREAM_LOGGING_ERROR_HANDLER;
    protected String flowName = FLOW_NAME_MISSING;

    protected Builder() { /* No implementation required */ }

    public final BuilderT setIfOutputPredicate(Predicate<Serializable> ifPredicate) {
      this.ifPredicate = ifPredicate;
      return getThis();
    }
    public final BuilderT setOutputOperators(String ...operatorsNames) {
      return setIfOutputFilter(Arrays.stream(operatorsNames)
                                     .map(operatorName -> BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                           .setServiceName(operatorName)
                                                                                           .build())
                                     .collect(Collectors.toUnmodifiableList()));
    }
    public final BuilderT setIfOutputFilter(ServiceDiscoverySearchFilter ifPredicateOutputOperator) {
      return setIfOutputFilter(List.of(ifPredicateOutputOperator));
    }

    public final BuilderT setIfOutputFilter(Collection<ServiceDiscoverySearchFilter> ifPredicateOutputOperators) {
      this.ifPredicateOutputOperators = ifPredicateOutputOperators;
      return getThis();
    }

    public final BuilderT setThenElseOutputFilter(ServiceDiscoverySearchFilter thenElseOutputOperator) {
      return setThenElseOutputFilter(List.of(thenElseOutputOperator));
    }

    public final BuilderT setThenElseOutputFilter(Collection<ServiceDiscoverySearchFilter> thenElseOutputOperators) {
      this.thenElseOutputOperators = thenElseOutputOperators;
      return getThis();
    }

    public final BuilderT setInputStreams(Collection<Stream<? extends Serializable>> inputStreams) {
      this.inputStreams = inputStreams;
      return getThis();
    }

    public final BuilderT setOperatorRouteeCfg(ReActorConfig operatorRouteeCfg) {
      this.operatorRouteeCfg = operatorRouteeCfg;
      return getThis();
    }

    public final BuilderT
    setInputStreamErrorHandler(TriConsumer<ReActorSystem, BuiltT,
                               ? super Throwable> inputStreamErrorHandler) {
      this.inputStreamErrorHandler = inputStreamErrorHandler;
      return getThis();
    }

    public final BuilderT setFlowName(String flowName) {
      this.flowName = flowName;
      return getThis();
    }

    public final BuilderT setOutputOperatorsRefreshPeriod(Duration outputOperatorsRefreshPeriod) {
      this.outputOperatorsRefreshPeriod = outputOperatorsRefreshPeriod;
      return getThis();
    }
  }
}
