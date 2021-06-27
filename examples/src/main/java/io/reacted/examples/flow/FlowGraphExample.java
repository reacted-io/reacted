/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.flow;

import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.examples.ExampleUtils;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.SourceStream;
import io.reacted.flow.operators.map.MapOperatorConfig;
import io.reacted.flow.operators.zip.ZipOperatorConfig;
import io.reacted.patterns.AsyncUtils;
import io.reacted.streams.ReactedSubmissionPublisher;
import io.reacted.streams.ReactedSubmissionPublisher.ReActedSubscriptionConfig;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowGraphExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlowGraphExample.class);

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    ReActorSystem flowReActorSystem = ExampleUtils.getDefaultInitedReActorSystem("FlowGraphSystem");
    List<String> inputData = List.of("HAKUNA", "MATATA");
    List<Integer> inputInt = List.of(1, 2);
    var stringPublisher = new ReactedSubmissionPublisher<>(flowReActorSystem, "StringPublisher",
                                                           10);
    var integerPublisher = new ReactedSubmissionPublisher<>(flowReActorSystem, "IntPublisher",
                                                            10);

    ReActedGraph flowMerge = ReActedGraph.newBuilder()
                                         .setReActorName("FlowMerge")
                                         .setDispatcherName("FlowDispatcher")
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("ToLower")
                                                                       .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                                    .setRequestOnStartup(1)
                                                                                                                    .setRealMailboxOwner(ctx)
                                                                                                                    .setBufferSize(1)
                                                                                                                    .setBackpressureTimeout(BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT)
                                                                                                                    .build())
                                                                       .setInputStreams(List.of(inputData.stream(), SourceStream.of(stringPublisher,
                                                                                                                                    ReActedSubscriptionConfig.<Serializable>newBuilder()
                                                                                                                                                             .setBufferSize(1)
                                                                                                                                                             .setBackpressureTimeout(ReactedSubmissionPublisher.RELIABLE_SUBSCRIPTION)
                                                                                                                                                             .setSubscriberName("ToLowerSubscription")
                                                                                                                                                             .build())))
                                                                       .setMapper(input -> List.of(((String)input).toLowerCase()))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .build())
                                                                       .build())
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("Multiplier")
                                                                       .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                                                    .setRequestOnStartup(1)
                                                                                                                    .setRealMailboxOwner(ctx)
                                                                                                                    .setBufferSize(1)
                                                                                                                    .setBackpressureTimeout(BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT)
                                                                                                                    .build())
                                                                       .setInputStreams(List.of(inputInt.stream(), SourceStream.of(integerPublisher,
                                                                                                                                   ReActedSubscriptionConfig.<Serializable>newBuilder()
                                                                                                                                                            .setBufferSize(1)
                                                                                                                                                            .setBackpressureTimeout(ReactedSubmissionPublisher.RELIABLE_SUBSCRIPTION)
                                                                                                                                                            .setSubscriberName("MultiplierSubscription")
                                                                                                                                                            .build())))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .build())
                                                                       .setMapper(input -> List.of(((Integer)input) * 123))
                                                                       .build())
                                         .addOperator(ZipOperatorConfig.newBuilder()
                                                                       .setReActorName("Joiner")
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                              .setServiceName("Printer")
                                                                                                                              .build())
                                                                       .setZipRequiredTypes(List.of(String.class, Integer.class))
                                                                       .setZipper(inputMap -> List.of(inputMap.values().stream()
                                                                                                              .flatMap(List::stream)
                                                                                                              .map(Object::toString)
                                                                                                              .collect(Collectors.joining(" "))))
                                                                       .build())
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("Printer")
                                                                       .setMapper(input -> { LOGGER.info("Merged: {}", input);
                                                                                                      return List.of(); })
                                                                       .build())
                                         .build();
    LOGGER.info("Launching the system");

    flowMerge.run(flowReActorSystem)
             .toCompletableFuture()
             .join()
             .peekFailure(error -> LOGGER.error("Failure, shutting down", error))
             .ifError(error -> flowReActorSystem.shutDown());

    var asyncLooperExecutor = Executors.newCachedThreadPool();
    AsyncUtils.asyncForeach(stringPublisher::backpressurableSubmit,
                            IntStream.range(1, 1_001)
                                     .mapToObj(num -> num + "")
                                     .iterator(),
                            error -> LOGGER.error("Error feeding string publisher", error),
                            asyncLooperExecutor);
    AsyncUtils.asyncForeach(integerPublisher::backpressurableSubmit,
                            IntStream.range(1, 1_001).iterator(),
                            error -> LOGGER.error("Error feeding int publisher", error),
                            asyncLooperExecutor);
    LOGGER.info("Waiting for pipeline to complete");
    TimeUnit.SECONDS.sleep(10);
    LOGGER.info("Shutting down publishers");
    stringPublisher.close();
    integerPublisher.close();
    TimeUnit.SECONDS.sleep(1);
    asyncLooperExecutor.shutdownNow();
    LOGGER.info("Stopping graph");
    flowMerge.stop(flowReActorSystem)
             .map(CompletionStage::toCompletableFuture)
             .ifPresent(CompletableFuture::join);
    LOGGER.info("Shutting down reactor system");
    flowReActorSystem.shutDown();
  }
}
