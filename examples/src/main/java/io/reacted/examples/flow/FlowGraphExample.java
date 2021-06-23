/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.flow;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.examples.ExampleUtils;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.SourceStream;
import io.reacted.flow.operators.MapOperatorConfig;
import io.reacted.flow.operators.ReduceOperatorConfig;
import io.reacted.patterns.AsyncUtils;
import io.reacted.streams.ReactedSubmissionPublisher;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowGraphExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlowGraphExample.class);

  public static void main(String[] args) throws InterruptedException {
    ReActorSystem flowReActorSystem = ExampleUtils.getDefaultInitedReActorSystem("FlowGraphSystem");
    List<String> inputData = List.of("HAKUNA", "MATATA");
    List<Integer> inputInt = List.of(1, 2);
    var stringPublisher = new ReactedSubmissionPublisher<>(flowReActorSystem, "StringPublisher");
    var integerPublisher = new ReactedSubmissionPublisher<>(flowReActorSystem, "IntPublisher");

    ReActedGraph flowMerge = ReActedGraph.newBuilder()
                                         .setReActorName("FlowMerge")
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("ToLower")
                                                                       .setInputStreams(List.of(inputData.stream(), SourceStream.of(stringPublisher)))
                                                                       .setMappingFunction(input -> List.of(((String)input).toLowerCase()))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .build())
                                                                       .build())
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("Multiplier")
                                                                       .setInputStreams(List.of(inputInt.stream(), SourceStream.of(integerPublisher)))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .build())
                                                                       .setMappingFunction(input -> List.of(((Integer)input) * 123))
                                                                       .build())
                                         .addOperator(ReduceOperatorConfig.newBuilder()
                                                                          .setReActorName("Joiner")
                                                                          .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                              .setServiceName("Printer")
                                                                                                                              .build())
                                                                          .setMergeRequiredTypes(List.of(String.class, Integer.class))
                                                                          .setReducer(inputMap -> List.of(inputMap.values().stream()
                                                                                                                  .flatMap(List::stream)
                                                                                                                  .map(Object::toString)
                                                                                                                  .collect(Collectors.joining(" "))))
                                                                          .build())
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("Printer")
                                                                       .setMappingFunction(input -> { LOGGER.info("Merged: {}",input);
                                                                                                      return List.of(); })
                                                                       .build())
                                         .build();
    LOGGER.info("Launching the system");

    flowMerge.run(flowReActorSystem)
             .peekFailure(error -> LOGGER.error("Failure, shutting down", error))
             .ifError(error -> flowReActorSystem.shutDown());

    var asyncLooperExecutor = Executors.newCachedThreadPool();
    AsyncUtils.asyncForeach(payload -> stringPublisher.backpressurableSubmit(payload)
                                                      .thenAccept(noVal -> {}),
                            List.of("I", "AM", "LOCUTUS").iterator(),
                            error -> LOGGER.error("Error feeding string publisher", error),
                            asyncLooperExecutor);
    AsyncUtils.asyncForeach(payload -> integerPublisher.backpressurableSubmit(payload)
                                                       .thenAccept(noVal -> {}),
                            List.of(1, 2, 3).iterator(),
                            error -> LOGGER.error("Error feeding int publisher", error),
                            asyncLooperExecutor);
    LOGGER.info("Waiting for pipeline to complete");
    TimeUnit.SECONDS.sleep(1);
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
