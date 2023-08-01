/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.flow;

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.services.LoadBalancingPolicies;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.operators.map.MapOperatorConfig;
import io.reacted.flow.operators.reduce.ReduceOperatorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ReducingExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReducingExample.class);
  public static void main(String[] args) throws InterruptedException {
    var inputData = List.of(new ReActedMessage.IntMessage(1),
                            new ReActedMessage.IntMessage(2),
                            new ReActedMessage.IntMessage(3),
                            new ReActedMessage.IntMessage(4));
    var graphDispatcher = "FlowProcessor";
    var system = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                      .setReactorSystemName("ReducingSystem")
                                                      .addDispatcherConfig(
                                                          DispatcherConfig.newBuilder()
                                                                          .setDispatcherName(graphDispatcher)
                                                                          .setDispatcherThreadsNum(2)
                                                                          .build())
                                                      .build()).initReActorSystem();
    @SuppressWarnings("unchecked")
    var graph = ReActedGraph.newBuilder()
                            .setDispatcherName(graphDispatcher)
                            .setReActorName("GraphController")
                            .addOperator(MapOperatorConfig.newBuilder()
                                                          .setReActorName("Mapper")
                                                          .setRouteesNum(2)
                                                          .setInputStreams(List.of(inputData.stream()))
                                                          .setLoadBalancingPolicy(LoadBalancingPolicies.partitionBy(message -> ((ReActedMessage.IntMessage)message).getPayload() % 2))
                                                          .setMapper(number -> List.of(ReActedMessage.of(String.valueOf((int)number))))
                                                          .setOutputOperators("Reducer")
                                                          .build())
                            .addOperator(ReduceOperatorConfig.newBuilder()
                                                             .setReActorName("Reducer")
                                                             .setReductionRules(Map.of(ReActedMessage.StringMessage.class, (long )inputData.size()))
                                                             .setReducer(payloads -> List.of(new ReActedMessage.StringMessage(String.join(",", ((List<ReActedMessage.StringMessage>)payloads.get(ReActedMessage.StringMessage.class))
                                                                     .stream()
                                                                     .map(ReActedMessage.StringMessage::toString)
                                                                     .toList()))))
                                                             .setOutputOperators("Printer")
                                                             .build())
                            .addOperator(MapOperatorConfig.newBuilder()
                                                          .setReActorName("Printer")
                                                          .setConsumer(string -> LOGGER.info((String) string))
                                                          .build())
                            .build();

    var graphInit = graph.run(system).toCompletableFuture().join();

    if (graphInit.isFailure()) {
      LOGGER.error("Unable to init graph", graphInit.getCause());
    } else {
      TimeUnit.SECONDS.sleep(1);
    }
    system.shutDown();
  }
}
