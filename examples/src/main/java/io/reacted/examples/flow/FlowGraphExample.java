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
import io.reacted.flow.operators.MapOperatorConfig;
import io.reacted.flow.operators.ReduceOperatorConfig;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FlowGraphExample {

  public static void main(String[] args) throws InterruptedException {
    ReActorSystem flowReActorSystem = ExampleUtils.getDefaultInitedReActorSystem("FlowGraphSystem");
    List<String> inputData = List.of("Hakuna", "Matata");
    List<Integer> inputInt = List.of(1, 2);
    ReActedGraph flowMerge = ReActedGraph.newBuilder()
                                         .setFlowName("FlowMerge")
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("ToLower")
                                                                       .setInputStreams(List.of(inputData.stream()))
                                                                       .setMappingFunction(input -> List.of(((String)input).toLowerCase()))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .setDiscoveryRequestId("ToLower-IfOut-Joiner")
                                                                                                                           .build())
                                                                       .build())
                                         .addOperator(MapOperatorConfig.newBuilder()
                                                                       .setReActorName("Multiplier")
                                                                       .setInputStreams(List.of(inputInt.stream()))
                                                                       .setIfOutputFilter(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                                                                           .setServiceName("Joiner")
                                                                                                                           .setDiscoveryRequestId("Multiplier-IfOut-Joiner")
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
                                                                       .setMappingFunction(input -> { System.out.println(input);
                                                                                                      return List.of(); })
                                                                       .build())
                                         .build();
    System.out.println("Running");
    flowMerge.run(flowReActorSystem);
    TimeUnit.SECONDS.sleep(5);
    System.out.println("Ran");
    flowMerge.stop(flowReActorSystem);
    flowReActorSystem.shutDown();
    TimeUnit.SECONDS.sleep(3);
  }
}
