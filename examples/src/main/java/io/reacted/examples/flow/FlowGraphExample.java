/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.flow;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.examples.ExampleUtils;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.Stage;
import io.reacted.flow.operators.MapOperator;
import io.reacted.flow.operators.ReduceOperator;
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
                                         .addStage(Stage.newBuilder()
                                                        .setStageName("Provider-1")
                                                        .setOutputStageName("Reducer-1")
                                                        .setInputStream(inputData.stream())
                                                        .setOperatorProvider(reActorSystem -> MapOperator.of(reActorSystem,
                                                                                                             ReActorConfig.newBuilder()
                                                                                                                          .setReActorName("ToLower-2")
                                                                                                                          .build(),
                                                                                                             input -> List.of(((String)input).toLowerCase())))
                                                        .build())
                                         .addStage(Stage.newBuilder()
                                                        .setStageName("Provider-2")
                                                        .setOutputStageName("Reducer-1")
                                                        .setInputStream(inputInt.stream())
                                                        .setOperatorProvider(reActorSystem -> MapOperator.of(reActorSystem,
                                                                                                             ReActorConfig.newBuilder()
                                                                                                                          .setReActorName("Multiplier-2")
                                                                                                                          .build(),
                                                                                                             input -> List.of(((Integer)input) * 123)))
                                                        .build())
                                         .addStage(Stage.newBuilder()
                                                        .setStageName("Reducer-1")
                                                        .setOutputStageName("Finalizer-1")
                                                        .setOperatorProvider(reActorSystem -> ReduceOperator.of(reActorSystem,
                                                                                                                ReActorConfig.newBuilder()
                                                                                                                             .setReActorName("Joiner-1")
                                                                                                                             .build(),
                                                                                                                List.of(String.class, Integer.class),
                                                                                                                inputMap -> List.of(inputMap.values().stream()
                                                                                                                                            .flatMap(List::stream)
                                                                                                                                            .map(Object::toString)
                                                                                                                                            .collect(Collectors.joining(" ")))))
                                                        .build())
                                         .addStage(Stage.newBuilder()
                                                        .setStageName("Finalizer-1")
                                                        .setOperatorProvider(reActorSystem -> MapOperator.of(reActorSystem,
                                                                                                             ReActorConfig.newBuilder()
                                                                                                                          .setReActorName("Printer-1")
                                                                                                                          .build(),
                                                                                                             input -> { System.out.println(input);
                                                                                                                        return List.of(); }))
                                                        .build())
                                         .build();
    System.out.println("Running");
    flowMerge.run(flowReActorSystem);
    TimeUnit.SECONDS.sleep(5);
    System.out.println("Ran");
    flowMerge.stop(flowReActorSystem);
    flowReActorSystem.shutDown();
  }
}
