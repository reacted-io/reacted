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
import io.reacted.flow.FlowGraph;
import io.reacted.flow.Stage;
import io.reacted.flow.operators.Mapper;
import io.reacted.flow.operators.Merge;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class FlowGraphExample {

  public static void main(String[] args) {
    ReActorSystem flowReActorSystem = ExampleUtils.getDefaultInitedReActorSystem("FlowGraphSystem");
    List<String> inputData = List.of("Hakuna", "Matata");
    List<Integer> inputInt = List.of(1, 2);
    FlowGraph flowMerge = FlowGraph.newBuilder()
                                   .setFlowName("FlowMerge")
                                   .addStage(Stage.newBuilder()
                                                  .setStageName("Provider-1")
                                                  .setOutputStageName("Feeder-1")
                                                  .setInputStream(inputData.stream())
                                                  .setOperatorProvider(reActorSystem -> Mapper.of(reActorSystem,
                                                                                                  ReActorConfig.newBuilder()
                                                                                                               .setReActorName("ToLower-2")
                                                                                                               .build(),
                                                                                                  input -> List.of(((String)input).toLowerCase())))
                                                  .build())
                                   .addStage(Stage.newBuilder()
                                                  .setStageName("Provider-2")
                                                  .setOutputStageName("Feeder-2")
                                                  .setInputStream(inputInt.stream())
                                                  .setOperatorProvider(reActorSystem -> Mapper.of(reActorSystem,
                                                                                                  ReActorConfig.newBuilder()
                                                                                                               .setReActorName("Multiplier-2")
                                                                                                               .build(),
                                                                                                  input -> List.of(((Integer)input) * 123)))
                                                  .build())
                                   .addStage(Stage.newBuilder()
                                                  .setStageName("Merger-1")
                                                  .setOutputStageName("Finalizer-1")
                                                  .setOperatorProvider(reActorSystem -> CompletableFuture.completedFuture(reActorSystem.spawn(new Merge(List.of(String.class, Integer.class),
                                                                                                                                                        inputList -> List.of(inputList.stream()
                                                                                                                                                                                      .map(Object::toString)
                                                                                                                                                                                      .collect(Collectors.joining()))),
                                                                                                                                              ReActorConfig.newBuilder()
                                                                                                                                                           .build())
                                                                                                                                       .orElseSneakyThrow()))
                                                  .build())
                                   .addStage(Stage.newBuilder()
                                                  .setStageName("Finalizer-1")
                                                  .setOperatorProvider(reActorSystem -> Mapper.of(reActorSystem,
                                                                                                  ReActorConfig.newBuilder()
                                                                                                               .setReActorName("Printer-1")
                                                                                                               .build(),
                                                                                                  input -> { System.out.println(input);
                                                                                                             return List.of(); }))
                                                  .build())
                                   .build();
    System.out.println("Running");
    flowMerge.run(flowReActorSystem);
    System.out.println("Ran");
    
  }
}
