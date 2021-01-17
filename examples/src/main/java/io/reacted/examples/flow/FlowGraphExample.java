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
import io.reacted.flow.operators.Map;
import java.util.List;

public class FlowGraphExample {

  public static void main(String[] args) {
    ReActorSystem flowReActorSystem = ExampleUtils.getDefaultInitedReActorSystem("FlowGraphSystem");
    List<String> inputData = List.of("Hakuna", "Matata");
    FlowGraph flow = FlowGraph.newBuilder()
                              .setFlowName("TestFlow")
                              .addStage(Stage.newBuilder()
                                             .setStageName("ToLower")
                                             .setInputStream(inputData.stream())
                                             .setOperatorProvider(reActorSystem -> Map.of(reActorSystem,
                                                                                          ReActorConfig.newBuilder()
                                                                                                       .setReActorName("ToLower-1")
                                                                                                       .build(),
                                                                                          input -> List.of(((String)input).toLowerCase())))
                                             .setOutputStagesNames("Printer")
                                             .build())
                              .addStage(Stage.newBuilder()
                                             .setStageName("Printer")
                                             .setOperatorProvider(reActorSystem -> Map.of(reActorSystem,
                                                                                          ReActorConfig.newBuilder()
                                                                                                       .setReActorName("Printer-1")
                                                                                                       .build(),
                                                                                          input -> { System.out.println(input); return List.of(); }))
                                             .build())
                              .build();
    System.out.println("Running");
    flow.run(flowReActorSystem);
    System.out.println("Ran");
    
  }
}
