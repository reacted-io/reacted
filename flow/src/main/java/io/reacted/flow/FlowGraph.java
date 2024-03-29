/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.Try;
import io.reacted.patterns.annotations.unstable.Unstable;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

@Unstable
public interface FlowGraph {

  /**
   * Start the computational graph
   * @param localReActorSystem {@link ReActorSystem} to use for spawning the graph stages and
   *                                                services resolution
   */
  @Nonnull
  CompletionStage<Try<Map<String, ReActorRef>>> run(@Nonnull ReActorSystem localReActorSystem);

  /**
   * Stop a computational graph
   */
  Optional<CompletionStage<Void>> stop(@Nonnull ReActorSystem localReActorSystem);

  @Nonnull
  String getFlowName();

  @Nonnull
  Map<String, ReActorRef> getOperatorsByName();
}
