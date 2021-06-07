/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.patterns.Try;
import java.util.Collection;
import javax.annotation.Nonnull;

public interface FlowGraph {

  /**
   * Start the computational graph
   * @param localReActorSystem {@link ReActorSystem} to use for spawning the graph stages and
   *                                                services resolution
   */
  Try<Void> run(@Nonnull ReActorSystem localReActorSystem);

  /**
   * Stop a computational graph
   */
  void stop(@Nonnull ReActorSystem localReActorSystem);

  /**
   * @return The Stages of this graph
   */
  @Nonnull
  Collection<FlowOperatorConfig<? extends FlowOperatorConfig.Builder<?, ?>,
                                ? extends FlowOperatorConfig<?, ?>>> getOperatorsCfgs();

  @Nonnull
  String getFlowName();
}
