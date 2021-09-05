/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.reduce;

public class ReduceOperator extends ReducingOperator<ReduceOperatorConfig.Builder,
                                                     ReduceOperatorConfig> {
  ReduceOperator(ReduceOperatorConfig config) {
    super(config);
  }
}
