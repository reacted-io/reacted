/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.builders;

import io.reacted.flow.operators.PipelineStage;

public interface StepLevel<BuiltT> {
    StepLevel<BuiltT> setLevelName(String levelName);
    StepLevel<BuiltT> setOperator(PipelineStage operator);
    BuiltT build();
}
