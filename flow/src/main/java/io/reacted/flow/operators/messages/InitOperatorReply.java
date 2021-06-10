/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.messages;

import io.reacted.flow.operators.FlowOperator;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nullable;

@NonNullByDefault
public class InitOperatorReply implements Serializable {
  private final boolean initComplete;
  private final String operatorName;
  private final Class<? extends FlowOperator> operatorType;
  public InitOperatorReply(String operatorName, Class<? extends FlowOperator> operatorType,
                            boolean isInitComplete) {
    this.operatorName = operatorName;
    this.operatorType = operatorType;
    this.initComplete = isInitComplete;
  }

  public String getOperatorName() {
    return operatorName;
  }

  public Class<? extends FlowOperator> getOperatorType() {
    return operatorType;
  }
  public boolean isInitComplete() {
    return initComplete;
  }
}
