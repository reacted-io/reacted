/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.messages;

import io.reacted.flow.operators.FlowOperator;
import io.reacted.flow.operators.FlowOperatorConfig;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;

@NonNullByDefault
public class OperatorOutputGatesReply<BuilderT extends FlowOperatorConfig.Builder<BuilderT, BuiltT>,
                                  BuiltT extends FlowOperatorConfig<BuilderT, BuiltT>> implements Serializable {
  private final boolean initComplete;
  private final String operatorName;
  private final Class<? extends FlowOperator<BuilderT, BuiltT>> operatorType;
  public OperatorOutputGatesReply(String operatorName,
                                  Class<? extends FlowOperator<BuilderT, BuiltT>> operatorType,
                                  boolean isInitComplete) {
    this.operatorName = operatorName;
    this.operatorType = operatorType;
    this.initComplete = isInitComplete;
  }

  public String getOperatorName() {
    return operatorName;
  }

  public Class<? extends FlowOperator<BuilderT, BuiltT>> getOperatorType() {
    return operatorType;
  }

  public boolean isInitComplete() {
    return initComplete;
  }

  @Override
  public String toString() {
    return "OperatorOutputGatesReply{" +
           "initComplete=" + initComplete +
           ", operatorName='" + operatorName + '\'' +
           ", operatorType=" + operatorType +
           '}';
  }
}
