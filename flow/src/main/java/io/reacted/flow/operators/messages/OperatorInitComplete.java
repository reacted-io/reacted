/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.messages;

import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@NonNullByDefault
@Immutable
public class OperatorInitComplete implements Serializable {
  private final String flowName;
  private final String operatorName;
  private final String routeeName;
  public OperatorInitComplete(String flowName, String operatorName, String routeeName) {
    this.flowName = Objects.requireNonNull(flowName);
    this.operatorName = Objects.requireNonNull(operatorName);
    this.routeeName = Objects.requireNonNull(routeeName);
  }

  public String getOperatorName() { return operatorName; }

  public String getFlowName() { return flowName; }

  public String getRouteeName() { return routeeName; }
}
