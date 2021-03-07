/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.operators.messages;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

@NonNullByDefault
public final class SetNextStagesRequest implements Serializable {
  private final Set<ReActorRef> nextStages;
  public SetNextStagesRequest(Set<ReActorRef> nextStages) {
    this.nextStages = Objects.requireNonNull(nextStages, "Next stages cannot be null");
  }
  public Set<ReActorRef> getNextStages() { return nextStages; }
}