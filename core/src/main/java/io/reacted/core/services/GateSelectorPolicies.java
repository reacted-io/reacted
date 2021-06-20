/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

import io.reacted.core.reactorsystem.ReActorRef;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public final class GateSelectorPolicies {
  private GateSelectorPolicies() { /* No implementation allowed */}
  public static final Function<Collection<ReActorRef>, Optional<ReActorRef>> FIRST_GATE =
      gates -> Optional.ofNullable(gates.isEmpty() ? null : gates.iterator().next());
  public static final Function<Collection<ReActorRef>, Optional<ReActorRef>> RANDOM_GATE =
      gates -> gates.isEmpty()
               ? Optional.empty()
               : gates.size() == 1
                 ? Optional.of(gates.iterator().next())
                 : gates.stream()
                        .skip(ThreadLocalRandom.current().nextInt(0, gates.size() - 1))
                        .findAny();

}
