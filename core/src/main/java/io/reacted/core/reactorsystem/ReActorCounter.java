/*
 * Copyright (c) 2022 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import java.util.concurrent.atomic.AtomicLong;

enum ReActorCounter {
  INSTANCE;

  private final AtomicLong reactorSchedulationId = new AtomicLong();

  public long nextSchedulationId() { return reactorSchedulationId.getAndIncrement(); }
}
