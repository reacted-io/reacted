/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.system.LoopbackDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
interface GatesRegistry {

  @Nonnull
  default LoopbackDriver<? extends ChannelDriverConfig<?, ?>> getLoopbackDriver() {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default ReActorSystemRef getLoopBack() {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default Optional<ReActorSystemRef> findGate(ReActorSystemId reActorSystemId,
                                              ChannelId preferredChannelId) {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default Collection<ReActorSystemRef> findGates(ReActorSystemId reActorSystemId) {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default Collection<ReActorSystemRef> findAllGates() {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default Collection<ReActorSystemRef> findAllGates(ChannelId channelId) {
    throw new UnsupportedOperationException();
  }

  default void unregisterRoute(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver) {
    throw new UnsupportedOperationException();
  }

  default void unregisterRoute(ReActorSystemId reActorSystemId, ChannelId channelId) {
    throw new UnsupportedOperationException();
  }
  @Nonnull
  default ReActorSystemRef registerNewRoute(ReActorSystemId reActorSystemId,
                                            ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> driver,
                                            ChannelId channelId, Properties channelProperties,
                                            ReActorRef sourceServiceRegistry) {
    throw new UnsupportedOperationException();
  }

  default boolean registerNewSource(ReActorRef sourceServiceRegistry, ReActorSystemId target) {
    throw new UnsupportedOperationException();
  }

  default void unregisterSource(ReActorRef registryDriver) {
    throw new UnsupportedOperationException();
  }

  default void unregisterTarget(ReActorSystemId reActorSystemId) {
    throw new UnsupportedOperationException();
  }
}
