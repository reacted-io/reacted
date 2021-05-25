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

public class NullRegistryGatesCentralizedManager implements GatesRegistry {

  @Nonnull
  @Override
  public LoopbackDriver<? extends ChannelDriverConfig<?, ?>> getLoopbackDriver() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public ReActorSystemRef getLoopBack() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Optional<ReActorSystemRef> findGate(@Nonnull ReActorSystemId reActorSystemId,
                                             @Nonnull ChannelId preferredChannelId) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Collection<ReActorSystemRef> findGates(@Nonnull ReActorSystemId reActorSystemId) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Collection<ReActorSystemRef> findAllGates() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Collection<ReActorSystemRef> findAllGates(@Nonnull ChannelId channelId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void unregisterRoute(
      @Nonnull ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void unregisterRoute(@Nonnull ReActorSystemId reActorSystemId, @Nonnull ChannelId channelId) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public ReActorSystemRef registerNewRoute(@Nonnull ReActorSystemId reActorSystemId,
                                           @Nonnull ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> driver,
                                           @Nonnull ChannelId channelId, @Nonnull Properties channelProperties,
                                           @Nonnull ReActorRef sourceServiceRegistry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized boolean registerNewSource(@Nonnull ReActorRef sourceServiceRegistry,
                                                @Nonnull ReActorSystemId target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void unregisterSource(@Nonnull ReActorRef registryDriver) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void unregisterTarget(@Nonnull ReActorSystemId reActorSystemId) {
    throw new UnsupportedOperationException();
  }
}
