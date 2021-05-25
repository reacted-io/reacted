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
  LoopbackDriver<? extends ChannelDriverConfig<?, ?>> getLoopbackDriver();
  @Nonnull
  ReActorSystemRef getLoopBack();
  @Nonnull
  Optional<ReActorSystemRef> findGate(ReActorSystemId reActorSystemId,
                                      ChannelId preferredChannelId);
  @Nonnull
  Collection<ReActorSystemRef> findGates(ReActorSystemId reActorSystemId);
  @Nonnull
  Collection<ReActorSystemRef> findAllGates();
  @Nonnull
  Collection<ReActorSystemRef> findAllGates(ChannelId channelId);

  void unregisterRoute(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver);

  void unregisterRoute(ReActorSystemId reActorSystemId, ChannelId channelId);
  @Nonnull
  ReActorSystemRef registerNewRoute(ReActorSystemId reActorSystemId,
                                    ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> driver,
                                    ChannelId channelId, Properties channelProperties,
                                    ReActorRef sourceServiceRegistry);

  boolean registerNewSource(ReActorRef sourceServiceRegistry, ReActorSystemId target);

  void unregisterSource(ReActorRef registryDriver);

  void unregisterTarget(ReActorSystemId reActorSystemId);
}
