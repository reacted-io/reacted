/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.system.LoopbackDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.patterns.NonNullByDefault;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@NonNullByDefault
class RegistryGatesCentralizedManager {
    /* Maps a reactor system to a route for reaching it. Multiple reactor systems may be reached from the
     *  same driver. A driver allows you to communicate with through a given middleware, so what it offers
     *  is a gate to reach other reactor systems */
    private final Map<ReActorSystemId, Map<ChannelId, ReActorSystemRef>> reActorSystemsGates;
    private final Multimap<ReActorRef, ReActorSystemId> serviceRegistryToReActorSystemId;
    private final Map<ReActorSystemId, ReActorRef> reActorSystemIdToSourceServiceRegistry;
    private final LoopbackDriver<? extends ChannelDriverConfig<?, ?>> loopbackDriver;
    private final ReActorSystemId localReActorSystemId;
    private final ReActorSystemRef loopBack;

    RegistryGatesCentralizedManager(ReActorSystemId localReActorSystemId,
                                    LoopbackDriver<? extends ChannelDriverConfig<?, ?>> loopbackDriver) {
        this.loopbackDriver = loopbackDriver;
        this.serviceRegistryToReActorSystemId = HashMultimap.create();
        this.reActorSystemIdToSourceServiceRegistry = new HashMap<>();
        this.reActorSystemsGates = new ConcurrentHashMap<>();
        this.localReActorSystemId = localReActorSystemId;
        this.loopBack = registerNewRoute(localReActorSystemId, loopbackDriver, loopbackDriver.getChannelId(),
                                         new Properties(), ReActorRef.NO_REACTOR_REF);
    }
    public LoopbackDriver<? extends ChannelDriverConfig<?, ?>> getLoopbackDriver() { return loopbackDriver; }
    public ReActorSystemRef getLoopBack() { return loopBack; }

    @Nullable
    public ReActorSystemRef findGate(@Nonnull ReActorSystemId reActorSystemId,
                                     @Nonnull ChannelId preferredChannelId) {
        if (ReActorSystemDriver.isLocalReActorSystem(localReActorSystemId, reActorSystemId)) {
            return loopBack;
        }
        var routesToReActorSystem = reActorSystemsGates.getOrDefault(reActorSystemId, Map.of());
        var route = routesToReActorSystem.get(preferredChannelId);
        if (route == null && routesToReActorSystem.size() != 0) {
            return routesToReActorSystem.values().stream().findAny().orElse(null);
        }
        return route;
    }

    public Collection<ReActorSystemRef> findGates(@Nonnull ReActorSystemId reActorSystemId) {
        return ReActorSystemDriver.isLocalReActorSystem(reActorSystemId, localReActorSystemId)
               ? List.of(loopBack)
               : new ArrayList<>(reActorSystemsGates.getOrDefault(reActorSystemId, Map.of())
                                                    .values());
    }

    public Collection<ReActorSystemRef> findAllGates() {
        return reActorSystemsGates.values().stream()
                .flatMap(gatesMap -> gatesMap.values().stream())
                .collect(Collectors.toUnmodifiableSet());
    }

    public Collection<ReActorSystemRef> findAllGates(@Nonnull ChannelId channelId) {
        return reActorSystemsGates.values().stream()
                                  .map(gatesMap -> gatesMap.get(channelId))
                                  .collect(Collectors.toUnmodifiableSet());
    }

    public synchronized void unregisterRoute(
        @Nonnull ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver) {
        reActorSystemsGates.entrySet().stream()
                           .filter(entry -> entry.getValue().values().stream()
                                                 .map(ReActorSystemRef::getBackingDriver)
                                                 .anyMatch(driver -> driver.equals(anyDriver)))
                           .forEachOrdered(entry -> entry.getValue()
                                                         .forEach((channelId, reActorRef) -> unregisterRoute(entry.getKey(),
                                                                                                             channelId)));
    }

    public synchronized void unregisterRoute(@Nonnull ReActorSystemId reActorSystemId, @Nonnull ChannelId channelId) {
        Optional.ofNullable(reActorSystemsGates.get(reActorSystemId))
                .ifPresent(elem -> elem.remove(channelId));
        unregisterTarget(reActorSystemId);
    }

    //XXX Define how a given reactor system / channel is reached from the current reactor system
    //A reactorsystem can be reached through different channels (i.e. kafka, grpc, chronicle queue...)
    //and channel properties define how to setup the channel driver to reach the reactor system.
    //i.e. for a reactor system reachable through grpc, channelProperties will contain ip/address of the other
    //reactor system
    public ReActorSystemRef registerNewRoute(@Nonnull ReActorSystemId reActorSystemId,
                                             @Nonnull ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> driver,
                                             @Nonnull ChannelId channelId, @Nonnull Properties channelProperties,
                                             @Nonnull ReActorRef sourceServiceRegistry) {
        var channelMap = this.reActorSystemsGates.computeIfAbsent(reActorSystemId,
                                                                  newReActorSystem -> new ConcurrentHashMap<>());
        var newRoute = channelMap.computeIfAbsent(channelId,
                                                  newChannelId -> new ReActorSystemRef(driver, channelProperties,
                                                                                       newChannelId, reActorSystemId));
        registerNewSource(sourceServiceRegistry, reActorSystemId);
        return newRoute;
    }

    public synchronized boolean registerNewSource(@Nonnull ReActorRef sourceServiceRegistry,
                                                  @Nonnull ReActorSystemId target) {
        var previousSource = reActorSystemIdToSourceServiceRegistry.get(target);
        if (previousSource != null && !previousSource.equals(sourceServiceRegistry)) {
            return false;
        }
        this.reActorSystemIdToSourceServiceRegistry.put(target, sourceServiceRegistry);
        this.serviceRegistryToReActorSystemId.put(sourceServiceRegistry, target);
        return true;
    }

    public synchronized void unregisterSource(@Nonnull ReActorRef registryDriver) {
        this.serviceRegistryToReActorSystemId.removeAll(registryDriver)
                                             .forEach(this.reActorSystemIdToSourceServiceRegistry::remove);
    }

    public synchronized void unregisterTarget(@Nonnull ReActorSystemId reActorSystemId) {
        var source = this.reActorSystemIdToSourceServiceRegistry.remove(reActorSystemId);
        if (source != null) {
            serviceRegistryToReActorSystemId.get(source).remove(reActorSystemId);
        }
    }
}
