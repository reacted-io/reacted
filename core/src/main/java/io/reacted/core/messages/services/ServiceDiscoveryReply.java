/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class ServiceDiscoveryReply implements Serializable {
    private final Set<ReActorRef> serviceRefs;
    public ServiceDiscoveryReply(ReActorRef serviceRef, ReActorSystem reActorSystem) {
        this.serviceRefs = ReActorSystem.getRoutedReference(serviceRef, reActorSystem);
    }
    public ServiceDiscoveryReply(Collection<ReActorRef> serviceRefs, ReActorSystem reActorSystem) {
        this.serviceRefs = serviceRefs.stream()
                                      .flatMap(serviceRef -> ReActorSystem.getRoutedReference(serviceRef,
                                                                                              reActorSystem).stream())
                                      .collect(Collectors.toUnmodifiableSet());
    }
    public Set<ReActorRef> getServiceGates() { return serviceRefs; }
}
