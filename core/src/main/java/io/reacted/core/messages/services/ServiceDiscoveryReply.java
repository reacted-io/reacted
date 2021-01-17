/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.reactorsystem.ReActorRef;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class ServiceDiscoveryReply implements Serializable {
    private final Set<ReActorRef> serviceRefs;
    public ServiceDiscoveryReply(ReActorRef serviceRef) {
        this.serviceRefs = Set.of(serviceRef);
    }
    public ServiceDiscoveryReply(Set<ReActorRef> serviceRefs) {
        this.serviceRefs = Collections.unmodifiableSet(serviceRefs);
    }
    public Set<ReActorRef> getServiceGates() { return serviceRefs; }

    @Override
    public String toString() {
        return "ServiceDiscoveryReply{" + "serviceRefs=" + serviceRefs + '}';
    }
}
