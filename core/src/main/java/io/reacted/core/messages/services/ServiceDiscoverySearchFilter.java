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

import java.util.Properties;

public interface ServiceDiscoverySearchFilter {

    String FIELD_NAME_SERVICE_NAME = "serviceName";
    String FIELD_NAME_CPU_LOAD = "cpuLoad";
    String FIELD_NAME_FREE_MEMORY_SIZE = "freeMemorySize";
    String FIELD_NAME_IP_ADDRESS = "ipAddress";
    String FIELD_NAME_HOSTNAME = "hostName";

    String getServiceName();
    boolean matches(Properties serviceProperties, ReActorRef serviceGate, ReActorSystem localReActorSystem);
}
