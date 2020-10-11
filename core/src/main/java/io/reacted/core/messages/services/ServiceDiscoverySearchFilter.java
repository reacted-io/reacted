/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.services.SelectionType;

import java.io.Serializable;
import java.util.Properties;

public interface ServiceDiscoverySearchFilter extends Serializable {

    String FIELD_NAME_SERVICE_NAME = "serviceName";
    String FIELD_NAME_CPU_LOAD = "cpuLoad";
    String FIELD_NAME_FREE_MEMORY_SIZE = "freeMemorySize";
    String FIELD_NAME_IP_ADDRESS = "ipAddress";
    String FIELD_NAME_HOSTNAME = "hostName";

    /**
     * @return The name of the service that we are looking for
     */
    String getServiceName();

    /**
     * @return The type of the reference that should be returned, is possible.
     * @see SelectionType and {@link io.reacted.core.services.ReActorService}
     */
    SelectionType getSelectionType();

    /**
     * Perform a check if the found service matches the request
     * @param serviceProperties service properties
     * @param serviceGate {@link ReActorRef} to the discovered service
     * @return the result of the match
     */
    boolean matches(Properties serviceProperties, ReActorRef serviceGate);
}
