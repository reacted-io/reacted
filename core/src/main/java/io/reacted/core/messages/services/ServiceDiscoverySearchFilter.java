/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.services.SelectionType;
import io.reacted.core.services.Service;

import java.util.Properties;

public interface ServiceDiscoverySearchFilter extends ReActedMessage {

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
     * @see SelectionType and {@link Service}
     */
    SelectionType getSelectionType();

    /**
     * Perform a check if the found service matches the request
     * @param serviceProperties service properties
     * @param serviceGate {@link ReActorRef} to the discovered service
     * @return the result of the match
     */
    boolean matches(Properties serviceProperties, ReActorRef serviceGate);

    /**
     * Returns a string that uniquely identifies this filter in a given moment within the
     * local Reactor System. This Id needs to be unique while the request is being performed.
     * Sequential requests can use the same id.
     * @return A String that uniquely identifies the request within a Reactor System
     */
    String getDiscoveryRequestId();
}
