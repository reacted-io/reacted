/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;


@NonNullByDefault
public class ServicePublicationRequest implements Serializable {

    private static final long serialVersionUID = 1;
    private static final long SERVICE_GATE_OFFSET = SerializationUtils.getFieldOffset(ServicePublicationRequest.class,
                                                                                      "serviceGate")
                                                                      .orElseSneakyThrow();
    private static final long SERVICE_PROPERTIES_OFFSET = SerializationUtils.getFieldOffset(ServicePublicationRequest.class,
                                                                                            "serviceProperties")
                                                                            .orElseSneakyThrow();
    private final ReActorRef serviceGate;
    private final Properties serviceProperties;

    private ServicePublicationRequest() {
        this.serviceGate = ReActorRef.NO_REACTOR_REF;
        this.serviceProperties = new Properties();
    }

    public ServicePublicationRequest(ReActorRef serviceGate, Properties serviceProperties) {
        this.serviceGate = Objects.requireNonNull(serviceGate);
        this.serviceProperties = Objects.requireNonNull(serviceProperties);
    }

    public ReActorRef getServiceGate() { return this.serviceGate; }

    public Properties getServiceProperties() { return this.serviceProperties; }

    @SuppressWarnings({"UnusedReturnValue", "unused"})
    private ServicePublicationRequest setServiceGate(ReActorRef reactorRef) {
        return SerializationUtils.setObjectField(this, SERVICE_GATE_OFFSET, reactorRef);
    }

    @SuppressWarnings({"UnusedReturnValue", "unused"})
    private ServicePublicationRequest setServiceProperties(Properties serviceProperties) {
        return SerializationUtils.setObjectField(this, SERVICE_PROPERTIES_OFFSET, serviceProperties);
    }
}
