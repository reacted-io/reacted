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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;


@NonNullByDefault
public class RegistryServicePublicationRequest implements Externalizable {
    private static final long serialVersionUID = 1;
    private static final long SERVICE_GATE_OFFSET = SerializationUtils.getFieldOffset(RegistryServicePublicationRequest.class,
                                                                                      "serviceGate")
                                                                      .orElseSneakyThrow();
    private static final long SERVICE_PROPERTIES_OFFSET = SerializationUtils.getFieldOffset(RegistryServicePublicationRequest.class,
                                                                                            "serviceProperties")
                                                                            .orElseSneakyThrow();
    private final ReActorRef serviceGate;
    private final Properties instanceProperties;

    public RegistryServicePublicationRequest(ReActorRef serviceGate, Properties serviceProperties) {
        this.serviceGate = serviceGate;
        this.instanceProperties = serviceProperties;
    }

    public RegistryServicePublicationRequest() {
        this.serviceGate = ReActorRef.NO_REACTOR_REF;
        this.instanceProperties = new Properties();
    }

    public ReActorRef getServiceGate() { return this.serviceGate; }

    public Properties getServiceProperties() { return this.instanceProperties; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.serviceGate);
        out.writeObject(this.instanceProperties);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setServiceGate((ReActorRef) in.readObject())
                .setInstanceProperties((Properties)in.readObject());
    }

    @SuppressWarnings("UnusedReturnValue")
    private RegistryServicePublicationRequest setServiceGate(ReActorRef reactorRef) {
        return SerializationUtils.setObjectField(this, SERVICE_GATE_OFFSET, reactorRef);
    }

    @SuppressWarnings("UnusedReturnValue")
    private RegistryServicePublicationRequest setInstanceProperties(Properties instanceProperties) {
        return SerializationUtils.setObjectField(this, SERVICE_PROPERTIES_OFFSET, instanceProperties);
    }
}
