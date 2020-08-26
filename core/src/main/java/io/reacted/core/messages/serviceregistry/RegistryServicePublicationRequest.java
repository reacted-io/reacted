/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;


@NonNullByDefault
public class RegistryServicePublicationRequest implements Serializable {
    private static final String SERVICE_NAME_SEPARATOR = ";";
    private static final String NAME_TO_ID_SEPARATOR = ":";
    private static final String REACTORSYSTEM_REF_SEPARATOR = "@";
    private final String serviceName;
    private final ReActorRef serviceGate;

    public RegistryServicePublicationRequest(ReActorRef serviceGate, String serviceName) {
        this.serviceGate = serviceGate;
        this.serviceName = serviceName;
    }

    public String toSerializedString() {
        return serviceName + SERVICE_NAME_SEPARATOR +
               serviceGate.getReActorId().getReActorName() + NAME_TO_ID_SEPARATOR +
               serviceGate.getReActorId().getReActorUuid().toString() + REACTORSYSTEM_REF_SEPARATOR +
               serviceGate.getReActorSystemRef().getReActorSystemId().getReActorSystemName();
    }

    public ReActorRef getServiceGate() { return serviceGate; }

    public String getServiceName() { return serviceName; }

    /**
     * @param serializedString String returned by toSerializedString
     * @throws RuntimeException Any runtime exception generated during the parsing of an illegal string
     */
    public static RegistryServicePublicationRequest fromSerializedString(String serializedString) {
        String[] serviceNameAndGate = serializedString.split(SERVICE_NAME_SEPARATOR);
        String[] reActorAndReActorSystem = serviceNameAndGate[1].split(REACTORSYSTEM_REF_SEPARATOR);
        String[] reActorNameAndUuid = reActorAndReActorSystem[0].split(NAME_TO_ID_SEPARATOR);
        String reActorSystemName = reActorAndReActorSystem[1];
        UUID reActorUuid = UUID.fromString(reActorNameAndUuid[1]);
        ReActorId reActorId = new ReActorId().setReActorName(reActorNameAndUuid[0])
                                             .setUuid(reActorUuid)
                                             .setHashCode(Objects.hash(reActorUuid, reActorNameAndUuid[0]));
        ReActorSystemId reActorSystemId = new ReActorSystemId(reActorSystemName);
        return new RegistryServicePublicationRequest(new ReActorRef(reActorId,
                                                                    new ReActorSystemRef(null, null, reActorSystemId)),
                                                     serviceNameAndGate[0]);
    }
}
