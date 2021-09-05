/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;

@Immutable
@NonNullByDefault
public class ServiceCancellationRequest implements Serializable {
    private final ReActorSystemId reActorSystemId;
    private final String serviceName;

    public ServiceCancellationRequest(ReActorSystemId reActorSystemId, String serviceName) {
        this.reActorSystemId = reActorSystemId;
        this.serviceName = serviceName;
    }

    public ReActorSystemId getReActorSystemId() { return reActorSystemId; }

    public String getServiceName() { return serviceName; }

    @Override
    public String toString() {
        return "ServiceCancellationRequest{" +
               "reActorSystemId=" + reActorSystemId +
               ", serviceName='" + serviceName + '\'' +
               '}';
    }
}
