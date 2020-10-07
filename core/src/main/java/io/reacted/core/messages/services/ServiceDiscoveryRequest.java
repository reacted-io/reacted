/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.services.SelectionType;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class ServiceDiscoveryRequest implements Serializable {
    private final String serviceName;
    private final SelectionType selectionType;
    public ServiceDiscoveryRequest(String serviceName, SelectionType selectionType) {
        this.serviceName = serviceName;
        this.selectionType = selectionType;
    }
    public boolean matchRequest(String serviceName) {
        return Objects.equals(this.serviceName, serviceName);
    }

    public String getServiceName() { return serviceName; }

    public SelectionType getSelectionType() { return selectionType; }
}
