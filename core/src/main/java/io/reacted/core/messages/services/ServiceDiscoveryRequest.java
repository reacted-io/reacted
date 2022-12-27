/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;

@Immutable
@NonNullByDefault
public class ServiceDiscoveryRequest implements ReActedMessage {
    private final ServiceDiscoverySearchFilter searchFilter;
    public ServiceDiscoveryRequest(ServiceDiscoverySearchFilter searchFilter) {
        this.searchFilter = searchFilter;
    }
    public ServiceDiscoverySearchFilter getSearchFilter() { return searchFilter; }

    @Override
    public String toString() {
        return "ServiceDiscoveryRequest{" +
               "searchFilter=" + searchFilter +
               '}';
    }
}
