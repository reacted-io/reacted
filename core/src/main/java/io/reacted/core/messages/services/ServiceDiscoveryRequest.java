/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.patterns.NonNullByDefault;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;

@Immutable
@NonNullByDefault
public class ServiceDiscoveryRequest implements Serializable {
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
