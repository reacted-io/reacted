/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.messages.services.FilterItem;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serializable;
import java.util.Collection;

@NonNullByDefault
@Immutable
public class FilterServiceDiscoveryRequest implements Serializable {
    private final ServiceDiscoverySearchFilter filteringRuleToApply;
    private final Collection<FilterItem> serviceDiscoveryResult;

    public FilterServiceDiscoveryRequest(ServiceDiscoverySearchFilter filteringRuleToApply,
                                         Collection<FilterItem> serviceDiscoveryResult) {
        this.filteringRuleToApply = filteringRuleToApply;
        this.serviceDiscoveryResult = serviceDiscoveryResult;
    }

    public ServiceDiscoverySearchFilter getFilteringRuleToApply() {
        return filteringRuleToApply;
    }

    public Collection<FilterItem> getServiceDiscoveryResult() {
        return serviceDiscoveryResult;
    }
}
