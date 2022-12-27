/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import com.google.common.base.Objects;
import io.reacted.core.messages.services.FilterItem;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.util.Collection;

@NonNullByDefault
@Immutable
public class FilterServiceDiscoveryRequest implements ReActedMessage {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FilterServiceDiscoveryRequest that = (FilterServiceDiscoveryRequest) o;
        return Objects.equal(getFilteringRuleToApply(), that.getFilteringRuleToApply()) &&
               Objects.equal(getServiceDiscoveryResult(), that.getServiceDiscoveryResult());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFilteringRuleToApply(), getServiceDiscoveryResult());
    }

    @Override
    public String toString() {
        return "FilterServiceDiscoveryRequest{" + "filteringRuleToApply=" + filteringRuleToApply + ", " +
               "serviceDiscoveryResult=" + serviceDiscoveryResult + '}';
    }
}
