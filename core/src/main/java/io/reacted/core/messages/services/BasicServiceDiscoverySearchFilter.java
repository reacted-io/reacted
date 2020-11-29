/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class BasicServiceDiscoverySearchFilter extends GenericServiceDiscoverySearchFilter<BasicServiceDiscoverySearchFilter.Builder,
                                                                                           BasicServiceDiscoverySearchFilter>
        implements ServiceDiscoverySearchFilter {

    private BasicServiceDiscoverySearchFilter(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder extends GenericServiceDiscoverySearchFilter.Builder<Builder, BasicServiceDiscoverySearchFilter> {
        private Builder() { }

        @Override
        public BasicServiceDiscoverySearchFilter build() {
            return new BasicServiceDiscoverySearchFilter(this);
        }
    }
}
