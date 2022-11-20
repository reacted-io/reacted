/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;

public abstract class ServiceRegistryConfig<BuilderT extends ServiceRegistryConfig.Builder<BuilderT,
                                                                                           BuiltT>,
                                            BuiltT extends ServiceRegistryConfig<BuilderT, BuiltT>>
        extends ReActiveEntityConfig<BuilderT, BuiltT> {
    @Nonnull
    private final Properties serviceRegistryProperties;

    protected ServiceRegistryConfig(@Nonnull Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.serviceRegistryProperties = Objects.requireNonNull(builder.serviceRegistryProperties,
                                                                "Service registry properties " +
                                                                "cannot be null");
    }

    @Nonnull
    public final Properties getServiceRegistryProperties() { return serviceRegistryProperties; }

    public abstract static class Builder<BuilderT, BuiltT>
        extends ReActiveEntityConfig.Builder<BuilderT, BuiltT> {
        private Properties serviceRegistryProperties;

        protected Builder() { this.serviceRegistryProperties = new Properties(); }

        /**
         * Set {@link Properties} for a service registry
         * @param serviceRegistryProperties Non-null {@link Properties} for this service registry
         *                                  instance. Default: empty properties
         * @return this {@link Builder}
         */
        public final BuilderT
        setServiceRegistryProperties(@Nonnull Properties serviceRegistryProperties) {
            this.serviceRegistryProperties = serviceRegistryProperties;
            return getThis();
        }
    }
}
