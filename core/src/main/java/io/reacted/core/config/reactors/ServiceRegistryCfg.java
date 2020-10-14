/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

public abstract class ServiceRegistryCfg<BuilderT extends ServiceRegistryCfg.Builder<BuilderT, BuiltT>,
                                         BuiltT extends ServiceRegistryCfg<BuilderT, BuiltT>>
        extends ReActiveEntityConfig<BuilderT, BuiltT> {
    @NonNullByDefault
    private final Properties serviceRegistryProperties;

    protected ServiceRegistryCfg(@Nonnull Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.serviceRegistryProperties = Objects.requireNonNull(builder.serviceRegistryProperties);
    }

    @NonNullByDefault
    public final Properties getServiceRegistryProperties() { return serviceRegistryProperties; }

    public abstract static class Builder<BuilderT, BuiltT> extends ReActiveEntityConfig.Builder<BuilderT, BuiltT> {
        private Properties serviceRegistryProperties;

        protected Builder() { }

        public final BuilderT setServiceRegistryProperties(@Nonnull Properties serviceRegistryProperties) {
            this.serviceRegistryProperties = serviceRegistryProperties;
            return getThis();
        }
    }
}