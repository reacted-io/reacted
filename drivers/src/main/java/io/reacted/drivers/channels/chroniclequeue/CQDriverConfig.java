/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.drivers.ChannelDriverConfig;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

public abstract class CQDriverConfig<BuilderT extends ChannelDriverConfig.Builder<BuilderT, BuiltT>,
        BuiltT extends ChannelDriverConfig<BuilderT, BuiltT>> extends ChannelDriverConfig<BuilderT, BuiltT> {
    public static final String CQ_FILES_DIRECTORY = "chronicleFilesDir";
    private final String chronicleFilesDir;

    protected CQDriverConfig(Builder<BuilderT, BuiltT> configBuilder) {
        super(configBuilder);
        this.chronicleFilesDir = Objects.requireNonNull(configBuilder.chronicleFilesDir,
                                                        "Output directory cannot be null");
    }

    public String getChronicleFilesDir() { return chronicleFilesDir; }
    @Nonnull
    @Override
    public Properties getChannelProperties() {
        Properties properties = super.getChannelProperties();
        properties.setProperty(CQ_FILES_DIRECTORY, getChronicleFilesDir());
        return properties;
    }

    public abstract static class Builder<BuilderT, BuiltT> extends ChannelDriverConfig.Builder<BuilderT, BuiltT> {
        private String chronicleFilesDir;
        protected Builder() { }

        public BuilderT setChronicleFilesDir(String chronicleFilesDir) {
            this.chronicleFilesDir = chronicleFilesDir;
            return getThis();
        }
    }
}
