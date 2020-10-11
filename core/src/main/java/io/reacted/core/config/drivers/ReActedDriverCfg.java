/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.InheritableBuilder;
import io.reacted.core.utils.ConfigUtils;
import io.reacted.patterns.NonNullByDefault;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;

@NonNullByDefault
public abstract class ReActedDriverCfg<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                       BuiltT extends InheritableBuilder<BuilderT, BuiltT>>
        extends InheritableBuilder<BuilderT, BuiltT> {
    public static final String CHANNEL_ID_PROPERTY_NAME = "channelName";
    public static final String IS_DELIVERY_ACK_REQUIRED_BY_CHANNEL_PROPERTY_NAME = "deliveryAckRequiredByChannel";
    private final String channelName;
    private final boolean deliveryAckRequiredByChannel;

    protected ReActedDriverCfg(Builder<BuilderT, BuiltT> builder) {
        super(builder);
        this.channelName = Objects.requireNonNull(builder.channelName);
        this.deliveryAckRequiredByChannel = builder.deliveryAckRequiredByChannel;
    }

    public Properties getProperties() {
        return ConfigUtils.toProperties(this, Set.of());
    }

    public String getChannelName() { return channelName; }

    public boolean isDeliveryAckRequiredByChannel() { return deliveryAckRequiredByChannel; }

    public static abstract class Builder<BuilderT, BuiltT>
            extends InheritableBuilder.Builder<BuilderT, BuiltT> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private String channelName;
        private boolean deliveryAckRequiredByChannel;

        public final BuilderT setChannelName(String channelName) {
            this.channelName = channelName;
            return getThis();
        }

        public final BuilderT setChannelRequiresDeliveryAck(boolean isDeliveryAckRequiredByChannel) {
            this.deliveryAckRequiredByChannel = isDeliveryAckRequiredByChannel;
            return getThis();
        }
    }
}
