/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.NullDriver;
import io.reacted.core.drivers.system.NullDriverConfig;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public final class NullReActorSystemRef extends ReActorSystemRef {
    public static final NullReActorSystemRef NULL_REACTOR_SYSTEM_REF = new NullReActorSystemRef(ReActorSystemId.NO_REACTORSYSTEM_ID,
                                                                                                NullDriver.NULL_DRIVER_PROPERTIES,
                                                                                                NullDriver.NULL_DRIVER);

    public NullReActorSystemRef() { /* Required by Externalizable */ }
    private NullReActorSystemRef(ReActorSystemId reActorSystemId, Properties channelProperties,
                                ReActorSystemDriver<NullDriverConfig> reActorSystemDriver) {
        super(reActorSystemDriver, channelProperties, reActorSystemId);
        super.setChannelId(ChannelId.NO_CHANNEL_ID);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>> tell(ReActorRef src, ReActorRef dst,
                                                                                     AckingPolicy ackingPolicy,
                                                                                     PayloadT message) {
        return NullDriver.NULL_DRIVER.tell(src, dst,ackingPolicy, message);
    }

    @Override
    public ReActorSystemId getReActorSystemId() { return ReActorSystemId.NO_REACTORSYSTEM_ID; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}
