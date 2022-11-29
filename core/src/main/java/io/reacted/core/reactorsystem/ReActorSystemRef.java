/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.NullDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.messages.reactors.DeliveryStatus;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;

/**
 * ReActorSystem abstraction. Through this interface we can perform operations on the contained reactors,
 * regardless if they are local or remote
 */
public class ReActorSystemRef implements Externalizable {
    @Serial
    private static final long serialVersionUID = 1;
    private static final long REACTORSYSTEM_ID_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                                "reActorSystemId")
                                                                          .orElseSneakyThrow();
    private static final long CHANNEL_ID_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                                    "channelId")
                                                                    .orElseSneakyThrow();
    private static final long BACKING_DRIVER_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                              "backingDriver") // reactor system driver
                                                                        .orElseSneakyThrow();
    private static final long GATE_PROPERTIES_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                               "gateProperties") // reactor system driver
                                                                         .orElseSneakyThrow();

    private final ReActorSystemId reActorSystemId;
    private final ChannelId channelId;
    private final transient ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> backingDriver;
    private final transient Properties gateProperties;

    public ReActorSystemRef() {
        this.reActorSystemId = ReActorSystemId.NO_REACTORSYSTEM_ID;
        this.channelId = ChannelId.INVALID_CHANNEL_ID;
        this.backingDriver = NullDriver.NULL_DRIVER;
        this.gateProperties = NullDriver.NULL_DRIVER_PROPERTIES;
    }

    public ReActorSystemRef(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> backingDriver,
                            Properties gateProperties, ReActorSystemId reActorSystemId) {
        this(backingDriver, gateProperties, ChannelId.INVALID_CHANNEL_ID, reActorSystemId);
    }

    public ReActorSystemRef(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> backingDriver,
                            Properties gateProperties, ChannelId channelId, ReActorSystemId reActorSystemId) {
        this.backingDriver = backingDriver;
        this.reActorSystemId = reActorSystemId;
        this.channelId = channelId;
        this.gateProperties = gateProperties;
    }

    <PayloadT extends Serializable>
    DeliveryStatus publish(ReActorRef src, ReActorRef dst, PayloadT message) {
        return backingDriver.publish(src, dst, message);
    }

    <PayloadT extends Serializable>
    DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
        return backingDriver.tell(src, dst, message);
    }

    <PayloadT extends Serializable>
    CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                             PayloadT message) {
        return backingDriver.apublish(src, dst, ackingPolicy, message);
    }

    <PayloadT extends Serializable>
    CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                          PayloadT message) {
        return backingDriver.atell(src, dst, ackingPolicy, message);
    }


    public ReActorSystemId getReActorSystemId() {
        return reActorSystemId;
    }

    @JsonIgnore
    public ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> getBackingDriver() { return backingDriver; }

    @JsonIgnore
    public Properties getGateProperties() { return gateProperties; }

    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof ReActorSystemRef)) return false;
        return reActorSystemId.equals(((ReActorSystemRef) o).getReActorSystemId()) &&
               backingDriver == ((ReActorSystemRef) o).backingDriver &&
               gateProperties.equals(((ReActorSystemRef) o).getGateProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(reActorSystemId, gateProperties);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Objects.requireNonNull(reActorSystemId).writeExternal(out);
        channelId.writeExternal(out);
    }

    @Override
    public String toString() {
        return "ReActorSystemRef{" +
               "reActorSystemId=" + reActorSystemId +
               ", channelId=" + channelId +
               ", backingDriver=" + backingDriver +
               ", gateProperties=" + gateProperties +
               '}';
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorSystemId reActorSystemId = new ReActorSystemId();
        reActorSystemId.readExternal(in);
        setReActorSystemId(reActorSystemId);
        /* The first time that a ReActorSystemRef is deserialized, the channel id is set by the receiver.
           In this way the decoding driver can set the channel id where this message has been received.
           If this message should be sent through a serializing local driver such as chronicle queue and
           deserialized again, since the channel id has been set at the previous step (reception from the remoting driver),
           and it will not be changed, leaving the original reference untouched.
           If the reference should be propagated to other systems, it will always carry the channel id from where it
           came from.
           If a driver should not be found given the specified channel id, the system tries to re-route the reference
           using another channel towards the destination
         */
        ChannelId sourceChannelId = new ChannelId();
        sourceChannelId.readExternal(in);
        DriverCtx driverCtx = ReActorSystemDriver.getDriverCtx();
        if (driverCtx != null) {
            ReActorSystemRef gateForReActor = driverCtx.getLocalReActorSystem()
                                                       .findGate(reActorSystemId, sourceChannelId.equals(ChannelId.INVALID_CHANNEL_ID)
                                                                                  ? driverCtx.getDecodingDriver()
                                                                                             .getChannelId()
                                                                                  : sourceChannelId);
            if (gateForReActor != null) {
                setBackingDriver(gateForReActor.getBackingDriver());
                setGateProperties(gateForReActor.getGateProperties());
            }
        }
        setChannelId(getBackingDriver().getChannelId());
    }
    private void setReActorSystemId(ReActorSystemId reActorSystemId) {
        SerializationUtils.setObjectField(this, REACTORSYSTEM_ID_OFFSET, reActorSystemId);
    }
    private void setChannelId(ChannelId channelId) {
        SerializationUtils.setObjectField(this, CHANNEL_ID_OFFSET, channelId);
    }
    private void setBackingDriver(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> gateDriver) {
        SerializationUtils.setObjectField(this, BACKING_DRIVER_OFFSET, gateDriver);
    }

    private void setGateProperties(Properties gateProperties) {
        SerializationUtils.setObjectField(this, GATE_PROPERTIES_OFFSET, gateProperties);
    }
}
