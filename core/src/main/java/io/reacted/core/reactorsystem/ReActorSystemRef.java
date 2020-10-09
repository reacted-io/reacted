/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.reacted.core.drivers.system.NullDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.patterns.Try;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

/**
 * ReActorSystem abstraction. Through this interface we can perform operations on the contained reactors,
 * regardless if they are local or remote
 */
public class ReActorSystemRef implements Externalizable {
    private static final long serialVersionUID = 1;
    private static final long REACTORSYSTEM_ID_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                                "reActorSystemId")
                                                                          .orElseSneakyThrow();
    private static final long BACKING_DRIVER_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                              "backingDriver") // reactor system driver
                                                                        .orElseSneakyThrow();
    private static final long GATE_PROPERTIES_OFFSET = SerializationUtils.getFieldOffset(ReActorSystemRef.class,
                                                                               "gateProperties") // reactor system driver
                                                                         .orElseSneakyThrow();

    private final ReActorSystemId reActorSystemId;
    private final transient ReActorSystemDriver backingDriver;
    private final transient Properties gateProperties;

    public ReActorSystemRef() {
        this.reActorSystemId = null;
        this.backingDriver = null;
        this.gateProperties = null;
    }

    public ReActorSystemRef(ReActorSystemDriver backingDriver, Properties gateProperties,
                            ReActorSystemId reActorSystemId) {
        this.backingDriver = backingDriver;
        this.reActorSystemId = reActorSystemId;
        this.gateProperties = gateProperties;
    }

    <PayloadT extends Serializable>
    CompletionStage<Try<DeliveryStatus>> tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                              PayloadT message) {
        return this.backingDriver.tell(src, dst, ackingPolicy, message);
    }

    public void stop(ReActorId dst) {
        this.backingDriver.stop(dst);
    }

    public ReActorSystemId getReActorSystemId() {
        return this.reActorSystemId;
    }

    @JsonIgnore
    public ReActorSystemDriver getBackingDriver() { return this.backingDriver; }

    @JsonIgnore
    public Properties getGateProperties() { return this.gateProperties; }

    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof ReActorSystemRef)) return false;
        return getReActorSystemId().equals(((ReActorSystemRef) o).getReActorSystemId()) &&
               getGateProperties().equals(((ReActorSystemRef) o).getGateProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(reActorSystemId, gateProperties);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Objects.requireNonNull(reActorSystemId).writeExternal(out);
    }

    @Override
    public String toString() {
        return "ReActorSystemRef{" + "reActorSystemId=" + reActorSystemId + ", backingDriver=" + backingDriver + ", " +
               "gateProperties=" + gateProperties + '}';
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorSystemId reActorSystemId = new ReActorSystemId();
        reActorSystemId.readExternal(in);
        setReActorSystemId(reActorSystemId);
        RemotingDriver.getDriverCtx()
                      .flatMap(driverCtx -> driverCtx.getLocalReActorSystem().findGate(reActorSystemId,
                                                                                       driverCtx.getDecodingDriver()
                                                                                                .getChannelId()))
                      .ifPresentOrElse(gateRoute -> setBackingDriver(gateRoute.getBackingDriver())
                                                            .setGateProperties(gateRoute.getGateProperties()),
                                       //We do not know how to communicate with the target reactor system, so
                                       //let's use the Null Driver that provides always failing implementations
                                       () -> setBackingDriver(NullDriver.NULL_DRIVER)
                                                .setGateProperties(NullDriver.NULL_DRIVER_PROPERTIES));
    }

    @SuppressWarnings("UnusedReturnValue")
    private ReActorSystemRef setReActorSystemId(ReActorSystemId reActorSystemId) {
        return SerializationUtils.setObjectField(this, REACTORSYSTEM_ID_OFFSET, reActorSystemId);
    }

    public ReActorSystemRef setBackingDriver(ReActorSystemDriver gateDriver) {
        return SerializationUtils.setObjectField(this, BACKING_DRIVER_OFFSET, gateDriver);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ReActorSystemRef setGateProperties(Properties gateProperties) {
        return SerializationUtils.setObjectField(this, GATE_PROPERTIES_OFFSET, gateProperties);
    }
}
