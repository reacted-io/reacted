/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.NullLocalDriverConfig;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class NullLocalDriver extends LocalDriver<NullLocalDriverConfig> {
    public static final Properties NULL_LOCAL_DRIVER_PROPERTIES = new Properties();
    private final ChannelId channelId;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private volatile ReActorSystem localReActorSystem;
    public NullLocalDriver(NullLocalDriverConfig config) {
        super(config);
        this.channelId = ChannelId.NO_CHANNEL_ID;
    }
    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        this.localReActorSystem = localReActorSystem;
        return Try.VOID;
    }

    @Override
    public ReActorSystem getLocalReActorSystem() {
        //If this is null it means that someone is trying to use the driver
        //before its initialization
        return Objects.requireNonNull(localReActorSystem,
                                      "Null local reactor system?");
    }

    @Override
    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) { return cleanDriverLoop(); }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() { return CompletableFuture.completedFuture(Try.VOID);  }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> {};
    }


    @Override
    public <PayloadT extends Serializable> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
        return DeliveryStatus.NOT_DELIVERED;
    }
    @Override
    public <PayloadT extends Serializable>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long seqNum, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy,
                               PayloadT message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Properties getChannelProperties() { return NULL_LOCAL_DRIVER_PROPERTIES; }
}
