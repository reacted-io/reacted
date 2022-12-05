/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.DirectCommunicationConfig;
import io.reacted.core.drivers.system.LocalDriver;
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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class DirectCommunicationDriver extends LocalDriver<DirectCommunicationConfig> {
    private final ChannelId channelId;
    public DirectCommunicationDriver(DirectCommunicationConfig config) {
        super(config);
        this.channelId = ChannelId.ChannelType.DIRECT_COMMUNICATION.forChannelName(config.getChannelName());
    }

    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        return Try.VOID;
    }

    @Override
    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        return CompletableFuture.completedFuture(Try.VOID);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) { /* Direct driver does not need an inited context */ }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() { return CompletableFuture.completedFuture(Try.VOID); }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() { return () -> {}; }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Properties getChannelProperties() {
        return new Properties();
    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                long seqNum, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        return destinationCtx.isStop()
               ? DeliveryStatus.NOT_DELIVERED
               : localDeliver(destinationCtx, new Message(source, destination, seqNum, reActorSystemId,
                                                          ackingPolicy, message));
    }
}
