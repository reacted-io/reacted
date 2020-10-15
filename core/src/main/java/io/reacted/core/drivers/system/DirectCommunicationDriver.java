/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class DirectCommunicationDriver extends LocalDriver<DirectCommunicationCfg> {
    private final ChannelId channelId;
    public DirectCommunicationDriver(DirectCommunicationCfg cfg) {
        super(cfg);
        this.channelId = new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION, cfg.getChannelName());
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
    public void initDriverLoop(ReActorSystem localReActorSystem) { }

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
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        return destination.isStop()
               ? Try.ofSuccess(DeliveryStatus.NOT_DELIVERED)
               : localDeliver(destination, message);
    }

    @Override
    public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
        return destination.isStop()
               ? CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.NOT_DELIVERED))
               : asyncLocalDeliver(destination, message);
    }
}
