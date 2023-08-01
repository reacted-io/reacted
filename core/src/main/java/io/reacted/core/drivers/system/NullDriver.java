/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.NullDriverConfig;
import io.reacted.core.exceptions.NoRouteToReActorSystem;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Recyclable;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class NullDriver extends ReActorSystemDriver<NullDriverConfig> {
    public static final NullDriver NULL_DRIVER = new NullDriver(NullDriverConfig.newBuilder()
                                                                                .build());
    public static final Properties NULL_DRIVER_PROPERTIES = new Properties();
    private final ChannelId channelId;
    @SuppressWarnings("NotNullFieldNotInitialized")
    private volatile ReActorSystem localReActorSystem;
    private NullDriver(NullDriverConfig config) {
        super(config);
        this.channelId = ChannelId.NO_CHANNEL_ID;
    }
    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        this.localReActorSystem = Objects.requireNonNull(localReActorSystem,
                                                         "Local reactor system cannot be null");
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
    public <PayloadT extends ReActedMessage> DeliveryStatus publish(ReActorRef src, ReActorRef dst,
                                                                  PayloadT message) {
        return DeliveryStatus.NOT_DELIVERED;
    }

    @Override
    public <PayloadT extends ReActedMessage> DeliveryStatus publish(ReActorRef src, ReActorRef dst,
                                                                  @Nullable TriConsumer<ReActorId, ReActedMessage, ReActorRef> propagateToSubscribers, PayloadT message) {
        return DeliveryStatus.NOT_DELIVERED;
    }

    @Override
    public <PayloadT extends ReActedMessage> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
        if (message instanceof Recyclable recyclable) {
            recyclable.revalidate();
        }
        return DeliveryStatus.NOT_DELIVERED;
    }

    @Override
    public <PayloadT extends ReActedMessage>
    CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                             PayloadT message) {
        return CompletableFuture.failedStage(new NoRouteToReActorSystem());
    }

    @Override
    public <PayloadT extends ReActedMessage> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                                                                    TriConsumer<ReActorId, ReActedMessage, ReActorRef> propagateToSubscribers,
                                                                                    PayloadT message) {
        return CompletableFuture.failedStage(new NoRouteToReActorSystem());
    }

    @Override
    public <PayloadT extends ReActedMessage> CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
        return CompletableFuture.failedStage(new NoRouteToReActorSystem());
    }

    @Override
    public <PayloadT extends ReActedMessage> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx,
                ReActorRef destination, long seqNum, ReActorSystemId reActorSystemId,
                AckingPolicy ackingPolicy, PayloadT message) {
        throw new NoRouteToReActorSystem();
    }
    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Properties getChannelProperties() { return NULL_DRIVER_PROPERTIES; }
}
