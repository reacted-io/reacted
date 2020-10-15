/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverCfg;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class LoopbackDriver<CfgT extends ChannelDriverCfg<?, CfgT>> extends ReActorSystemDriver<CfgT> {
    private final LocalDriver<CfgT> localDriver;
    private final ReActorSystem localReActorSystem;

    public LoopbackDriver(ReActorSystem reActorSystem, LocalDriver<CfgT> localDriver) {
        super(localDriver.getDriverConfig());
        this.localDriver = Objects.requireNonNull(localDriver);
        this.localReActorSystem = Objects.requireNonNull(reActorSystem);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
    tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT payload) {
        ReActorContext dstCtx = localReActorSystem.getNullableReActorCtx(dst.getReActorId());
        CompletionStage<Try<DeliveryStatus>> tellResult;
        boolean isAckRequired = isAckRequired(localDriver.channelRequiresDeliveryAck(), ackingPolicy);
        long seqNum = localReActorSystem.getNewSeqNum();
        if (dstCtx != null) {
            var pendingAck = isAckRequired
                             ? localDriver.newPendingAckTrigger(seqNum)
                             : null;
            tellResult = localDriver.sendAsyncMessage(dstCtx, new Message(src, dstCtx.getSelf(), seqNum,
                                                                          localReActorSystem.getLocalReActorSystemId(),
                                                                          ackingPolicy, payload));
            if (isAckRequired) {
                tellResult.thenAccept(deliveryStatusTry -> localDriver.removePendingAckTrigger(seqNum)
                                                                      .ifPresent(deliveryAttempt -> deliveryAttempt.toCompletableFuture()
                                                                                                                   .complete(deliveryStatusTry)));
                tellResult = pendingAck;
            }

            propagateMessage(dst.getReActorId(), payload, src);
        } else {
            tellResult = CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.NOT_DELIVERED));
            if (!dst.equals(localReActorSystem.getSystemDeadLetters())) {
                localReActorSystem.getSystemDeadLetters().tell(src, new DeadMessage(payload));
            } else {
                LOGGER.error("Critic! Deadletters not found!? Source {} Destination {} Message {}",
                             src, dst, payload);
                localReActorSystem.logError("Critic! Deadletters not found! Source {} Destination {} " +
                                            "Message {}", src, dst, payload, new RuntimeException());
            }
        }
        return tellResult;
    }

    @Override
    public ReActorSystem getLocalReActorSystem() { return localReActorSystem; }

    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        return localDriver.initDriverCtx(localReActorSystem);
    }

    @Override
    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        return localDriver.stopDriverCtx(reActorSystem);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) throws Exception {
        localDriver.initDriverLoop(localReActorSystem);
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return localDriver.getDriverLoop();
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() { return localDriver.cleanDriverLoop(); }

    @Override
    public final ChannelId getChannelId() { return localDriver.getChannelId(); }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        return Try.ofFailure(new UnsupportedOperationException());
    }

    @Override
    public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
        return CompletableFuture.completedFuture(Try.ofFailure(new UnsupportedOperationException()));
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return localDriver.channelRequiresDeliveryAck(); }

    @Override
    public Properties getChannelProperties() { return localDriver.getChannelProperties(); }

    private void propagateMessage(ReActorId originalDst, Serializable msgPayload, ReActorRef src) {
        var subscribers = localReActorSystem.getTypedSubscribers().get(msgPayload.getClass(),
                                                                       TypedSubscriptionPolicy.LOCAL);
        if (!subscribers.isEmpty()) {
            localReActorSystem.getMsgFanOutPool()
                              .submit(() -> propagateToSubscribers(localDriver, subscribers, originalDst,
                                                                   localReActorSystem, src, msgPayload));
        }
    }

    private void propagateToSubscribers(LocalDriver<CfgT> localDriver, Collection<ReActorContext> subscribers,
                                        ReActorId originalDestination, ReActorSystem localReActorSystem,
                                        ReActorRef src, Serializable payload) {
        subscribers.stream()
                   .filter(reActorCtx -> !reActorCtx.getSelf().getReActorId().equals(originalDestination))
                   .forEach(dstCtx -> localDriver.sendMessage(dstCtx,
                                                              new Message(src, dstCtx.getSelf(),
                                                                          localReActorSystem.getNewSeqNum(),
                                                                          localReActorSystem.getLocalReActorSystemId(),
                                                                          AckingPolicy.NONE, payload)));
    }
}
