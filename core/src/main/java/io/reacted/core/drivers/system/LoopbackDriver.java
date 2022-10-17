/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;

@NonNullByDefault
public class LoopbackDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>> extends ReActorSystemDriver<ConfigT> {
    private static final TriConsumer<ReActorId, Serializable, ReActorRef> DO_NOT_PROPAGATE = (a, b, c) -> { };
    private final TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers = this::propagateMessage;
    private final LocalDriver<ConfigT> localDriver;
    private final ReActorSystem localReActorSystem;

    public LoopbackDriver(ReActorSystem reActorSystem, LocalDriver<ConfigT> localDriver) {
        super(localDriver.getDriverConfig());
        this.localDriver = Objects.requireNonNull(localDriver,
                                                  "Local driver cannot be null");
        this.localReActorSystem = Objects.requireNonNull(reActorSystem,
                                                         "ReActorSystem cannot be null");
    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus
    tell(ReActorRef src, ReActorRef dst, PayloadT payload) {
        return tell(src, dst, propagateToSubscribers, payload);
    }
    @Override
    public <PayloadT extends Serializable> DeliveryStatus
    tell(ReActorRef src, ReActorRef dst,
         TriConsumer<ReActorId, Serializable, ReActorRef> toSubscribers, PayloadT payload){
        ReActorContext dstCtx = localReActorSystem.getReActorCtx(dst.getReActorId());
        DeliveryStatus tellResult;
        long seqNum = localReActorSystem.getNewSeqNum();
        if (dstCtx != null) {

            tellResult = localDriver.sendMessage(dstCtx, new Message(src, dstCtx.getSelf(), seqNum,
                                                                     localReActorSystem.getLocalReActorSystemId(),
                                                                     AckingPolicy.NONE, payload));
            toSubscribers.accept(dst.getReActorId(), payload, src);

        } else {
            tellResult = DeliveryStatus.NOT_SENT;

            if (localReActorSystem.isSystemDeadLetters(dst)) {
                //if here we are trying to deliver a message to deadletter because we did not find deadletter
                LOGGER.error("Critic! Deadletters not found!? Source {} Destination {} Message {}",
                             src, dst, payload);
            } else {
                localReActorSystem.toDeadLetters(src, payload);
            }
        }
        return tellResult;
    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus
    route(ReActorRef src, ReActorRef dst, PayloadT payload) {
        return tell(src, dst, DO_NOT_PROPAGATE, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT payload) {
        return atell(src, dst, ackingPolicy, propagateToSubscribers, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    aroute(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT payload) {
        return atell(src, dst, ackingPolicy, DO_NOT_PROPAGATE, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
          TriConsumer<ReActorId, Serializable, ReActorRef> toSubscribers, PayloadT payload) {
        ReActorContext dstCtx = localReActorSystem.getReActorCtx(dst.getReActorId());
        CompletionStage<DeliveryStatus> tellResult;
        long seqNum = localReActorSystem.getNewSeqNum();
        if (dstCtx != null) {
            Message messageToSend = new Message(src, dstCtx.getSelf(), seqNum,
                                                localReActorSystem.getLocalReActorSystemId(),
                                                ackingPolicy, payload);
            if (ackingPolicy.isAckRequired()) {
                tellResult = localDriver.sendAsyncMessage(dstCtx, messageToSend);

            } else {
                tellResult = DELIVERY_RESULT_CACHE[localDriver.sendMessage(dstCtx, messageToSend).ordinal()];
            }

            toSubscribers.accept(dst.getReActorId(), payload, src);

        } else {
            tellResult = DELIVERY_RESULT_CACHE[DeliveryStatus.NOT_DELIVERED.ordinal()];
            if (localReActorSystem.isSystemDeadLetters(dst)) {
                //if here we are trying to deliver a message to deadletter because we did not find deadletter
                LOGGER.error("Critic! Deadletters not found!? Source {} Destination {} Message {}",
                             src, dst, payload);
            } else {
                localReActorSystem.toDeadLetters(src, payload);
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
    public DeliveryStatus sendMessage(ReActorContext destination, Message message) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Properties getChannelProperties() { return localDriver.getChannelProperties(); }

    private void propagateMessage(ReActorId originalDst, Serializable msgPayload, ReActorRef src) {
        var subscribers = localReActorSystem.getTypedSubscriptionsManager()
                                            .getLocalSubscribers(msgPayload.getClass());
        if (!subscribers.isEmpty()) {
            try {
                localReActorSystem.getMsgFanOutPool()
                                  .submit(() -> propagateToSubscribers(localDriver, subscribers, originalDst,
                                                                       localReActorSystem, src, msgPayload));
            } catch (RejectedExecutionException propagationFailed) {
                localReActorSystem.logError("Error propagating {} towards subscribers",
                                            msgPayload, propagationFailed);
            }
        }
    }

    private void propagateToSubscribers(LocalDriver<ConfigT> localDriver, List<ReActorContext> subscribers,
                                        ReActorId originalDestination, ReActorSystem localReActorSystem,
                                        ReActorRef src, Serializable payload) {
        for (ReActorContext ctx : subscribers) {
            if (!ctx.getSelf().getReActorId().equals(originalDestination)) {
                localDriver.sendMessage(ctx, new Message(src, ctx.getSelf(),
                                                         localReActorSystem.getNewSeqNum(),
                                                         localReActorSystem.getLocalReActorSystemId(),
                                                         AckingPolicy.NONE, payload));
            }
        }
    }
}
