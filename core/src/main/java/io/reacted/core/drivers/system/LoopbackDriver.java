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
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@NonNullByDefault
public class LoopbackDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>> extends ReActorSystemDriver<ConfigT> {
    private final TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers = this::propagateMessage;
    private final LocalDriver<ConfigT> localDriver;
    private final ReActorSystem localReActorSystem;
    private final ExecutorService fanOutPool;

    public LoopbackDriver(ReActorSystem reActorSystem, LocalDriver<ConfigT> localDriver) {
        super(localDriver.getDriverConfig());
        this.localDriver = Objects.requireNonNull(localDriver,
                                                  "Local driver cannot be null");
        this.localReActorSystem = Objects.requireNonNull(reActorSystem,
                                                         "ReActorSystem cannot be null");
        this.fanOutPool = localReActorSystem.getMsgFanOutPool();

    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef src, ReActorRef dst, PayloadT payload) {
        return publish(src, dst, propagateToSubscribers, payload);
    }
    @Override
    public <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef source, ReActorRef destination,
                                                                  @Nullable TriConsumer<ReActorId, Serializable, ReActorRef> toSubscribers, PayloadT payload){
        ReActorContext dstCtx = localReActorSystem.getReActorCtx(destination.getReActorId());
        DeliveryStatus tellResult;
        long seqNum = localReActorSystem.getNewSeqNum();
        if (dstCtx != null) {

            tellResult = localDriver.sendMessage(source, dstCtx, destination, seqNum, localReActorSystem.getLocalReActorSystemId(),
                                                 AckingPolicy.NONE, payload);
            if (toSubscribers != null) {
                toSubscribers.accept(destination.getReActorId(), payload, source);
            }
        } else {
            tellResult = DeliveryStatus.NOT_SENT;

            if (localReActorSystem.isSystemDeadLetters(destination)) {
                //if here we are trying to deliver a message to deadletter because we did not find deadletter
                LOGGER.error("Critic! Deadletters not found!? Source {} Destination {} Message {}",
                             source, destination, payload);
            } else {
                localReActorSystem.toDeadLetters(source, payload);
            }
        }
        return tellResult;
    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT payload) {
        return publish(src, dst, DO_NOT_PROPAGATE, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT payload) {
        return apublish(src, dst, ackingPolicy, propagateToSubscribers, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT payload) {
        return apublish(src, dst, ackingPolicy, DO_NOT_PROPAGATE, payload);
    }

    @Override
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef source, ReActorRef destnation, AckingPolicy ackingPolicy,
                                                                                    TriConsumer<ReActorId, Serializable, ReActorRef> toSubscribers, PayloadT payload) {
        ReActorContext destinationContext = localReActorSystem.getReActorCtx(destnation.getReActorId());
        CompletionStage<DeliveryStatus> tellResult;
        long seqNum = localReActorSystem.getNewSeqNum();
        if (destinationContext != null) {
            if (ackingPolicy.isAckRequired()) {
                tellResult = localDriver.sendAsyncMessage(source, destinationContext, destnation, seqNum,
                                                          localReActorSystem.getLocalReActorSystemId(),
                                                          ackingPolicy, payload);

            } else {
                tellResult = DELIVERY_RESULT_CACHE[localDriver.sendMessage(source, destinationContext, destnation, seqNum,
                                                                           localReActorSystem.getLocalReActorSystemId(),
                                                                           ackingPolicy, payload).ordinal()];
            }

            toSubscribers.accept(destnation.getReActorId(), payload, source);

        } else {
            tellResult = DELIVERY_RESULT_CACHE[DeliveryStatus.NOT_DELIVERED.ordinal()];
            if (localReActorSystem.isSystemDeadLetters(destnation)) {
                //if here we are trying to deliver a message to deadletter because we did not find deadletter
                LOGGER.error("Critic! Deadletters not found!? Source {} Destination {} Message {}",
                             source, destnation, payload);
            } else {
                localReActorSystem.toDeadLetters(source, payload);
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
    public <PayloadT extends Serializable> DeliveryStatus
    sendMessage(ReActorRef src, ReActorContext destinationCtx, ReActorRef destination, long seqNum,
                ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Properties getChannelProperties() { return localDriver.getChannelProperties(); }

    private void propagateMessage(ReActorId originalDst, Serializable msgPayload, ReActorRef src) {
        var subscribers = localReActorSystem.getTypedSubscriptionsManager()
                                            .getLocalSubscribers(msgPayload.getClass());
        if (!subscribers.isEmpty()) {
            if (subscribers.size() < 6) {
                propagateToSubscribers(localDriver, subscribers, originalDst, localReActorSystem, src, msgPayload);
            } else {
                fanOutPool.execute(() -> propagateToSubscribers(localDriver, subscribers, originalDst,
                                                                localReActorSystem, src, msgPayload));
            }
        }
    }

    private void propagateToSubscribers(LocalDriver<ConfigT> localDriver, List<ReActorContext> subscribers,
                                        ReActorId originalDestination, ReActorSystem localReActorSystem,
                                        ReActorRef source, Serializable payload) {
        for (ReActorContext ctx : subscribers) {
            if (!ctx.getSelf().getReActorId().equals(originalDestination)) {
                localDriver.sendMessage(source, ctx, ctx.getSelf(), localReActorSystem.getNewSeqNum(),
                                        localReActorSystem.getLocalReActorSystemId(), AckingPolicy.NONE, payload);
            }
        }
    }
}
