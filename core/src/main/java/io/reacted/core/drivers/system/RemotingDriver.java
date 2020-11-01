/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public abstract class RemotingDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>>
        extends ReActorSystemDriver<ConfigT> {

    protected RemotingDriver(ConfigT config) { super(config); }

    @Override
    public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
        return CompletableFuture.completedFuture(sendMessage(destination, message));
    }

    /**
     * Sends a message over a remoting channel
     *
     * @param src          source of the message
     * @param dst          destination of the message
     * @param ackingPolicy Specify if or how we should receive an ACK from the remote reactor system for this message
     * @param message      payload
     * @return A completablefuture that is going to be completed with the outcome of the operation once the message
     * has been delivered into the target's mailbox and has been ACK-ed accordingly with the specified policy
     */
    @Override
    public <PayloadT extends Serializable>
    CompletionStage<Try<DeliveryStatus>> tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                              PayloadT message) {
        long nextSeqNum = getLocalReActorSystem().getNewSeqNum();
        boolean isAckRequired = isAckRequired(channelRequiresDeliveryAck(), ackingPolicy);
        var pendingAck = isAckRequired ? newPendingAckTrigger(nextSeqNum) : null;
        var sendResult = sendMessage(ReActorContext.NO_REACTOR_CTX,
                                     new Message(src, dst, nextSeqNum,
                                                 getLocalReActorSystem().getLocalReActorSystemId(),
                                                 ackingPolicy, message));
        CompletionStage<Try<DeliveryStatus>> tellResult;
        if (isAckRequired) {
            tellResult = sendResult.filter(DeliveryStatus::isDelivered, DeliveryException::new)
                                   .map(success -> pendingAck)
                                   .peekFailure(error -> removePendingAckTrigger(nextSeqNum))
                                   .orElseGet(() -> CompletableFuture.completedFuture(sendResult));
        } else {
            tellResult = CompletableFuture.completedFuture(sendResult);
        }
        return tellResult;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getChannelId(), getChannelProperties());
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemotingDriver<?> that = (RemotingDriver<?>) o;
        return getChannelId().equals(that.getChannelId()) &&
               getChannelProperties().equals(that.getChannelProperties());
    }

    /**
     * Forward a message just received by a channel to ReActed for propagating it towards the destination mailbox
     */
    protected void offerMessage(Message message) {
        //We don't have to read the messages published by the local reactor system because they are meant for someone
        //else. This is a remoting driver, this means that several systems are looking at it
        //An example of this scenario might be when producer/consumer reactor systems communicate using the same shared
        //bus (i.e. a chronicle queue file)
        if (isMessageComingFromLocalReActorSystem(getLocalReActorSystem().getLocalReActorSystemId(),
                                                  message.getDataLink())) {
            return;
        }

        ReActorRef sender = message.getSender();
        ReActorRef destination = message.getDestination();
        Serializable payload = message.getPayload();
        Class<? extends Serializable> payloadType = payload.getClass();
        boolean hasBeenSniffed = false;

        //If the message destination is within reactor system. We might have just intercepted a message for some other
        //reactor system is the source channel is a 1:N channel such as a kafka topic
        if (isLocalReActorSystem(getLocalReActorSystem().getLocalReActorSystemId(),
                                 destination.getReActorSystemRef().getReActorSystemId())) {
            //If so, this is an ACK confirmation for a message sent with atell
            if (payloadType == DeliveryStatusUpdate.class) {
                DeliveryStatusUpdate deliveryStatusUpdate = message.getPayload();
                removePendingAckTrigger(deliveryStatusUpdate.getMsgSeqNum())
                        .ifPresent(pendingAckTrigger -> pendingAckTrigger.toCompletableFuture()
                                                                         .complete(Try.ofSuccess(deliveryStatusUpdate.getDeliveryStatus())));
                //This is functionally useless because systemSink by design swallows received messages, it is required
                //only for consistent logging if a logging direct communication local driver is used because in this way
                //also the ACK will appear in logs
                getLocalReActorSystem().getSystemSink()
                                       .tell(message.getSender(), message.getPayload());
                return;
            }
        } else {
            //If it was not meant for a ReActor within this reactor system it might still be of some interest for typed
            //subscribers
            if (!isTypeSubscribed(getLocalReActorSystem(), payloadType)) {
                return;
            }
            //Mark the sink as destination. Once the message has been sent within the main flow, it will be
            //automatically propagated toward the subscribers by the loopback driver tell implementation
            destination = getLocalReActorSystem().getSystemSink();
            hasBeenSniffed = true;
        }
        boolean isAckRequired = !hasBeenSniffed &&
                                message.getDataLink().getAckingPolicy() != AckingPolicy.NONE;
        var deliverAttempt = (isAckRequired ? destination.atell(sender, payload) : destination.tell(sender, payload));
        if (isAckRequired) {
            deliverAttempt.thenAccept(deliveryResult -> sendDeliveryAck(getLocalReActorSystem().getLocalReActorSystemId(),
                                                                        getLocalReActorSystem().getNewSeqNum(), this,
                                                                        deliveryResult, message)
                                                        .ifError(error -> getLocalReActorSystem()
                                                                            .logError("Unable to send ack", error)));
        }
    }
    private static boolean isTypeSubscribed(ReActorSystem localReActorSystem,
                                            Class<? extends Serializable> payloadType) {
        var subscribersGroup = localReActorSystem.getTypedSubscribers().getKeyGroup(payloadType);
        return !subscribersGroup.get(TypedSubscription.REMOTE).isEmpty();
    }
}
