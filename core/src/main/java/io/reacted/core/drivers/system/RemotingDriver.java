/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked.TriConsumer;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;

@NonNullByDefault
public abstract class RemotingDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>>
        extends ReActorSystemDriver<ConfigT> {
    protected RemotingDriver(ConfigT config) { super(config); }

    @Override
    public final <PayloadT extends Serializable>
    DeliveryStatus publish(ReActorRef src, ReActorRef dst, PayloadT message) {
        //While sending towards a remote peer, propagation towards subscribers never takes place
        return publish(src, dst, DO_NOT_PROPAGATE, message);
    }

    @Override
    public final <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef source, ReActorRef destination,
                                                                        @Nullable TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
        long nextSeqNum = getLocalReActorSystem().getNewSeqNum();
        return sendMessage(source, ReActorContext.NO_REACTOR_CTX, destination, nextSeqNum,
                           getLocalReActorSystem().getLocalReActorSystemId(), AckingPolicy.NONE, message);
    }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
        return publish(src, dst, DO_NOT_PROPAGATE, message);
    }

    /**
     * Sends a message over a remoting channel
     *
     * @param source          source of the message
     * @param destination          destination of the message
     * @param ackingPolicy Specify if or how we should receive an ACK from the remote reactor system for this message
     * @param message      payload
     * @return A {@link CompletableFuture} that is going to be completed with the outcome of the operation once the message
     * has been delivered into the target's mailbox and has been ACK-ed accordingly with the specified policy
     */
    @Override
    public <PayloadT extends Serializable>
    CompletionStage<DeliveryStatus> apublish(ReActorRef source, ReActorRef destination, AckingPolicy ackingPolicy,
                                             PayloadT message) {
        long nextSeqNum = getLocalReActorSystem().getNewSeqNum();
        var pendingAck = ackingPolicy.isAckRequired() ? newPendingAckTrigger(nextSeqNum) : null;
        DeliveryStatus sendResult = sendMessage(source, ReActorContext.NO_REACTOR_CTX,
                                                destination, nextSeqNum,
                                                getLocalReActorSystem().getLocalReActorSystemId(),
                                                ackingPolicy, message);
        CompletionStage<DeliveryStatus> tellResult = DELIVERY_RESULT_CACHE[sendResult.ordinal()];
        if (ackingPolicy.isAckRequired()) {
            if (sendResult.isSent()) {
                tellResult = pendingAck;
            } else {
                removePendingAckTrigger(nextSeqNum);
            }
        }
        return tellResult;
    }

    @Override
    public final <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                                                                          TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
        //While sending towards a remote peer, propagation towards subscribers never takes place
        return apublish(src, dst, ackingPolicy, message);
    }

    @Override
    public final <PayloadT extends Serializable> CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
        //While sending towards a remote peer, propagation towards subscribers never takes place,
        //so tell and publish behave in the same way
        return apublish(src, dst, ackingPolicy, message);
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
    @Override
    protected <PayloadT extends Serializable> void offerMessage(ReActorRef source, ReActorRef destination,
                                                                long sequenceNumber,
                                                                ReActorSystemId fromReActorSystemId,
                                                                AckingPolicy ackingPolicy,
                                                                PayloadT payload) {
        //We don't have to read the messages published by the local reactor system because they are meant for someone
        //else. This is a remoting driver, this means that several systems are looking at it
        //An example of this scenario might be when producer/consumer reactor systems communicate using the same shared
        //bus (i.e. a chronicle queue file)
        if (isMessageComingFromLocalReActorSystem(getLocalReActorSystem().getLocalReActorSystemId(),
                                                  fromReActorSystemId)) {
            return;
        }

        Class<? extends Serializable> payloadType = payload.getClass();
        boolean hasBeenSniffed = false;

        //If the message destination is within reactor system. We might have just intercepted a message for some other
        //reactor system is the source channel is a 1:N channel such as a kafka topic
        if (isLocalReActorSystem(getLocalReActorSystem().getLocalReActorSystemId(),
                                 destination.getReActorSystemRef().getReActorSystemId())) {
            //If so, this is an ACK confirmation for a message sent with apublish
            if (payloadType == DeliveryStatusUpdate.class) {
                DeliveryStatusUpdate deliveryStatusUpdate = (DeliveryStatusUpdate)payload;

                if (messageWasNotSentFromThisDriverInstance(deliveryStatusUpdate)) {
                    /* We are not in the correct driver? This is an asymmetrical ACK, we must forward
                       this message to the proper driver, if any
                     */
                    forwardMessageToSenderDriverInstance(source, destination, sequenceNumber, fromReActorSystemId,
                                                         ackingPolicy, payload, deliveryStatusUpdate);
                } else {
                    var pendingAckTrigger = removePendingAckTrigger(
                        deliveryStatusUpdate.getMsgSeqNum());
                    if (pendingAckTrigger != null) {
                        pendingAckTrigger.toCompletableFuture()
                                         .complete(deliveryStatusUpdate.getDeliveryStatus());
                    }
                    //This is functionally useless because systemSink by design swallows received messages, it is required
                    //only for consistent logging if a logging direct communication local driver is used because in this way
                    //also the ACK will appear in logs
                    getLocalReActorSystem().getSystemSink()
                                           .publish(source, payload);
                }
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
        boolean isAckRequired = !hasBeenSniffed && ackingPolicy != AckingPolicy.NONE;
        if (isAckRequired) {
            //Be better java
            var dst = destination;
            var deliverAttempt = destination.apublish(source, payload);

            deliverAttempt.handle((deliveryStatus, deliveryError) -> {
                              DeliveryStatus result = deliveryStatus;
                              if (deliveryError != null) {
                                  result = DeliveryStatus.NOT_DELIVERED;
                                  getLocalReActorSystem().logInfo("Unable to deliver {} {} {} {} {} {}: Reason {}",
                                                                  source, dst, sequenceNumber, fromReActorSystemId,
                                                                  ackingPolicy, payload, deliveryError);
                              }
                              return sendDeliveryAck(getLocalReActorSystem(), getChannelId(), result, sequenceNumber,
                                                     fromReActorSystemId);
                          })
                          .handle((ackDeliveryStatus, ackDeliveryError) -> {
                              if (ackDeliveryError != null || ackDeliveryStatus.isNotSent()) {
                                  getLocalReActorSystem().logError("Unable to send ack for {} {} {} {} {} {}",
                                                                   source, dst, sequenceNumber, fromReActorSystemId,
                                                                   ackingPolicy, payload, ackDeliveryError);
                              }
                              return null;
                          });
        } else {
            var deliveryAttempt = destination.publish(source, payload);
            if (!deliveryAttempt.isSent()) {
                getLocalReActorSystem().logInfo("Unable to deliver {} {} {} {} {} {}: Reason {}",
                                                source, destination, sequenceNumber, fromReActorSystemId,
                                                ackingPolicy, payload, deliveryAttempt);
            }
        }
    }

    private <PayloadT extends Serializable> void
    forwardMessageToSenderDriverInstance(ReActorRef source, ReActorRef destination, long sequenceNumber,
                                         ReActorSystemId fromReActorSystemId, AckingPolicy ackingPolicy,
                                         PayloadT payload, DeliveryStatusUpdate deliveryStatusUpdate) {
        ReActorSystemRef gateForDestination = getLocalReActorSystem().findGate(deliveryStatusUpdate.getAckSourceReActorSystem(),
                                                                               deliveryStatusUpdate.getFirstMessageSourceChannelId());
        if (gateForDestination != null) {
            gateForDestination.getBackingDriver()
                              .offerMessage(source, destination, sequenceNumber, fromReActorSystemId, ackingPolicy,
                                            payload);
        }
    }

    private boolean messageWasNotSentFromThisDriverInstance(DeliveryStatusUpdate deliveryStatusUpdate) {
        return !getChannelId().equals(deliveryStatusUpdate.getFirstMessageSourceChannelId());
    }

    private static boolean isTypeSubscribed(ReActorSystem localReActorSystem,
                                            Class<? extends Serializable> payloadType) {
        return localReActorSystem.getTypedSubscriptionsManager().hasFullSubscribers(payloadType);
    }
}
