/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked.TriConsumer;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public abstract class LocalDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>>
        extends ReActorSystemDriver<ConfigT> {
     protected LocalDriver(ConfigT config) {
          super(config);
     }

     @Override
     public final <PayloadT extends Serializable>
     DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public final <PayloadT extends Serializable> DeliveryStatus
     tell(ReActorRef src, ReActorRef dst,
          TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public <PayloadT extends Serializable> DeliveryStatus
     route(ReActorRef src, ReActorRef dst, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
     atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
     atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
           TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
     aroute(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     protected final void offerMessage(Message message) {
          Objects.requireNonNull(message, "Cannot offer() a null message");
          ReActorId destinationId = message.getDestination().getReActorId();
          ReActorContext destinationCtx = getLocalReActorSystem().getReActorCtx(destinationId);
          CompletionStage<DeliveryStatus> asyncMessageDeliveryAttempt = null;

          if (destinationCtx != null) {
               if (message.getDataLink().getAckingPolicy().isAckRequired()) {
                    asyncMessageDeliveryAttempt = asyncForwardMessageToLocalActor(destinationCtx,
                                                                                  message);
               } else {
                    try {
                         syncForwardMessageToLocalActor(destinationCtx, message);
                    } catch (DeliveryException deliveryException) {
                         getLocalReActorSystem().toDeadLetters(message);
                    }
               }
          } else {
               getLocalReActorSystem().toDeadLetters(message);
          }

          if (message.getDataLink().getAckingPolicy().isAckRequired()) {
               CompletionStage<DeliveryStatus> ackTrigger;
               ackTrigger = removePendingAckTrigger(message.getSequenceNumber());

               if (ackTrigger != null) {
                    asyncMessageDeliveryAttempt.handle(((deliveryStatus, error) -> {
                         if (error == null) {
                              ackTrigger.toCompletableFuture().complete(deliveryStatus);
                         } else {
                              ackTrigger.toCompletableFuture().completeExceptionally(error);
                         }
                         if (destinationCtx != null &&
                             deliveryStatus == null || !deliveryStatus.isDelivered()) {
                              getLocalReActorSystem().toDeadLetters(message);
                         }
                         return null;
                    }));
               }
          }
     }

     protected static DeliveryStatus syncForwardMessageToLocalActor(ReActorContext destination,
                                                                    Message message) {
          return SystemLocalDrivers.DIRECT_COMMUNICATION
                                   .sendMessage(destination,
                                                     Objects.requireNonNull(message,
                                                                            "Cannot forward a null message"));
     }

     protected static CompletionStage<DeliveryStatus>
     asyncForwardMessageToLocalActor(ReActorContext destination, Message message) {
          return SystemLocalDrivers.DIRECT_COMMUNICATION
              .sendAsyncMessage(destination,
                           Objects.requireNonNull(message, "Cannot forward a null message"));
     }

     protected static DeliveryStatus localDeliver(ReActorContext destination, Message message) {
          DeliveryStatus deliverOperation = destination.getMbox().deliver(message);
          if (deliverOperation.isDelivered()) {
               destination.reschedule();
          }
          return deliverOperation;
     }
}
