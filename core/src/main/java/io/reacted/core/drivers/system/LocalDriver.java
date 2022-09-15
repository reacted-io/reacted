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
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked.TriConsumer;

import java.io.Serializable;
import java.util.NoSuchElementException;
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
     public boolean channelRequiresDeliveryAck() { return false; }

     @Override
     public final <PayloadT extends Serializable>
     CompletionStage<Try<DeliveryStatus>> tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                               PayloadT message) {
          return CompletableFuture.completedFuture(Try.ofFailure(new UnsupportedOperationException()));
     }

     @Override
     public final <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
     tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
          TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
          return CompletableFuture.completedFuture(Try.ofFailure(new UnsupportedOperationException()));
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
     route(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
          return CompletableFuture.completedFuture(Try.ofFailure(new UnsupportedOperationException()));
     }

     @Override
     protected final void offerMessage(Message message) {
          Objects.requireNonNull(message, "Cannot offer() a null message");
          ReActorId destinationId = message.getDestination().getReActorId();
          ReActorContext destinationCtx = getLocalReActorSystem().getReActorCtx(destinationId);
          CompletionStage<Try<DeliveryStatus>> messageDeliveryAttempt = null;
          if (destinationCtx != null) {
               messageDeliveryAttempt = forwardMessageToLocalActor(destinationCtx, message);
          }
          var ackTrigger = removePendingAckTrigger(message.getSequenceNumber());

          if (messageDeliveryAttempt != null) {
               if (ackTrigger != null) {
                    messageDeliveryAttempt.thenAccept(status -> ackTrigger.toCompletableFuture().complete(status));
               }
          } else {
               if(ackTrigger != null) {
                    ackTrigger.toCompletableFuture()
                              .complete(Try.ofFailure(new NoSuchElementException()));
               }
               propagateToDeadLetters(getLocalReActorSystem().getSystemDeadLetters(), message);
          }
     }

     protected static CompletionStage<Try<DeliveryStatus>> forwardMessageToLocalActor(ReActorContext destination,
                                                                                      Message message) {
          return SystemLocalDrivers.DIRECT_COMMUNICATION
                                   .sendAsyncMessage(destination,
                                                     Objects.requireNonNull(message,
                                                                            "Cannot forward a null message"));
     }

     protected static DeliveryStatus localDeliver(ReActorContext destination, Message message) {
          DeliveryStatus deliverOperation = destination.getMbox().deliver(message);
          rescheduleIfSuccess(deliverOperation, destination);
          return deliverOperation;
     }

     protected static CompletionStage<Try<DeliveryStatus>> asyncLocalDeliver(ReActorContext destination,
                                                                             Message message) {
          var asyncDeliverResult = destination.getMbox().asyncDeliver(message);
          asyncDeliverResult.thenAccept(result -> rescheduleIfSuccess(result.orElse(DeliveryStatus.NOT_DELIVERED), destination));
          return asyncDeliverResult;
     }

     protected static void rescheduleIfSuccess(DeliveryStatus deliveryStatus, ReActorContext destination) {
          if (deliveryStatus == DeliveryStatus.DELIVERED) {
               destination.reschedule();
          }
     }

     private static void propagateToDeadLetters(ReActorRef systemDeadLetters, Message originalMessage) {
          systemDeadLetters.tell(originalMessage.getSender(), new DeadMessage(originalMessage.getPayload()));
     }
}
