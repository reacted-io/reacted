/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public abstract class LocalDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>>
        extends ReActorSystemDriver<ConfigT> {
     private static final Try<DeliveryStatus> TARGET_MISSING = Try.ofFailure(new NoSuchElementException());

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

     protected void offerMessage(Message message) {
          var deliveryAttempt = getLocalReActorSystem().getReActor(Objects.requireNonNull(message)
                                                                          .getDestination()
                                                                          .getReActorId())
                                                       .map(raCtx -> forwardMessageToLocalActor(raCtx, message));
          var ackTrigger = removePendingAckTrigger(message.getSequenceNumber());

          if (deliveryAttempt.isPresent()) {
               var attemptResult = deliveryAttempt.get();
               attemptResult.thenAccept(status -> ackTrigger.ifPresent(trigger -> trigger.toCompletableFuture()
                                                                                         .complete(status)));

          } else {
               ackTrigger.ifPresent(trigger -> trigger.toCompletableFuture()
                                                      .complete(TARGET_MISSING));
               propagateToDeadLetters(getLocalReActorSystem().getSystemDeadLetters(), message);
          }
     }

     protected static CompletionStage<Try<DeliveryStatus>> forwardMessageToLocalActor(ReActorContext destination,
                                                                                      Message message) {
          return SystemLocalDrivers.DIRECT_COMMUNICATION.sendAsyncMessage(destination, Objects.requireNonNull(message));
     }

     protected static Try<DeliveryStatus> localDeliver(ReActorContext destination, Message message) {
          Try<DeliveryStatus> deliverOperation = Try.of(() -> destination.getMbox().deliver(message));
          rescheduleIfSuccess(deliverOperation, destination);
          return deliverOperation;
     }

     protected static CompletionStage<Try<DeliveryStatus>> asyncLocalDeliver(ReActorContext destination,
                                                                             Message message) {
          var asyncDeliverResult = destination.getMbox().asyncDeliver(message);
          asyncDeliverResult.thenAccept(result -> rescheduleIfSuccess(result, destination));
          return asyncDeliverResult;
     }

     protected static void rescheduleIfSuccess(Try<DeliveryStatus> deliveryResult, ReActorContext destination) {
          deliveryResult.filter(DeliveryStatus::isDelivered, DeliveryException::new)
                        .ifSuccess(deliveryStatus -> destination.reschedule());
     }

     private static void propagateToDeadLetters(ReActorRef systemDeadLetters, Message originalMessage) {
          systemDeadLetters.tell(originalMessage.getSender(), new DeadMessage(originalMessage.getPayload()));
     }
}
