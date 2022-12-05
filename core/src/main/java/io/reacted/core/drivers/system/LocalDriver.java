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
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.UnChecked.TriConsumer;

import javax.annotation.Nullable;
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
     DeliveryStatus publish(ReActorRef src, ReActorRef dst, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public final <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef src, ReActorRef dst,
                                                                         @Nullable TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public <PayloadT extends Serializable> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message) {
          throw new UnsupportedOperationException();
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                                                                     TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     public <PayloadT extends Serializable> CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message) {
          return CompletableFuture.failedStage(new UnsupportedOperationException());
     }

     @Override
     protected final <PayloadT extends Serializable> void
     offerMessage(ReActorRef source, ReActorRef destination, long sequenceNumber, ReActorSystemId fromReActorSystemId,
                  AckingPolicy ackingPolicy, PayloadT payloadT) {
          ReActorId destinationId = destination.getReActorId();
          ReActorContext destinationCtx = getLocalReActorSystem().getReActorCtx(destinationId);
          DeliveryStatus deliveryStatus;

          if (destinationCtx != null) {
               deliveryStatus = syncForwardMessageToLocalActor(destinationCtx, message);
          } else {
               deliveryStatus = DeliveryStatus.NOT_DELIVERED;
               getLocalReActorSystem().toDeadLetters(message);
          }

          if (ackingPolicy.isAckRequired()) {
               CompletionStage<DeliveryStatus> ackTrigger;
               ackTrigger = removePendingAckTrigger(sequenceNumber);

               if (ackTrigger != null) {
                    ackTrigger.toCompletableFuture().complete(deliveryStatus);
               }
          }
     }

     protected static <PayloadT extends Serializable> DeliveryStatus
     syncForwardMessageToLocalActor(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                                    long sequenceNumber, ReActorSystemId fromReActorSystemId, AckingPolicy ackingPolicy,
                                    PayloadT payload) {
          return SystemLocalDrivers.DIRECT_COMMUNICATION
                                   .sendMessage(source, destinationCtx, destination, sequenceNumber,
                                                fromReActorSystemId, ackingPolicy,
                                                Objects.requireNonNull(payload, "Cannot forward a null message"));
     }
     protected static DeliveryStatus localDeliver(ReActorContext destination, Message message) {
          DeliveryStatus deliverOperation = destination.getMbox().deliver(message);
          if (deliverOperation.isDelivered()) {
               destination.reschedule();
          }
          return deliverOperation;
     }
}
