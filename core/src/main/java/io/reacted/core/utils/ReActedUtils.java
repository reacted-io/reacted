/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.utils;

import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NonNullByDefault
public final class ReActedUtils {
    private ReActedUtils() { /* No Implementation required */ }

    public static CompletionStage<List<ReActorRef>>
    resolveServices(Collection<ServiceDiscoverySearchFilter> filters, ReActorSystem localSystem,
                    Function<Collection<ReActorRef>, Optional<ReActorRef>> gateSelector,
                    String uniqueRequestId) {
        var results = filters.stream()
                             .map(filter -> localSystem.serviceDiscovery(filter, uniqueRequestId))
                             .map(discovery -> discovery.thenApplyAsync(reply -> reply.map(ServiceDiscoveryReply::getServiceGates)
                                                                                      .map(gateSelector::apply)
                                                                                      .peekFailure(error -> localSystem.logError("Error discovering services", error))
                                                                                      .orElse(Optional.empty())
                                                                                      .orElse(ReActorRef.NO_REACTOR_REF))
                                                        .toCompletableFuture())
                             .map(gate -> gate.thenApplyAsync(Stream::of))
                             .reduce((first, second) -> first.thenCombine(second, Stream::concat))
                             .orElse(CompletableFuture.completedFuture(Stream.empty()));
        return results.thenApply(gates -> gates.filter(Predicate.not(ReActorRef.NO_REACTOR_REF::equals))
                                               .collect(Collectors.toUnmodifiableList()));
    }
    public static CompletionStage<Try<DeliveryStatus>>
    composeDeliveries(CompletionStage<Try<DeliveryStatus>> first,
                      CompletionStage<Try<DeliveryStatus>> second,
                      Consumer<Throwable> onFailedDelivery) {
        return ifNotDelivered(first, onFailedDelivery)
            .thenCompose(prevStep -> ifNotDelivered(second, onFailedDelivery));
    }
    public static CompletionStage<Try<DeliveryStatus>>
    ifNotDelivered(CompletionStage<Try<DeliveryStatus>> deliveryAttempt,
                   Consumer<Throwable> onFailedDelivery) {
        return deliveryAttempt.thenApplyAsync(deliveryStatusTry ->
                                              deliveryStatusTry.filter(DeliveryStatus::isDelivered,
                                                                       DeliveryException::new)
                                                               .peekFailure(onFailedDelivery));
    }

    public static  <PayloadT extends Serializable>
    void rescheduleIf(BiConsumer<ReActorContext, PayloadT> realCall,
                      BooleanSupplier shouldReschedule, Duration rescheduleInterval,
                      ReActorContext raCtx, PayloadT message) {

        if (shouldReschedule.getAsBoolean()) {
            raCtx.rescheduleMessage(message, rescheduleInterval)
                 .ifError(error -> raCtx.logError("WARNING {} misbehaves. Error attempting a {} " +
                                                  "reschedulation. " +
                                                  "System remoting may become unreliable ",
                                                  realCall.toString(),
                                                  message.getClass().getSimpleName(),
                                                  error));
        } else {
            realCall.accept(raCtx, message);
        }
    }
}
