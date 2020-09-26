/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

public class TimeReActor implements ReActor {
    private final String serviceToQuery;
    private final String reactorName;
    private int received = 0;
    public TimeReActor(String serviceToQuery, String reactorName) {
        this.serviceToQuery = serviceToQuery;
        this.reactorName = reactorName;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(ServiceDiscoveryReply.class, this:: onServiceDiscoveryReply)
                        .reAct(ZonedDateTime.class, this::onServiceResponse)
                        .reAct(ReActorStop.class, this::onStop)
                        .build();
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        raCtx.getReActorSystem().serviceDiscovery(serviceToQuery,
                                                  ServiceDiscoveryRequest.SelectionType.DIRECT,
                                                  raCtx.getSelf());
    }

    private void onServiceDiscoveryReply(ReActorContext raCtx, ServiceDiscoveryReply serviceDiscoveryReply) {
        var gate = serviceDiscoveryReply.getServiceGates().stream().findAny();
        gate.ifPresentOrElse(serviceGate -> serviceGate.tell(raCtx.getSelf(), new TimeRequest())
                                                       .toCompletableFuture()
                                                       .thenAccept(result -> result.filter(DeliveryStatus::isDelivered)
                                                                                   .ifError(Throwable::printStackTrace)),
                             () -> raCtx.getReActorSystem().logError("No response received"));
    }

    private void onServiceResponse(ReActorContext raCtx, ZonedDateTime time) {
        raCtx.getReActorSystem().logInfo("Received {} response from service: {}", ++received, time.toString());
        raCtx.stop();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        raCtx.getReActorSystem()
             .logInfo("{} is exiting and exiting reactorsystem...", raCtx.getSelf().getReActorId().getReActorName());

        CompletableFuture.supplyAsync(() -> Try.ofRunnable(() -> raCtx.getReActorSystem().shutDown())
                                               .ifError(Throwable::printStackTrace));
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(TimeReActor.class.getSimpleName() + "-" + reactorName)
                            .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                            .setMailBoxProvider(BasicMbox::new)
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .build();
    }
}
