/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.SelectionType;

import javax.annotation.Nonnull;
import java.time.ZonedDateTime;

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

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
        ifNotDelivered(raCtx.getReActorSystem()
                            .serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                               .setServiceName(serviceToQuery)
                                                                               .setSelectionType(SelectionType.DIRECT)
                                                                               .build(), raCtx.getSelf()),
                       error -> raCtx.logError("Error discovering service", error));
    }

    private void onServiceDiscoveryReply(ReActorContext raCtx, ServiceDiscoveryReply serviceDiscoveryReply) {
        var gate = serviceDiscoveryReply.getServiceGates().stream().findAny();
        gate.ifPresentOrElse(serviceGate -> ifNotDelivered(serviceGate.tell(raCtx.getSelf(), new TimeRequest()),
                                                          Throwable::printStackTrace),
                             () -> raCtx.logError("No service discovery response received"));
    }

    private void onServiceResponse(ReActorContext raCtx, ZonedDateTime time) {
        raCtx.logInfo("Received {} response from service: {}", ++received, time.toString());
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        raCtx.logInfo("{} is exiting", raCtx.getSelf().getReActorId().getReActorName());
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(TimeReActor.class.getSimpleName() + "-" + reactorName)
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .build();
    }
}
