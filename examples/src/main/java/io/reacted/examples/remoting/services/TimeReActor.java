/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.services.SelectionType;
import io.reacted.core.typedsubscriptions.TypedSubscription;

import javax.annotation.Nonnull;

public class TimeReActor implements ReActor {
    private final String serviceToQuery;
    private int received = 0;
    public TimeReActor(String serviceToQuery) {
        this.serviceToQuery = serviceToQuery;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(ServiceDiscoveryReply.class, this:: onServiceDiscoveryReply)
                        .reAct(TimeMessages.TimeReply.class, this::onServiceResponse)
                        .reAct(ReActorStop.class, this::onStop)
                        .build();
    }

    private void onInit(ReActorContext ctx, ReActorInit init) {
        if (!ctx.getReActorSystem()
                            .serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                               .setServiceName(serviceToQuery)
                                                                               .setSelectionType(SelectionType.DIRECT)
                                                                               .build(), ctx.getSelf()).isDelivered()) {
            ctx.logError("Error discovering service");
        }
    }

    private void onServiceDiscoveryReply(ReActorContext ctx, ServiceDiscoveryReply serviceDiscoveryReply) {
        var gate = serviceDiscoveryReply.getServiceGates().stream()
                                        .findAny();
        gate.ifPresentOrElse(serviceGate -> serviceGate.publish(ctx.getSelf(), new TimeMessages.TimeRequest()),
                             () -> ctx.logError("No service discovery response received"));
    }

    private void onServiceResponse(ReActorContext ctx, TimeMessages.TimeReply time) {
        ctx.logInfo("Received {} response from service: {}", ++received, time.toString());
    }

    private void onStop(ReActorContext ctx, ReActorStop stop) {
        ctx.logInfo("{} is exiting", ctx.getSelf().getReActorId().getReActorName());
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(TimeReActor.class.getSimpleName())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .setMailBoxProvider(ctx -> new UnboundedMbox())
                            .setDispatcherName(Dispatcher.DEFAULT_DISPATCHER_NAME)
                            .build();
    }
}
