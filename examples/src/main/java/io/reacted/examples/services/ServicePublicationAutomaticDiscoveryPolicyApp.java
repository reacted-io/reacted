/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.services;

import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorServiceConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.services.ReActorService;
import io.reacted.core.services.SelectionType;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ServicePublicationAutomaticDiscoveryPolicyApp {
    public static void main(String[] args) {
        String serviceName = "Clock Service";
        String serviceDispatcherName = serviceName + " Dedicated Dispatcher";
        //Since we want to grant the best possible service, let's not mix system messages execution with the
        //message execution of this service, so it needs a dedicated dispatcher
        DispatcherConfig serviceDispatcherCfg = DispatcherConfig.newBuilder()
                                                                //High overhead, but very reactive
                                                                .setBatchSize(1)
                                                                //This dispatcher will have this label in the thead list
                                                                .setDispatcherName(serviceDispatcherName)
                                                                .setDispatcherThreadsNum(1)
                                                                .build();

        ReActorSystemConfig systemConfig = ReActorSystemConfig.newBuilder()
                                                              .setReactorSystemName(ServicePublicationAutomaticDiscoveryPolicyApp.class.getSimpleName())
                                                              .setMsgFanOutPoolSize(1)
                                                              .setRecordExecution(false)
                                                              .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                              //.setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                              //We can add as many dispatchers as we want
                                                              .addDispatcherConfig(serviceDispatcherCfg)
                                                              .build();
        var reActorSystem = new ReActorSystem(systemConfig).initReActorSystem();

        ReActorConfig routeeConfig = ReActorConfig.newBuilder()
                                                  .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                                  .setReActorName("ClockWorker")
                                                  //Not only the service, but we want also its workers to use the same
                                                  //dedicated dispatcher
                                                  .setDispatcherName(serviceDispatcherName)
                                                  .build();
        var routeeReActions = ReActions.newBuilder()
                                       .reAct(TimeRequest.class, ServicePublicationAutomaticDiscoveryPolicyApp::onTimeRequest)
                                       .reAct((raCtx, any) -> {})
                                       .build();
        //Here we define how a routee behaves. This is going to be the actual body of our servuce
        UnChecked.CheckedSupplier<ReActor> routeeProvider = () -> new ReActor() {
            @Nonnull
            @Override
            public ReActions getReActions() { return routeeReActions; }

            @Nonnull
            @Override
            public ReActorConfig getConfig() { return routeeConfig; }
        };
        var clockServiceConfig = ReActorServiceConfig.newBuilder()
                                                     //Name of the service. It will be published with this name
                                                     .setReActorName(serviceName)
                                                     .setRouteeProvider(routeeProvider)
                                                     //On service startup, 5 routees will be created
                                                     .setRouteesNum(5)
                                                     //and requests to the service will be balanced
                                                     //among the routees using the following policy
                                                     .setSelectionPolicy(ReActorService.LoadBalancingPolicy.LOWEST_LOAD)
                                                     .setDispatcherName(serviceDispatcherName)
                                                     //We can have at maximum 5 pending messages in the
                                                     //service mailbox. The exceeding ones will be
                                                     //dropped win an error to the sender
                                                     .setMailBoxProvider(ctx -> new BoundedBasicMbox(5))
                                                     .build();
        reActorSystem.spawnService(clockServiceConfig).orElseSneakyThrow();
        //Ask for a reference to a service called Clock Service. A reference to the service itself will be returned
        //This means that all the requests sent to the returned reference will be routed to one of the available workers
        reActorSystem.serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                        .setServiceName(serviceName)
                                                                        .setSelectionType(SelectionType.ROUTED)
                                                                        .build())
                     .thenApply(discovery -> discovery.map(ServiceDiscoveryReply::getServiceGates))
                     .thenApply(services -> services.filter(list -> !list.isEmpty()))
                     //get the first gate available
                     .thenApply(services -> services.map(list -> list.iterator().next()))
                     .thenApply(Try::orElseSneakyThrow)
                     //Ask the service for the time
                     .thenCompose(gate -> gate.ask(new TimeRequest(), ZonedDateTime.class, "Request the time"))
                     //print the answer
                     .thenAcceptAsync(timeRequest -> timeRequest.ifSuccessOrElse(System.out::println,
                                                                                 Throwable::printStackTrace))
                     //kill the reactor system
                     .thenAccept(dummyReturn -> reActorSystem.shutDown());
    }

    private static void onTimeRequest(ReActorContext raCtx, TimeRequest timeRequest) {
        raCtx.logInfo("{} received {}", raCtx.getSelf().getReActorId().getReActorName(),
                      timeRequest.getClass().getSimpleName());
        raCtx.reply(ReActorRef.NO_REACTOR_REF, ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
    }
}
