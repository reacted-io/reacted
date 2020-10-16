/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.reactors.SystemMonitorReport;
import io.reacted.core.messages.serviceregistry.ServiceCancellationRequest;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequest;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequestError;
import io.reacted.core.messages.serviceregistry.ServiceRegistryNotAvailable;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ServiceConfig;
import io.reacted.patterns.Try;
import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class Service implements ReActiveEntity {
    private static final String ROUTEE_REACTIONS_RETRIEVAL_ERROR = "Unable to get routee reactions from specified provider";
    private static final String ROUTEE_SPAWN_ERROR = "Unable to spawn routee";
    private static final String NO_ROUTEE_FOR_SPECIFIED_ROUTER = "No routee found for router {}";
    private static final String REACTOR_SERVICE_NAME_FORMAT = "[%s-%s-%d]";
    private final Properties serviceInfo;
    private final ServiceConfig serviceConfig;
    private long msgReceived;

    public Service(ServiceConfig serviceConfig) {
        this.serviceInfo = new Properties();
        this.serviceConfig = Objects.requireNonNull(serviceConfig);
        this.msgReceived = 1;
        this.serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME, serviceConfig.getReActorName());
    }

    @Nonnull
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(this::routeMessage)
                        .reAct(ServiceRegistryNotAvailable.class, this::onServiceRegistryNotAvailable)
                        .reAct(ServiceDiscoveryRequest.class, this::serviceDiscovery)
                        .reAct(RouteeReSpawnRequest.class, this::respawnRoutee)
                        .reAct(ReActorInit.class, this::initService)
                        .reAct(ReActorStop.class, this::stopService)
                        .reAct(ServicePublicationRequestError.class, this::onServicePublicationError)
                        .reAct(SystemMonitorReport.class, this::onSystemInfoReport)
                        .build();
    }

    private void onServiceRegistryNotAvailable(ReActorContext raCtx, ServiceRegistryNotAvailable notAvailable) {
        raCtx.logInfo("{} makes itself discoverable",
                      this.serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        raCtx.addTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class));
    }

    public void onServicePublicationError(ReActorContext raCtx, ServicePublicationRequestError error) {
        if (!this.serviceConfig.isRemoteService()) {
            return;
        }

        Try.of(() -> raCtx.getReActorSystem()
                          .getSystemSchedulingService()
                          .schedule(() -> sendPublicationRequest(raCtx, this.serviceInfo),
                                    this.serviceConfig.getServiceRepublishReattemptDelayOnError().toMillis(),
                                    TimeUnit.MILLISECONDS))
           .peekFailure(failure -> raCtx.logError("Unable to reschedule service publication", failure))
           .ifError(failure -> raCtx.getSelf().tell(raCtx.getSender(), error));
    }

    private void onSystemInfoReport(ReActorContext raCtx, SystemMonitorReport report) {
        this.serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_CPU_LOAD, report.getCpuLoad());
        this.serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_FREE_MEMORY_SIZE, report.getFreeMemorySize());
        updateServiceRegistry(raCtx, this.serviceInfo);
    }

    private void stopService(ReActorContext raCtx, ReActorStop stop) {
        raCtx.getReActorSystem()
             .getSystemRemotingRoot()
             .tell(raCtx.getSelf(), new ServiceCancellationRequest(raCtx.getReActorSystem().getLocalReActorSystemId(),
                                                                   serviceConfig.getReActorName()));
    }

    private void initService(ReActorContext raCtx, ReActorInit reActorInit) {
        //All the services can receive service stats
        raCtx.addTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(SystemMonitorReport.class));

        //spawn the minimum number or routees
        for (int currentRoutee = 0; currentRoutee < serviceConfig.getRouteesNum(); currentRoutee++) {
            try {
                ReActor routee = Objects.requireNonNull(serviceConfig.getRouteeProvider()
                                                                     .get());
                ReActorConfig routeeConfig = routee.getConfig();
                ReActions routeeReActions = routee.getReActions();
                //A service has multiple children, so they cannot share the same name
                String routeeNewName = String.format(REACTOR_SERVICE_NAME_FORMAT,
                                                     serviceConfig.getReActorName(),
                                                     routeeConfig.getReActorName(), currentRoutee);
                ReActorConfig newRouteeCfg = ReActorConfig.fromConfig(routeeConfig)
                                                          .setReActorName(routeeNewName)
                                                          .build();
                spawnRoutee(raCtx, routeeReActions, newRouteeCfg);
            } catch (Throwable routeeSpawnError) {
                raCtx.logError(ROUTEE_SPAWN_ERROR, routeeSpawnError);
            }
        }
        if (this.serviceConfig.isRemoteService()) {
            sendPublicationRequest(raCtx, this.serviceInfo);
        }
    }

    private void serviceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (!request.getSearchFilter().matches(this.serviceInfo, raCtx.getSelf())) {
            return;
        }

        Optional<ReActorRef> serviceSelection = switch (request.getSearchFilter().getSelectionType()) {
            case ROUTED -> Optional.of(raCtx.getSelf());
            case DIRECT -> selectRoutee(raCtx, msgReceived);
        };

        serviceSelection.map(ServiceDiscoveryReply::new)
                        .ifPresent(discoveryReply -> raCtx.getSender().tell(raCtx.getSelf(),
                                                                                     discoveryReply));
    }

    private void routeMessage(ReActorContext raCtx, Serializable newMessage) {
        selectRoutee(raCtx, ++msgReceived)
                .ifPresentOrElse(routee -> routee.tell(raCtx.getSender(), newMessage),
                                 () -> raCtx.logError(NO_ROUTEE_FOR_SPECIFIED_ROUTER,
                                                      serviceConfig.getReActorName(), new IllegalStateException()));
    }

    private Optional<ReActorRef> selectRoutee(ReActorContext routerCtx, long msgReceived) {
        return serviceConfig.getLoadBalancingPolicy().selectRoutee(routerCtx, msgReceived);
    }

    private void respawnRoutee(ReActorContext raCtx, RouteeReSpawnRequest reSpawnRequest) {
        Try.of(() -> Objects.requireNonNull(serviceConfig.getRouteeProvider()
                                                         .get()))
           .peekFailure(error -> raCtx.logError(ROUTEE_REACTIONS_RETRIEVAL_ERROR, error))
           .ifSuccess(routee -> spawnRoutee(raCtx, routee.getReActions(), reSpawnRequest.routeeConfig))
           .ifError(spawnError -> raCtx.logError(ROUTEE_SPAWN_ERROR, spawnError));
    }

    private void spawnRoutee(ReActorContext routerCtx, ReActions routeeReActions, ReActorConfig routeeConfig) {
        ReActorRef routee = routerCtx.spawnChild(routeeReActions, routeeConfig).orElseSneakyThrow();
        ReActorContext routeeCtx = routerCtx.getReActorSystem().getReActor(routee.getReActorId())
                                            .orElseThrow();
        //when a routee dies, asks the father to be respawn. If the father is stopped (i.e. on system shutdown)
        //the message will be simply routed to deadletter
        routeeCtx.getHierarchyTermination()
                 .thenAccept(terminated -> { if (!routeeCtx.isStop()) {
                                                  routerCtx.getSelf().tell(ReActorRef.NO_REACTOR_REF,
                                                                           new RouteeReSpawnRequest(routeeConfig)); }});
    }

    private void updateServiceRegistry(ReActorContext raCtx, Properties serviceInfo) {
        if (!this.serviceConfig.isRemoteService()) {
            return;
        }

        sendPublicationRequest(raCtx, serviceInfo)
             .thenAcceptAsync(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                .ifError(error -> raCtx.logError("Unable to refresh " +
                                                                                                 "service info {}",
                                                                                                 serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME),
                                                                                                 error)));
    }

    private static CompletionStage<Try<DeliveryStatus>> sendPublicationRequest(ReActorContext raCtx,
                                                                               Properties serviceInfo) {
        return raCtx.getReActorSystem()
                    .getSystemRemotingRoot()
                    .tell(raCtx.getSelf(), new ServicePublicationRequest(raCtx.getSelf(),
                                                                         serviceInfo));
    }

    public static class RouteeReSpawnRequest implements Serializable {
        final ReActorConfig routeeConfig;
        public RouteeReSpawnRequest(ReActorConfig routeeConfig) {
            this.routeeConfig = routeeConfig;
        }
    }

    public enum LoadBalancingPolicy {
        ROUND_ROBIN {
            @Override
            public Optional<ReActorRef> selectRoutee(ReActorContext routerCtx, long msgNum) {
                int routeeIdx = (int) ((msgNum % Integer.MAX_VALUE) % routerCtx.getChildren().size());
                return Try.of(() -> routerCtx.getChildren().get(routeeIdx))
                          .toOptional();
            }
        },
        LOWEST_LOAD {
            @Override
            public Optional<ReActorRef> selectRoutee(ReActorContext routerCtx, long msgNum) {
                return routerCtx.getChildren().stream()
                                .map(ReActorRef::getReActorId)
                                .map(routerCtx.getReActorSystem()::getReActor)
                                .flatMap(Optional::stream)
                                .min(Comparator.comparingLong(reActorCtx -> reActorCtx.getMbox().getMsgNum()))
                                .map(ReActorContext::getSelf);
            }
        };
        abstract Optional<ReActorRef> selectRoutee(ReActorContext routerCtx, long msgNum);
    }
}
