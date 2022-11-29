/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
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
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

@NonNullByDefault
public class Service<ServiceCfgBuilderT extends ReActorServiceConfig.Builder<ServiceCfgBuilderT, ServiceCfgT>,
                     ServiceCfgT extends ReActorServiceConfig<ServiceCfgBuilderT, ServiceCfgT>>
    implements ReActiveEntity {

    private static final Logger LOGGER = LoggerFactory.getLogger(Service.class);
    private static final String ROUTEE_SPAWN_ERROR = "Unable to spawn routee";
    private static final String NO_ROUTEE_FOR_SPECIFIED_ROUTER = "No routee found for router %s";
    private static final String REACTOR_SERVICE_NAME_FORMAT = "[%s-%s-%d]";
    private final Properties serviceInfo;
    private final ServiceCfgT serviceConfig;
    private final ArrayList<ReActorRef> routeesMap;
    private long msgReceived;

    public Service(ServiceCfgT serviceConfig) {
        this.serviceInfo = new Properties();
        this.serviceConfig = Objects.requireNonNull(serviceConfig);
        this.msgReceived = 1;
        this.routeesMap = new ArrayList<>(serviceConfig.getRouteesNum());
        this.serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME, serviceConfig.getReActorName());
    }

    @Nonnull
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct((raCtx, message) -> requestNextMessage(raCtx, message, this::routeMessage))
                        .reAct(ServiceRegistryNotAvailable.class, this::onServiceRegistryNotAvailable)
                        .reAct(ServiceDiscoveryRequest.class, this::serviceDiscovery)
                        .reAct(RouteeReSpawnRequest.class, this::respawnRoutee)
                        .reAct(ReActorInit.class, this::initService)
                        .reAct(ReActorStop.class, this::stopService)
                        .reAct(ServicePublicationRequestError.class, this::onServicePublicationError)
                        .reAct(SystemMonitorReport.class, this::onSystemInfoReport)
                        .build();
    }

    private void onServiceRegistryNotAvailable(ReActorContext raCtx,
                                               ServiceRegistryNotAvailable notAvailable) {
        raCtx.logInfo("{} makes itself locally discoverable",
                      serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        raCtx.addTypedSubscriptions(TypedSubscription.LOCAL.forType(ServiceDiscoveryRequest.class));
    }

    public void onServicePublicationError(ReActorContext raCtx, ServicePublicationRequestError error) {
        if (!serviceConfig.isRemoteService()) {
            return;
        }

        Try.of(() -> raCtx.getReActorSystem()
                          .getSystemSchedulingService()
                          .schedule(() -> sendPublicationRequest(raCtx, serviceInfo),
                                    serviceConfig.getServiceRepublishReattemptDelayOnError().toMillis(),
                                    TimeUnit.MILLISECONDS))
           .peekFailure(failure -> raCtx.logError("Unable to reschedule service publication", failure))
           .ifError(failure -> raCtx.getSelf().publish(raCtx.getSender(), error));
    }

    List<ReActorRef> getRouteesMap() { return routeesMap; }

    private void onSystemInfoReport(ReActorContext raCtx, SystemMonitorReport report) {
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_CPU_LOAD, report.getCpuLoad());
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_FREE_MEMORY_SIZE, report.getFreeMemorySize());
        updateServiceRegistry(raCtx, serviceInfo);
    }

    private void stopService(ReActorContext raCtx, ReActorStop stop) {
        raCtx.getReActorSystem()
             .getSystemRemotingRoot()
             .publish(raCtx.getSelf(), new ServiceCancellationRequest(raCtx.getReActorSystem().getLocalReActorSystemId(),
                                                                      serviceConfig.getReActorName()));
    }

    private void initService(ReActorContext raCtx, ReActorInit reActorInit) {
        //All the services can receive service stats
        raCtx.addTypedSubscriptions(TypedSubscription.LOCAL.forType(SystemMonitorReport.class));

        var backpressuringMbox = BackpressuringMbox.toBackpressuringMailbox(raCtx.getMbox());
        backpressuringMbox.filter(mbox -> !mbox.isDelayable(ReActorInit.class))
                          .ifPresent(mbox -> mbox.request(1));

        //spawn the minimum number or routees
        for (int currentRoutee = 0; currentRoutee < serviceConfig.getRouteesNum(); currentRoutee++) {
            try {
                ReActor routee = Objects.requireNonNull(serviceConfig.getRouteeProvider()
                                                                     .apply(serviceConfig));
                ReActorConfig routeeConfig = routee.getConfig();
                ReActions routeeReActions = routee.getReActions();
                //A service has multiple children, so they cannot share the same name
                String routeeNewName = String.format(REACTOR_SERVICE_NAME_FORMAT,
                                                     serviceConfig.getReActorName(),
                                                     routeeConfig.getReActorName(), currentRoutee);
                ReActorConfig newRouteeCfg = ReActorConfig.fromConfig(routeeConfig)
                                                          .setReActorName(routeeNewName)
                                                          .build();
                this.routeesMap.add(spawnRoutee(raCtx, routeeReActions, newRouteeCfg));
            } catch (Throwable routeeSpawnError) {
                raCtx.logError(ROUTEE_SPAWN_ERROR, routeeSpawnError);
            }
        }
        if (serviceConfig.isRemoteService()) {
            sendPublicationRequest(raCtx, serviceInfo);
        }
    }

    private boolean routeesCannotBeFedAllTogether(int requestOnStartup) {
        return requestOnStartup < serviceConfig.getRouteesNum();
    }

    private void serviceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (!request.getSearchFilter().matches(serviceInfo, raCtx.getSelf())) {
            return;
        }

        ReActorRef serviceSelection = request.getSearchFilter()
                                                       .getSelectionType() == SelectionType.ROUTED
                                                ? raCtx.getSelf()
                                                : selectRoutee(raCtx, msgReceived, request);
        if (serviceSelection != null) {
            raCtx.reply(raCtx.getSelf(), new ServiceDiscoveryReply(serviceSelection));
        }
    }

    private DeliveryStatus routeMessage(ReActorContext raCtx,
                                        Serializable newMessage) {
        ReActorRef routee = selectRoutee(raCtx, ++msgReceived, newMessage);
        return routee != null
               ? routee.tell(raCtx.getSender(), newMessage)
               : DeliveryStatus.NOT_DELIVERED;
    }

    @Nullable
    private ReActorRef selectRoutee(ReActorContext routerCtx, long msgReceived,
                                              Serializable message) {
        return serviceConfig.getLoadBalancingPolicy()
                            .selectRoutee(routerCtx, this, msgReceived, message);
    }
    private void respawnRoutee(ReActorContext raCtx, RouteeReSpawnRequest reSpawnRequest) {
        this.routeesMap.remove(reSpawnRequest.deadRoutee);
        Try.of(() -> Objects.requireNonNull(serviceConfig.getRouteeProvider()
                                                         .apply(serviceConfig)))
           .map(routee -> spawnRoutee(raCtx, routee.getReActions(),
                                      ReActorConfig.fromConfig(routee.getConfig())
                                                   .setReActorName(reSpawnRequest.routeeName)
                                                   .build()))
           .ifSuccessOrElse(this.routeesMap::add, spawnError -> raCtx.logError(ROUTEE_SPAWN_ERROR,
                                                                               spawnError));
    }

    private ReActorRef spawnRoutee(ReActorContext routerCtx, ReActions routeeReActions,
                                   ReActorConfig routeeConfig) {
        ReActorRef routee = routerCtx.spawnChild(routeeReActions, routeeConfig).orElseSneakyThrow();
        ReActorContext routeeCtx = routerCtx.getReActorSystem().getReActorCtx(routee.getReActorId());

        if (routeeCtx == null) {
            throw new IllegalStateException("Unable to find actor (routee) ctx for a newly spawned actor");
        }
        //when a routee dies, asks the father to be respawn. If the father is stopped (i.e. on system shutdown)
        //the message will be simply routed to deadletter
        routeeCtx.getHierarchyTermination()
                 .thenAccept(terminated -> { if (routeeCtx.isStop()) {
                                                this.routeesMap.remove(routee);
                                             } else {
                                                routerCtx.selfTell(new RouteeReSpawnRequest(routee, routeeConfig.getReActorName()));
                                             }
                                           });
        return routee;
    }

    private void updateServiceRegistry(ReActorContext raCtx, Properties serviceInfo) {
        if (!serviceConfig.isRemoteService()) {
            return;
        }

        if (!sendPublicationRequest(raCtx, serviceInfo).isSent()) {
            raCtx.logError("Unable to refresh service info {}",
                           serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        }
    }

    private static DeliveryStatus sendPublicationRequest(ReActorContext raCtx,
                                                         Properties serviceInfo) {
        return raCtx.getReActorSystem()
                    .getSystemRemotingRoot()
                    .publish(raCtx.getSelf(), new ServicePublicationRequest(raCtx.getSelf(), serviceInfo));
    }

    private static <PayloadT extends Serializable>
    void requestNextMessage(ReActorContext raCtx, PayloadT payload,
                            BiFunction<ReActorContext, PayloadT, DeliveryStatus> realCall) {
        realCall.apply(raCtx, payload);
        raCtx.getMbox().request(1);
    }

    //Messages required for the Service management logic cannot be backpressured
    private static Set<Class<? extends Serializable>> getNonDelayedMessageTypes() {
        return Set.of(ServiceRegistryNotAvailable.class,
                      ServiceDiscoveryRequest.class, RouteeReSpawnRequest.class,
                      ServicePublicationRequestError.class, SystemMonitorReport.class);
    }

    public static class RouteeReSpawnRequest implements Serializable {
        private final ReActorRef deadRoutee;
        private final String routeeName;
        public RouteeReSpawnRequest(ReActorRef deadRoutee, String routeeName) {
            this.deadRoutee = deadRoutee;
            this.routeeName = routeeName;
        }
    }
}
