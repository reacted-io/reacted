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
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
                        .reAct((ctx, message) -> requestNextMessage(ctx, message, this::routeMessage))
                        .reAct(ServiceRegistryNotAvailable.class, this::onServiceRegistryNotAvailable)
                        .reAct(ServiceDiscoveryRequest.class, this::serviceDiscovery)
                        .reAct(RouteeReSpawnRequest.class, this::respawnRoutee)
                        .reAct(ReActorInit.class, this::initService)
                        .reAct(ReActorStop.class, this::stopService)
                        .reAct(ServicePublicationRequestError.class, this::onServicePublicationError)
                        .reAct(SystemMonitorReport.class, this::onSystemInfoReport)
                        .build();
    }

    private void onServiceRegistryNotAvailable(ReActorContext ctx,
                                               ServiceRegistryNotAvailable notAvailable) {
        ctx.logInfo("{} makes itself locally discoverable",
                      serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        ctx.addTypedSubscriptions(TypedSubscription.LOCAL.forType(ServiceDiscoveryRequest.class));
    }

    public void onServicePublicationError(ReActorContext ctx, ServicePublicationRequestError error) {
        if (!serviceConfig.isRemoteService()) {
            return;
        }

        Try.of(() -> ctx.getReActorSystem()
                          .getSystemSchedulingService()
                          .schedule(() -> sendPublicationRequest(ctx, serviceInfo),
                                    serviceConfig.getServiceRepublishReattemptDelayOnError().toMillis(),
                                    TimeUnit.MILLISECONDS))
           .peekFailure(failure -> ctx.logError("Unable to reschedule service publication", failure))
           .ifError(failure -> ctx.getSelf().publish(ctx.getSender(), error));
    }

    List<ReActorRef> getRouteesMap() { return routeesMap; }

    private void onSystemInfoReport(ReActorContext ctx, SystemMonitorReport report) {
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_CPU_LOAD, report.getCpuLoad());
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_FREE_MEMORY_SIZE, report.getFreeMemorySize());
        updateServiceRegistry(ctx, serviceInfo);
    }

    private void stopService(ReActorContext ctx, ReActorStop stop) {
        ctx.getReActorSystem()
             .getSystemRemotingRoot()
             .publish(ctx.getSelf(), new ServiceCancellationRequest(ctx.getReActorSystem().getLocalReActorSystemId(),
                                                                      serviceConfig.getReActorName()));
    }

    private void initService(ReActorContext ctx, ReActorInit reActorInit) {
        //All the services can receive service stats
        ctx.addTypedSubscriptions(TypedSubscription.LOCAL.forType(SystemMonitorReport.class));

        var backpressuringMbox = BackpressuringMbox.toBackpressuringMailbox(ctx.getMbox());
        backpressuringMbox.filter(mbox -> !mbox.isDelayable(ReActorInit.class))
                          .ifPresent(mbox -> mbox.request(1));
        backpressuringMbox.ifPresent(mbox -> mbox.addNonDelayableTypes(getNonDelayedMessageTypes()));

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
                this.routeesMap.add(spawnRoutee(ctx, routeeReActions, newRouteeCfg));
            } catch (Throwable routeeSpawnError) {
                ctx.logError(ROUTEE_SPAWN_ERROR, routeeSpawnError);
            }
        }
        if (serviceConfig.isRemoteService()) {
            sendPublicationRequest(ctx, serviceInfo);
        }
    }

    private void serviceDiscovery(ReActorContext ctx, ServiceDiscoveryRequest request) {
        if (!request.getSearchFilter().matches(serviceInfo, ctx.getSelf())) {
            return;
        }

        ReActorRef serviceSelection = request.getSearchFilter()
                                                       .getSelectionType() == SelectionType.ROUTED
                                                ? ctx.getSelf()
                                                : selectRoutee(ctx, msgReceived, request);
        if (serviceSelection != null) {
            ctx.reply(ctx.getSelf(), new ServiceDiscoveryReply(serviceSelection));
        }
    }

    private DeliveryStatus routeMessage(ReActorContext ctx,
                                        ReActedMessage newMessage) {
        ReActorRef routee = selectRoutee(ctx, ++msgReceived, newMessage);
        return routee != null
               ? routee.tell(ctx.getSender(), newMessage)
               : DeliveryStatus.NOT_SENT;
    }

    @Nullable
    private ReActorRef selectRoutee(ReActorContext routerCtx, long msgReceived,
                                              ReActedMessage message) {
        return serviceConfig.getLoadBalancingPolicy()
                            .selectRoutee(routerCtx, this, msgReceived, message);
    }
    private void respawnRoutee(ReActorContext ctx, RouteeReSpawnRequest reSpawnRequest) {
        this.routeesMap.remove(reSpawnRequest.deadRoutee);
        Try.of(() -> Objects.requireNonNull(serviceConfig.getRouteeProvider()
                                                         .apply(serviceConfig)))
           .map(routee -> spawnRoutee(ctx, routee.getReActions(),
                                      ReActorConfig.fromConfig(routee.getConfig())
                                                   .setReActorName(reSpawnRequest.routeeName)
                                                   .build()))
           .ifSuccessOrElse(this.routeesMap::add, spawnError -> ctx.logError(ROUTEE_SPAWN_ERROR,
                                                                               spawnError));
    }

    private ReActorRef spawnRoutee(ReActorContext routerCtx, ReActions routeeReActions,
                                   ReActorConfig routeeConfig) {
        ReActorRef routee = routerCtx.spawnChild(routeeReActions, routeeConfig).orElseSneakyThrow();
        ReActorContext routeeCtx = routerCtx.getReActorSystem().getReActorCtx(routee.getReActorId());

        if (routeeCtx == null) {
            throw new IllegalStateException("Unable to find actor (routee) ctx for a newly spawned actor");
        }
        //when a routee dies, asks the father to be respawned. If the father is stopped (i.e. on system shutdown)
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

    private void updateServiceRegistry(ReActorContext ctx, Properties serviceInfo) {
        if (!serviceConfig.isRemoteService()) {
            return;
        }

        if (!sendPublicationRequest(ctx, serviceInfo).isSent()) {
            ctx.logError("Unable to refresh service info {}",
                           serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        }
    }

    private static DeliveryStatus sendPublicationRequest(ReActorContext ctx,
                                                         Properties serviceInfo) {
        return ctx.getReActorSystem()
                    .getSystemRemotingRoot()
                    .publish(ctx.getSelf(), new ServicePublicationRequest(ctx.getSelf(), serviceInfo));
    }

    private static <PayloadT extends ReActedMessage>
    void requestNextMessage(ReActorContext ctx, PayloadT payload,
                            BiFunction<ReActorContext, PayloadT, DeliveryStatus> realCall) {
        if (realCall.apply(ctx, payload).isNotSent()) {
            ctx.getReActorSystem().toDeadLetters(ctx.getSender(), payload);
            ctx.logError(NO_ROUTEE_FOR_SPECIFIED_ROUTER, ctx.getSelf().getReActorId().getReActorName());
        }
        ctx.getMbox().request(1);
    }

    //Messages required for the Service management logic cannot be backpressured
    private static Set<Class<? extends ReActedMessage>> getNonDelayedMessageTypes() {
        return Set.of(ServiceRegistryNotAvailable.class,
                      ServiceDiscoveryRequest.class, RouteeReSpawnRequest.class,
                      ServicePublicationRequestError.class, SystemMonitorReport.class);
    }

    public static class RouteeReSpawnRequest implements ReActedMessage {
        private final ReActorRef deadRoutee;
        private final String routeeName;
        public RouteeReSpawnRequest(ReActorRef deadRoutee, String routeeName) {
            this.deadRoutee = deadRoutee;
            this.routeeName = routeeName;
        }
    }
}
