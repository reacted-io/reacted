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
import io.reacted.core.reactorsystem.ServiceConfig;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

@NonNullByDefault
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
                        .reAct((raCtx, message) -> requestNextMessage(raCtx, message, this::routeMessage))
                        .reAct(ServiceRegistryNotAvailable.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message,
                                                                      this::onServiceRegistryNotAvailable))
                        .reAct(ServiceDiscoveryRequest.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::serviceDiscovery))
                        .reAct(RouteeReSpawnRequest.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::respawnRoutee))
                        .reAct(ReActorInit.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::initService))
                        .reAct(ReActorStop.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::stopService))
                        .reAct(ServicePublicationRequestError.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::onServicePublicationError))
                        .reAct(SystemMonitorReport.class,
                               (raCtx, message) -> requestNextMessage(raCtx, message, this::onSystemInfoReport))
                        .build();
    }

    private void onServiceRegistryNotAvailable(ReActorContext raCtx, ServiceRegistryNotAvailable notAvailable) {
        raCtx.logInfo("{} makes itself discoverable",
                      this.serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME));
        raCtx.addTypedSubscriptions(TypedSubscriptionPolicy.LOCAL.forType(ServiceDiscoveryRequest.class));
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
           .ifError(failure -> raCtx.getSelf().tell(raCtx.getSender(), error));
    }

    private void onSystemInfoReport(ReActorContext raCtx, SystemMonitorReport report) {
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_CPU_LOAD, report.getCpuLoad());
        serviceInfo.put(ServiceDiscoverySearchFilter.FIELD_NAME_FREE_MEMORY_SIZE, report.getFreeMemorySize());
        updateServiceRegistry(raCtx, serviceInfo);
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

        if (BackpressuringMbox.class.isAssignableFrom(raCtx.getMbox().getClass())) {
            BackpressuringMbox myMbox = (BackpressuringMbox)raCtx.getMbox();
            myMbox.setNotDelayedMessageTypes(getNonDelayedMessageTypes(myMbox.getNotDelayedMessageTypes()));
        }

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
        if (serviceConfig.isRemoteService()) {
            sendPublicationRequest(raCtx, serviceInfo);
        }
    }

    private void serviceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (!request.getSearchFilter().matches(serviceInfo, raCtx.getSelf())) {
            return;
        }

        Optional<ReActorRef> serviceSelection = request.getSearchFilter().getSelectionType() == SelectionType.ROUTED
                                                ? Optional.of(raCtx.getSelf())
                                                : selectRoutee(raCtx, msgReceived);

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
        if (!serviceConfig.isRemoteService()) {
            return;
        }

        ifNotDelivered(sendPublicationRequest(raCtx, serviceInfo),
                       error -> raCtx.logError("Unable to refresh service info {}",
                                               serviceInfo.getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME),
                                               error));
    }

    private static CompletionStage<Try<DeliveryStatus>> sendPublicationRequest(ReActorContext raCtx,
                                                                               Properties serviceInfo) {
        return raCtx.getReActorSystem()
                    .getSystemRemotingRoot()
                    .tell(raCtx.getSelf(), new ServicePublicationRequest(raCtx.getSelf(), serviceInfo));
    }

    private static <PayloadT extends Serializable>
    void requestNextMessage(ReActorContext raCtx, PayloadT payload, BiConsumer<ReActorContext, PayloadT> realCall) {
        raCtx.getMbox().request(1);
        realCall.accept(raCtx, payload);
    }

    //Messages required for the Service management logic cannot be backpressured
    private static Set<Class<? extends Serializable>>
    getNonDelayedMessageTypes(Set<Class<? extends Serializable>> userDefinedNotDelayedMessageTypes) {
        return Stream.concat(userDefinedNotDelayedMessageTypes.stream(),
                             Stream.of(ReActorInit.class, ServiceRegistryNotAvailable.class,
                                       ServiceDiscoveryRequest.class, RouteeReSpawnRequest.class,
                                       ServicePublicationRequestError.class, SystemMonitorReport.class))
                     .collect(Collectors.toUnmodifiableSet());
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
