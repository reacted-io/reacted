/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.serviceregistries;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryInit;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.serviceregistry.RegistryDriverInitComplete;
import io.reacted.core.messages.serviceregistry.RegistryGateRemoved;
import io.reacted.core.messages.serviceregistry.RegistryGateUpserted;
import io.reacted.core.messages.serviceregistry.RegistryPublicationRequest;
import io.reacted.core.messages.serviceregistry.RegistryServiceCancellationRequest;
import io.reacted.core.messages.serviceregistry.RegistryServicePublicationFailed;
import io.reacted.core.messages.serviceregistry.RegistryServicePublicationRequest;
import io.reacted.core.messages.serviceregistry.RegistrySubscriptionComplete;
import io.reacted.core.messages.serviceregistry.RegistrySubscriptionRequest;
import io.reacted.core.messages.serviceregistry.RegistryUnregisterChannel;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZooKeeperDriver implements ServiceRegistryDriver {
    public static final String ZK_CONNECTION_STRING = "zkConnectionString";
    private static final String REACTED_REACTORSYSTEMS_ROOT = "REACTED_REACTOR_SYSTEMS";
    private static final String REACTED_SERVICES_ROOT = "REACTED_SERVICES";
    private static final String CLUSTER_REGISTRY_ROOT_PATH = ZKPaths.makePath("", "REACTED_CLUSTER_ROOT");
    private static final String CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH = ZKPaths.makePath(CLUSTER_REGISTRY_ROOT_PATH,
                                                                                             REACTED_REACTORSYSTEMS_ROOT);
    private static final String CLUSTER_REGISTRY_SERVICES_ROOT_PATH = ZKPaths.makePath(CLUSTER_REGISTRY_ROOT_PATH,
                                                                                       REACTED_SERVICES_ROOT);
    private static final String CLUSTER_GATE_PUBLICATION_PATH = ZKPaths.makePath(CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH,
                                                                                 "%s","%s");
    private static final CompletionStage<Try<DeliveryStatus>> SUCCESS = CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.DELIVERED));

    private final Properties config;
    @Nullable
    private CuratorFramework client;
    @Nullable
    private ServiceDiscovery<String> serviceDiscovery;
    @Nullable
    private AsyncCuratorFramework asyncClient;
    @Nullable
    private TreeCache reActedHierarchy;

    public ZooKeeperDriver(Properties config) {
        this.config = config;
    }

    @Override
    public void init(ServiceRegistryInit initInfo) {
        this.client = CuratorFrameworkFactory.newClient(getZkConnectionString(),
                                                        new ExponentialBackoffRetry(1000, 20));
        this.client.start();
        this.asyncClient = AsyncCuratorFramework.wrap(client);

        var gatesPathCreation = Try.ofRunnable(() -> client.create()
                                                           .creatingParentsIfNeeded()
                                                           .withMode(CreateMode.PERSISTENT)
                                                           .forPath(CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH))
                                                           .recover(KeeperException.NodeExistsException.class, Try.VOID);
        var servicesPathCreation = gatesPathCreation.flatMap(success -> Try.ofRunnable(() -> client.create()
                                                                                                   .creatingParentsIfNeeded()
                                                                                                   .withMode(CreateMode.PERSISTENT)
                                                                                                   .forPath(CLUSTER_REGISTRY_SERVICES_ROOT_PATH))
                                                                            .recover(KeeperException.NodeExistsException.class,
                                                                                     Try.VOID));
        var serviceDiscovery = servicesPathCreation.map(success -> ServiceDiscoveryBuilder.builder(String.class)
                                                                                          .basePath(CLUSTER_REGISTRY_SERVICES_ROOT_PATH)
                                                                                          .client(this.client)
                                                                                          .build());
        this.serviceDiscovery = serviceDiscovery.orElse(null,
                                                        error -> { initInfo.getReActorSystem()
                                                                           .logError("Error initializing {} driver",
                                                                                     ZooKeeperDriver.class.getSimpleName(),
                                                                                     error);
                                                                   stop(); });
        if (this.serviceDiscovery != null) {
            initInfo.getReActorSystem().getSystemRemotingRoot().tell(initInfo.getDriverReActor(),
                                                                     new RegistryDriverInitComplete());
        }
    }

    @Override
    public Try<Void> stop() {
        return Try.ofRunnable(() -> {
                    if (this.serviceDiscovery != null) {
                        this.serviceDiscovery.close();
                    }
                    if (this.reActedHierarchy != null) {
                        this.reActedHierarchy.close();
                    }
                    if (this.client != null) {
                        this.client.close();
                    }
                });
    }

    @Override
    public Properties getConfiguration() { return config; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperDriver that = (ZooKeeperDriver) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConfiguration());
    }

    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> {})
                        .reAct(ReActorStop.class, this::onStop)
                        .reAct(RegistryPublicationRequest.class, this::onPublicationRequest)
                        .reAct(RegistrySubscriptionRequest.class, this::onRegistrySubscriptionRequest)
                        .reAct(RegistryUnregisterChannel.class, this::onChannelCancel)
                        .reAct(RegistryServicePublicationRequest.class, this::onServicePublicationRequest)
                        .reAct(RegistryServiceCancellationRequest.class, this::onServiceCancellationRequest)
                        .reAct(ServiceDiscoveryRequest.class, this::onServiceDiscovery)
                        .reAct(ZooKeeperDriver::onSpuriousMessage)
                        .build();
    }

    private String getZkConnectionString() { return getConfiguration().getProperty(ZK_CONNECTION_STRING); }

    private void onServiceCancellationRequest(ReActorContext raCtx,
                                              RegistryServiceCancellationRequest cancellationRequest) {
        if (this.serviceDiscovery != null) {
            getServiceInstance(cancellationRequest.getServiceName(),
                               raCtx.getReActorSystem().getLocalReActorSystemId(), "")
                    .ifSuccessOrElse( this.serviceDiscovery::unregisterService,
                                     error -> raCtx.logError("Unable to unregister service {}",
                                                             cancellationRequest.toString(), error));
        }
    }

    private void onServicePublicationRequest(ReActorContext raCtx, RegistryServicePublicationRequest publishService) {
        if (this.serviceDiscovery == null) {
            return;
        }
        getServiceInstance(publishService.getServiceName(), raCtx.getReActorSystem().getLocalReActorSystemId(),
                           publishService.toSerializedString())
                .map(service -> { this.serviceDiscovery.registerService(service); return null; })
                .ifError(registeringError -> raCtx.getSender()
                                                  .tell(raCtx.getSelf(),
                                                  new RegistryServicePublicationFailed(publishService.getServiceName(),
                                                                                       registeringError)));
    }

    private void onServiceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (this.serviceDiscovery != null) {
            Try.of(() -> this.serviceDiscovery.queryForInstances(request.getServiceName()))
                    .peekFailure(error -> raCtx.logError("Error discovering service {}",
                                                         request.getServiceName(), error))
                    .toOptional()
                    .filter(Predicate.not(Collection::isEmpty))
                    .map(serviceInstances -> toServiceDiscoveryReply(serviceInstances, raCtx.getReActorSystem()))
                    .filter(serviceDiscoveryReply -> !serviceDiscoveryReply.getServiceGates().isEmpty())
                    .ifPresent(serviceDiscoveryReply -> raCtx.reply(ReActorRef.NO_REACTOR_REF, serviceDiscoveryReply));
        }
    }

    private void onChannelCancel(ReActorContext raCtx, RegistryUnregisterChannel cancelRequest) {
        if (this.asyncClient != null) {
            this.asyncClient.delete()
                    .forPath(ZooKeeperDriver.getGatePublicationPath(cancelRequest.getReActorSystemId(),
                            cancelRequest.getChannelId()));
        }
    }

    private void onPublicationRequest(ReActorContext raCtx, RegistryPublicationRequest pubRequest) {
        try {
            if (this.client != null) {
                this.client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(ZooKeeperDriver.getGatePublicationPath(pubRequest.getReActorSystemId(),
                                pubRequest.getChannelId()),
                                encodeProperties(pubRequest.getChannelIdData()));
            }
        } catch (KeeperException.NodeExistsException alreadyPublished) {
            raCtx.logError("Trying to publish on something already there? ", alreadyPublished);
        } catch (Exception nodeCreationError) {
            raCtx.logError("Unable to publish ReActorSystem ", nodeCreationError);
        }
    }

    private void onRegistrySubscriptionRequest(ReActorContext raCtx, RegistrySubscriptionRequest subRequest) {
        if (this.client == null) {
            return;
        }
        this.reActedHierarchy = TreeCache.newBuilder(client, CLUSTER_REGISTRY_ROOT_PATH)
                                         .build();
        try {
            this.reActedHierarchy.start()
                                 .getListenable()
                                 .addListener(getTreeListener(raCtx.getReActorSystem(), raCtx.getSelf()));
            raCtx.getReActorSystem().getSystemRemotingRoot().tell(raCtx.getSelf(),
                                                                  new RegistrySubscriptionComplete());
        } catch (Exception subscriptionError) {
            raCtx.logError("Error starting registry subscription", subscriptionError);
            raCtx.stop();
        }
    }

    private void onStop(ReActorContext raCtx, ReActorStop stopRequest) {
        stop().ifError(error -> raCtx.getReActorSystem()
                                     .logError("Error stopping service registry", error));
    }

    private static void onSpuriousMessage(ReActorContext raCtx, Object message) {
        raCtx.logError("Unrecognized message received in {}", ZooKeeperDriver.class.getSimpleName(),
                       new IllegalStateException(message.toString()));
    }

    private static TreeCacheListener getTreeListener(ReActorSystem reActorSystem, ReActorRef driverReActor) {
        return (curatorFramework, treeCacheEvent) ->
                cacheEventsRouter(curatorFramework, treeCacheEvent, reActorSystem, driverReActor)
                        .thenAccept(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                      .ifError(error -> reActorSystem.logError("Error handling zookeeper event {}",
                                                                                                               treeCacheEvent.toString(),
                                                                                                               error)));
    }

    private static CompletionStage<Try<DeliveryStatus>>
    cacheEventsRouter(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent, ReActorSystem reActorSystem,
                      ReActorRef driverReActor) {
        //Lack of data? Ignore the update
        if (treeCacheEvent.getData() == null || treeCacheEvent.getData().getPath() == null) {
            return SUCCESS;
        }

        return switch (treeCacheEvent.getType()) {
            case CONNECTION_SUSPENDED, CONNECTION_LOST, INITIALIZED -> SUCCESS;
            case CONNECTION_RECONNECTED -> reActorSystem.getSystemRemotingRoot().tell(driverReActor,
                                                                                      new RegistryDriverInitComplete());
            case NODE_ADDED, NODE_UPDATED -> ZooKeeperDriver.shouldProcessUpdate(treeCacheEvent.getData().getPath())
                                             ? ZooKeeperDriver.upsertGate(reActorSystem, driverReActor,
                                                                          treeCacheEvent.getData())
                                             : SUCCESS;
            case NODE_REMOVED -> ZooKeeperDriver.shouldProcessUpdate(treeCacheEvent.getData().getPath())
                                 ? ZooKeeperDriver.removeGate(reActorSystem, driverReActor, treeCacheEvent.getData())
                                 : SUCCESS;
        };
    }

    private static CompletionStage<Try<DeliveryStatus>> removeGate(ReActorSystem reActorSystem, ReActorRef driverReActor,
                                                                   ChildData childData) {
        ZKPaths.PathAndNode reActorSystemNameAndChannelId = ZooKeeperDriver.getGateUpsertPath(childData.getPath());
        Optional<ChannelId> channelId = ChannelId.fromToString(reActorSystemNameAndChannelId.getNode());
        return channelId.map(channel -> reActorSystem.getSystemRemotingRoot()
                                                     .tell(driverReActor,
                                                           new RegistryGateRemoved(reActorSystemNameAndChannelId.getPath().substring(1),
                                                                                   channel)))
                        .orElse(CompletableFuture.completedFuture(Try.ofFailure(new IllegalArgumentException("Unable to decode channel id from " +
                                reActorSystemNameAndChannelId.toString()))));
    }

    private static CompletionStage<Try<DeliveryStatus>> upsertGate(ReActorSystem reActorSystem, ReActorRef driverReActor,
                                                                   ChildData nodeData) {
        ZKPaths.PathAndNode reActorSystemNameAndChannelId = ZooKeeperDriver.getGateUpsertPath(nodeData.getPath());
        String reActorSystemName = reActorSystemNameAndChannelId.getPath().substring(1);
        Optional<ChannelId> channelId = ChannelId.fromToString(reActorSystemNameAndChannelId.getNode());
        Try<Properties> properties = Try.of(() -> ZooKeeperDriver.decodeProperties(nodeData));

        return channelId.map(channel -> properties.map(props -> reActorSystem.getSystemRemotingRoot()
                                                                             .tell(driverReActor,
                                                                                   new RegistryGateUpserted(reActorSystemName,
                                                                                                            channel,
                                                                                                            props)))
                                                  .orElseGet(err -> CompletableFuture.completedFuture(Try.ofFailure(err))))
                        .orElse(CompletableFuture.completedFuture(Try.ofFailure(new IllegalArgumentException("Unable to decode channel id from " +
                                                                                                             reActorSystemNameAndChannelId.toString()))));
    }

    private static String getGatePublicationPath(ReActorSystemId reActorSystemId, ChannelId channelId) {
        return String.format(CLUSTER_GATE_PUBLICATION_PATH, reActorSystemId.getReActorSystemName(),
                             channelId.toString());
    }

    private static Properties decodeProperties(ChildData childData) throws IOException {
        var byteArrayInputStream = new ByteArrayInputStream(childData.getData());
        var properties = new Properties();
        properties.load(byteArrayInputStream);
        return properties;
    }

    private static byte[] encodeProperties(Properties properties) throws IOException {
        var byteArrayOutputStream = new ByteArrayOutputStream();
        properties.store(byteArrayOutputStream,"");
        return byteArrayOutputStream.toByteArray();
    }

    private static boolean shouldProcessUpdate(String updatedPath) {

        if (updatedPath.length() > CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH.length() &&
            updatedPath.startsWith(CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH)) {
            ZKPaths.PathAndNode pathAndNode = ZooKeeperDriver.getGateUpsertPath(updatedPath);
            return ChannelId.fromToString(pathAndNode.getNode()).isPresent();
        }
        return false;
    }

    private static ZKPaths.PathAndNode getGateUpsertPath(String gateUpdateFullPath) {
        return ZKPaths.getPathAndNode(gateUpdateFullPath.substring(CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH.length()));
    }

    private static <PayloadT> Try<ServiceInstance<PayloadT>> getServiceInstance(String serviceName,
                                                                                ReActorSystemId localReActorSystemId,
                                                                                PayloadT servicePayload) {
        return Try.of(() -> ServiceInstance.<PayloadT>builder()
                                           .serviceType(ServiceType.DYNAMIC)
                                           .name(serviceName)
                                           .payload(servicePayload)
                                           .id(localReActorSystemId.getReActorSystemName() + "_" + serviceName)
                                           .build());
    }

    private static ServiceDiscoveryReply toServiceDiscoveryReply(Collection<ServiceInstance<String>> serviceInstances,
                                                                 ReActorSystem localReActorSystem) {

        var servicesReferences = serviceInstances.stream()
                                                         .map(ServiceInstance::getPayload)
                                                         .flatMap(serializedString -> Try.of(() -> RegistryServicePublicationRequest.fromSerializedString(serializedString))
                                                                                         .peekFailure(error -> localReActorSystem.logError("Unable to decode service {}",
                                                                                                                                           serializedString, error))
                                                                                         .stream())
                                                         .map(RegistryServicePublicationRequest::getServiceGate)
                                                         .collect(Collectors.toUnmodifiableSet());
        return new ServiceDiscoveryReply(servicesReferences, localReActorSystem);
    }
}
