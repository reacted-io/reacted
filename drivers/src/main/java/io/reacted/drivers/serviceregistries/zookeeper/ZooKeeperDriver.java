/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.serviceregistries.zookeeper;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.serviceregistry.FilterServiceDiscoveryRequest;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdPublicationError;
import io.reacted.core.messages.serviceregistry.RegistryDriverInitComplete;
import io.reacted.core.messages.serviceregistry.RegistryGateRemoved;
import io.reacted.core.messages.serviceregistry.RegistryGateUpserted;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdPublicationRequest;
import io.reacted.core.messages.serviceregistry.ServiceCancellationRequest;
import io.reacted.core.messages.serviceregistry.RegistryServicePublicationFailed;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequest;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequestError;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryComplete;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryFailed;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryRequest;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdCancellationRequest;
import io.reacted.core.messages.services.FilterItem;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.ExistsOption;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.zookeeper.CreateMode;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZooKeeperDriver extends ServiceRegistryDriver<ZooKeeperDriverCfg.Builder, ZooKeeperDriverCfg> {
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
    private static final byte[] NO_PAYLOAD = new byte[0];
    @Nullable
    private volatile ServiceDiscovery<ServicePublicationRequest> serviceDiscovery;
    @Nullable
    private AsyncCuratorFramework asyncClient;
    @Nullable
    private TreeCache reActedHierarchy;

    public ZooKeeperDriver(ZooKeeperDriverCfg cfg) {
        super(cfg);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperDriver that = (ZooKeeperDriver) o;
        return Objects.equals(getConfig().getServiceRegistryProperties(),
                              that.getConfig().getServiceRegistryProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConfig());
    }

    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> this.onInit(raCtx))
                        .reAct(ReActorStop.class, (raCtx, stop) -> this.onStop(raCtx))
                        .reAct(ZooKeeperRootPathsCreated.class, this::onRootPathsCreated)
                        .reAct(ReActorSystemChannelIdPublicationRequest.class, this::onChannelIdPublicationRequest)
                        .reAct(ReActorSystemChannelIdCancellationRequest.class, this::onChannelIdCancellationRequest)
                        .reAct(SynchronizationWithServiceRegistryRequest.class, this::onSynchronizationWithRegistryRequest)
                        .reAct(ServicePublicationRequest.class, this::onServicePublicationRequest)
                        .reAct(ServiceCancellationRequest.class, this::onServiceCancellationRequest)
                        .reAct(ServiceDiscoveryRequest.class, this::onServiceDiscovery)
                        .reAct(ZooKeeperDriver::onSpuriousMessage)
                        .build();
    }

    private String getZkConnectionString() {
        return getConfig().getServiceRegistryProperties().getProperty(ZK_CONNECTION_STRING);
    }

    private void onServiceCancellationRequest(ReActorContext raCtx,
                                              ServiceCancellationRequest cancellationRequest) {
        if (this.serviceDiscovery == null) {
            return;
        }
        getConfig().getAsyncExecutionService()
                   .execute(() -> getServiceInstance(cancellationRequest.getServiceName(),
                                                     raCtx.getReActorSystem().getLocalReActorSystemId(),
                                                     (ServicePublicationRequest)null)
                                    .ifSuccessOrElse(Objects.requireNonNull(this.serviceDiscovery)::unregisterService,
                                                     error -> raCtx.logError("Unable to unregister service {}",
                                                                             cancellationRequest.toString(), error)));
    }

    private void onServicePublicationRequest(ReActorContext raCtx, ServicePublicationRequest serviceInfo) {
        if (this.serviceDiscovery == null) {
            raCtx.rescheduleMessage(serviceInfo, getConfig().getPingInterval())
                 .ifError(error -> raCtx.reply(new ServicePublicationRequestError()));
            return;
        }
        String serviceName = serviceInfo.getServiceProperties()
                                        .getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME);
        if (StringUtils.isBlank(serviceName)) {
            raCtx.logError("Skipping publication attempt of an invalid service name {}", serviceName);
            raCtx.reply(new RegistryServicePublicationFailed(serviceName,
                                                             new IllegalArgumentException("Invalid name: blank")));
            return;
        }
        getConfig().getAsyncExecutionService()
                   .execute(() -> getServiceInstance(serviceName, raCtx.getReActorSystem()
                                                                       .getLocalReActorSystemId(), serviceInfo)
                                    .ifSuccess(Objects.requireNonNull(this.serviceDiscovery)::registerService)
                                    .ifError(registeringError -> raCtx.reply(new RegistryServicePublicationFailed(serviceName,
                                                                                                                  registeringError))));
    }

    private void onServiceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (this.serviceDiscovery == null) {
            return;
        }
        ReActorRef discoveryRequester = raCtx.getSender();
        CompletableFuture.supplyAsync(() -> queryZooKeeper(raCtx, Objects.requireNonNull(this.serviceDiscovery),
                                                           request.getSearchFilter()),
                                      getConfig().getAsyncExecutionService())
                         .thenAccept(filterItemSet -> raCtx.getReActorSystem().getSystemRemotingRoot()
                                                           .tell(discoveryRequester,
                                                                 new FilterServiceDiscoveryRequest(request.getSearchFilter(),
                                                                                                   filterItemSet)));
    }

    private static Set<FilterItem> queryZooKeeper(ReActorContext raCtx,
                                                  ServiceDiscovery<ServicePublicationRequest> synchronousServiceDiscovery,
                                                  ServiceDiscoverySearchFilter serviceFilter) {
        var nameMatchingServices = Try.of(() -> synchronousServiceDiscovery.queryForInstances(serviceFilter.getServiceName()))
           .orElse(List.of(), error -> raCtx.logError("Error discovering service {}",
                                                      serviceFilter.getServiceName(), error));
        return nameMatchingServices.stream()
                                   .map(serviceInstance -> new FilterItem(serviceInstance.getPayload().getServiceGate(),
                                                                          patchServiceProperties(serviceInstance.getPayload().getServiceProperties(),
                                                                                                 ServiceDiscoverySearchFilter.FIELD_NAME_IP_ADDRESS,
                                                                                                 serviceInstance.getAddress())))
                                   .collect(Collectors.toUnmodifiableSet());
    }

    private void onChannelIdCancellationRequest(ReActorContext raCtx,
                                                ReActorSystemChannelIdCancellationRequest cancelRequest) {
        if (this.asyncClient == null) {
            return;
        }
        getConfig().getAsyncExecutionService()
                   .execute(() -> this.asyncClient.delete()
                                                  .forPath(ZooKeeperDriver.getGatePublicationPath(cancelRequest.getReActorSystemId(),
                                                                                                  cancelRequest.getChannelId())));
    }

    private void onChannelIdPublicationRequest(ReActorContext raCtx,
                                               ReActorSystemChannelIdPublicationRequest pubRequest) {
        if (this.asyncClient == null) {
            raCtx.rescheduleMessage(pubRequest, getConfig().getPingInterval())
                 .peekFailure(error -> raCtx.logError("Critic! {} cannot reschedule a channel publication",
                                                      getConfig().getReActorName(), error))
                 .ifError(error -> raCtx.reply(new ReActorSystemChannelIdPublicationError()));
            return;
        }
        CompletableFuture.runAsync(() -> Try.of(() -> createPathIfRequired(this.asyncClient,  CreateMode.EPHEMERAL,
                                                                           ZooKeeperDriver.getGatePublicationPath(pubRequest.getReActorSystemId(),
                                                                                                                  pubRequest.getChannelId()),
                                                                           encodeProperties(pubRequest.getChannelIdData())))
                                            .ifError(encodeError -> raCtx.logError("Permanent error, unable to encode channel properties {}",
                                                                                   pubRequest.getChannelIdData(), encodeError)),
                                   getConfig().getAsyncExecutionService());
    }

    private void onSynchronizationWithRegistryRequest(ReActorContext raCtx,
                                                      SynchronizationWithServiceRegistryRequest subRequest) {
        if (this.asyncClient == null) {
            raCtx.rescheduleMessage(subRequest, getConfig().getPingInterval())
                 .ifError(error -> raCtx.reply(new SynchronizationWithServiceRegistryFailed()));
            return;
        }
        if (this.reActedHierarchy == null) {
            this.reActedHierarchy = TreeCache.newBuilder(this.asyncClient.unwrap(), CLUSTER_REGISTRY_ROOT_PATH)
                                             .build();
        }
        try {
            this.reActedHierarchy.start()
                                 .getListenable()
                                 .addListener(getTreeListener(raCtx.getReActorSystem(), raCtx.getSelf()));
            raCtx.getReActorSystem()
                 .getSystemRemotingRoot()
                 .tell(raCtx.getSelf(), new SynchronizationWithServiceRegistryComplete());
        } catch (Exception subscriptionError) {
            raCtx.logError("Error starting registry subscription", subscriptionError);
            raCtx.rescheduleMessage(subRequest, getConfig().getPingInterval())
                 .ifError(error -> raCtx.reply(new SynchronizationWithServiceRegistryFailed()));
        }
    }

    private void onStop(ReActorContext raCtx) {

        Try.ofRunnable(this::shutdownZookeeperConnection)
           .ifError(error -> raCtx.logError("Error stopping service registry", error));
    }

    private void onInit(ReActorContext raCtx) {
        this.asyncClient = AsyncCuratorFramework.wrap(CuratorFrameworkFactory.newClient(getZkConnectionString(),
                                                                                        20000, 20000,
                                                                                        new ExponentialBackoffRetry(1000,
                                                                                                                    20)));

        CompletableFuture.runAsync(() -> this.asyncClient.unwrap().start(), getConfig().getAsyncExecutionService())
                         .thenAccept(noVal -> createPathIfRequired(this.asyncClient, CreateMode.PERSISTENT,
                                                                   CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH,
                                                                   NO_PAYLOAD))
                         .thenCompose(createdPath -> createPathIfRequired(this.asyncClient, CreateMode.PERSISTENT,
                                                                          CLUSTER_REGISTRY_SERVICES_ROOT_PATH,
                                                                          NO_PAYLOAD))
                         .thenAccept(createdPath -> raCtx.selfTell(new ZooKeeperRootPathsCreated()));
    }

    private void onRootPathsCreated(ReActorContext raCtx, ZooKeeperRootPathsCreated created) {
        CompletableFuture.supplyAsync(() -> ServiceDiscoveryBuilder.builder(ServicePublicationRequest.class)
                                                                   .basePath(CLUSTER_REGISTRY_SERVICES_ROOT_PATH)
                                                                   .client(Objects.requireNonNull(this.asyncClient).unwrap())
                                                                   .build(),
                                      getConfig().getAsyncExecutionService())
                         .thenAccept(serviceDiscovery -> this.serviceDiscovery = serviceDiscovery)
                         .thenAccept(noVal -> raCtx.getReActorSystem().getSystemRemotingRoot()
                                                   .tell(raCtx.getSelf(), new RegistryDriverInitComplete()));
    }

    private void shutdownZookeeperConnection() throws IOException {
        if (this.serviceDiscovery != null) {
            Objects.requireNonNull(this.serviceDiscovery).close();
        }
        if (this.reActedHierarchy != null) {
            this.reActedHierarchy.close();
        }
        if (this.asyncClient != null) {
            this.asyncClient.unwrap().close();
        }
    }

    private static CompletionStage<String> createPathIfRequired(AsyncCuratorFramework asyncClient,
                                                                CreateMode creationMode, String pathToCreate,
                                                                byte[] payload) {
        return checkPathIfExists(asyncClient, pathToCreate)
                          .thenCompose(exists -> exists
                                                 ? CompletableFuture.completedFuture(null)
                                                 : createPath(asyncClient, creationMode, pathToCreate, payload))
                          .exceptionally(error -> { error.printStackTrace(); return null; });
    }

    private static CompletionStage<Boolean> checkPathIfExists(AsyncCuratorFramework asyncClient, String pathToCreate) {
        return asyncClient.checkExists()
                          .withOptions(Set.of(ExistsOption.createParentsIfNeeded))
                          .forPath(pathToCreate)
                          .toCompletableFuture()
                          .thenApply(Objects::nonNull)
                          .exceptionally(error ->  false);
    }

    private static void onSpuriousMessage(ReActorContext raCtx, Object message) {
        raCtx.logError("Unrecognized message received in {}", ZooKeeperDriver.class.getSimpleName(),
                       new IllegalStateException(message.toString()));
    }

    private static CompletionStage<String> createPath(AsyncCuratorFramework asyncClient, CreateMode creationMode,
                                                      String pathToCreate, byte[] payload) {
        return asyncClient.create()
                          .withOptions(Set.of(CreateOption.createParentsIfNeeded), creationMode)
                          .forPath(pathToCreate, payload)
                          .toCompletableFuture();
    }

    private TreeCacheListener getTreeListener(ReActorSystem reActorSystem, ReActorRef driverReActor) {
        return (curatorFramework, treeCacheEvent) ->
                getConfig().getAsyncExecutionService()
                           .execute(() -> cacheEventsRouter(curatorFramework, treeCacheEvent, reActorSystem, driverReActor)
                                                .thenAcceptAsync(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                                                   .ifError(error -> reActorSystem.logError("Error handling zookeeper event {}",
                                                                                                                                            treeCacheEvent.toString(),
                                                                                                                                            error))));
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
        System.out.println("Removing gate? " + childData.getPath());
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
                                                                                @Nullable PayloadT servicePayload) {
        return Try.of(() -> ServiceInstance.<PayloadT>builder()
                                           .serviceType(ServiceType.DYNAMIC)
                                           .name(serviceName)
                                           .payload(servicePayload)
                                           .id(localReActorSystemId.getReActorSystemName() + "|" + serviceName)
                                           .build());
    }

    //Side effect on the input properties
    private static Properties patchServiceProperties(Properties serviceProperties,
                                                     String key, Object value) {
        serviceProperties.merge(key, value, (oldObj, newObj) -> value);
        return serviceProperties;
    }
}
