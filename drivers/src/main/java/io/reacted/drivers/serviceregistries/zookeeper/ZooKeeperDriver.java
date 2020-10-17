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
import io.reacted.core.messages.serviceregistry.RegistryConnectionLost;
import io.reacted.core.messages.serviceregistry.RegistryDriverInitComplete;
import io.reacted.core.messages.serviceregistry.RegistryGateRemoved;
import io.reacted.core.messages.serviceregistry.RegistryGateUpserted;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdPublicationRequest;
import io.reacted.core.messages.serviceregistry.ServiceCancellationRequest;
import io.reacted.core.messages.serviceregistry.RegistryServicePublicationFailed;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequest;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryComplete;
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
import io.reacted.core.utils.ReActedUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
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

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

@NonNullByDefault
public class ZooKeeperDriver extends ServiceRegistryDriver<ZooKeeperDriverConfig.Builder, ZooKeeperDriverConfig> {
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
    private volatile AsyncCuratorFramework asyncClient;
    @Nullable
    private volatile CuratorCache curatorCache;

    public ZooKeeperDriver(ZooKeeperDriverConfig config) { super(config); }

    @Override
    public int hashCode() {
        return Objects.hash(getConfig());
    }

    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(ReActorStop.class, (raCtx, stop) -> onStop(raCtx))
                        .reAct(ZooKeeperRootPathsCreated.class, this::onRootPathsCreated)
                        .reAct(ReActorSystemChannelIdPublicationRequest.class,
                               (raCtx, systemPubRequest) -> ReActedUtils.rescheduleIf(this::onChannelIdPublicationRequest,
                                                                                      this::isCuratorClientMissing,
                                                                                      getConfig().getReconnectionDelay(),
                                                                                      raCtx, systemPubRequest))
                        .reAct(ReActorSystemChannelIdCancellationRequest.class,
                               (raCtx, cancRequest) -> ReActedUtils.rescheduleIf(this::onChannelIdCancellationRequest,
                                                                                 this::isServiceDiscoveryClientMissing,
                                                                                 getConfig().getReconnectionDelay(),
                                                                                 raCtx, cancRequest))
                        .reAct(SynchronizationWithServiceRegistryRequest.class,
                               (raCtx, syncRequest) -> ReActedUtils.rescheduleIf(this::onSynchronizationWithRegistryRequest,
                                                                                 this::isCuratorClientMissing,
                                                                                 getConfig().getReconnectionDelay(),
                                                                                 raCtx, syncRequest))
                        .reAct(ServicePublicationRequest.class,
                               (raCtx, servicePubRequest) -> ReActedUtils.rescheduleIf(this::onServicePublicationRequest,
                                                                                       this::isServiceDiscoveryClientMissing,
                                                                                       getConfig().getReconnectionDelay(),
                                                                                       raCtx, servicePubRequest))
                        .reAct(ServiceCancellationRequest.class,
                               (raCtx, serviceCancRequest) -> ReActedUtils.rescheduleIf(this::onServiceCancellationRequest,
                                                                                        this::isServiceDiscoveryClientMissing,
                                                                                        getConfig().getReconnectionDelay(),
                                                                                        raCtx, serviceCancRequest))
                        .reAct(ServiceDiscoveryRequest.class,
                               (raCtx, serviceDiscoveryRequest) -> ReActedUtils.rescheduleIf(this::onServiceDiscovery,
                                                                                             this::isServiceDiscoveryClientMissing,
                                                                                             getConfig().getReconnectionDelay(),
                                                                                             raCtx, serviceDiscoveryRequest))
                        .reAct(ZooKeeperDriver::onSpuriousMessage)
                        .build();
    }

    private String getZkConnectionString() {
        return getConfig().getServiceRegistryProperties().getProperty(ZK_CONNECTION_STRING);
    }

    private void onServiceCancellationRequest(ReActorContext raCtx, ServiceCancellationRequest cancellationRequest) {
        getConfig().getAsyncExecutionService()
                   .execute(() -> getServiceInstance(cancellationRequest.getServiceName(),
                                                     raCtx.getReActorSystem().getLocalReActorSystemId(),
                                                     (ServicePublicationRequest)null)
                                    .ifSuccessOrElse(Objects.requireNonNull(this.serviceDiscovery)::unregisterService,
                                                     error -> raCtx.logError("Unable to unregister service {}",
                                                                             cancellationRequest.toString(), error)));
    }

    private void onServicePublicationRequest(ReActorContext raCtx, ServicePublicationRequest serviceInfo) {
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
                                    .ifSuccessOrElse(noVal -> raCtx.logInfo("Service {} published",
                                                                            serviceInfo.getServiceProperties()
                                                                                       .getProperty(ServiceDiscoverySearchFilter.FIELD_NAME_SERVICE_NAME)),
                                                     registeringError -> raCtx.reply(new RegistryServicePublicationFailed(serviceName,
                                                                                                                          registeringError))));
    }

    private void onServiceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        ReActorRef discoveryRequester = raCtx.getSender();
        CompletableFuture.supplyAsync(() ->  queryZooKeeper(raCtx, Objects.requireNonNull(this.serviceDiscovery),
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
        getConfig().getAsyncExecutionService()
                   .execute(() -> Objects.requireNonNull(this.asyncClient)
                                         .delete()
                                         .forPath(ZooKeeperDriver.getGatePublicationPath(cancelRequest.getReActorSystemId(),
                                                                                         cancelRequest.getChannelId())));
    }

    private void onChannelIdPublicationRequest(ReActorContext raCtx,
                                               ReActorSystemChannelIdPublicationRequest pubRequest) {
        CompletableFuture.runAsync(() -> Try.of(() -> createPathIfRequired(Objects.requireNonNull(this.asyncClient),
                                                                           CreateMode.EPHEMERAL,
                                                                           ZooKeeperDriver.getGatePublicationPath(pubRequest.getReActorSystemId(),
                                                                                                                  pubRequest.getChannelId()),
                                                                           encodeProperties(pubRequest.getChannelIdData())))
                                            .ifError(encodeError -> raCtx.logError("Permanent error, unable to encode channel properties {}",
                                                                                   pubRequest.getChannelIdData(), encodeError)),
                                   getConfig().getAsyncExecutionService());
    }

    private void onSynchronizationWithRegistryRequest(ReActorContext raCtx,
                                                      SynchronizationWithServiceRegistryRequest subRequest) {
        if (curatorCache == null) {
            this.curatorCache = CuratorCache.builder(Objects.requireNonNull(asyncClient).unwrap(),
                                                     CLUSTER_REGISTRY_ROOT_PATH)
                                            .withExceptionHandler(Throwable::printStackTrace)
                                            .build();
            Objects.requireNonNull(curatorCache).listenable()
                        .addListener(CuratorCacheListener.builder()
                                                         .forTreeCache(Objects.requireNonNull(asyncClient).unwrap(),
                                                                       getTreeListener(raCtx.getReActorSystem(),
                                                                                       raCtx.getSelf()))
                                                         .build(), getConfig().getAsyncExecutionService());
            Objects.requireNonNull(curatorCache).start();
        }

        raCtx.getReActorSystem()
             .getSystemRemotingRoot()
             .tell(raCtx.getSelf(), new SynchronizationWithServiceRegistryComplete());
    }

    private void onStop(ReActorContext raCtx) {

        Try.ofRunnable(this::shutdownZookeeperConnection)
           .ifError(error -> raCtx.logError("Error stopping service registry", error));
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        if (asyncClient == null) {
            var curatorClient = CuratorFrameworkFactory.newClient(getZkConnectionString(),
                                                                 (int) getConfig().getSessionTimeout()
                                                                                 .toMillis(),
                                                                 (int) getConfig().getConnectionTimeout()
                                                                                  .toMillis(),
                                                                  new ExponentialBackoffRetry((int) getConfig().getReconnectionDelay()
                                                                                                               .toMillis(),
                                                                                              getConfig().getMaxReconnectionAttempts()));
            curatorClient.getConnectionStateListenable()
                         .addListener((curator, newState) -> onConnectionStateChange(raCtx, curator, curatorCache,
                                                                                     newState),
                                                             getConfig().getAsyncExecutionService());
            this.asyncClient = AsyncCuratorFramework.wrap(curatorClient);
        }

        var cacheStart = CompletableFuture.runAsync(() -> Objects.requireNonNull(asyncClient)
                                                                 .unwrap().start(),
                                                    getConfig().getAsyncExecutionService());
        cacheStart.thenCompose(noVal -> createPathIfRequired(Objects.requireNonNull(asyncClient), CreateMode.PERSISTENT,
                                                             CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH, NO_PAYLOAD))
                  .thenCompose(isPathCreated ->  isPathCreated
                                                 ? createPathIfRequired(Objects.requireNonNull(asyncClient),
                                                                        CreateMode.PERSISTENT,
                                                                        CLUSTER_REGISTRY_SERVICES_ROOT_PATH, NO_PAYLOAD)
                                                 : CompletableFuture.completedFuture(false))
                  .thenCompose(isPathCreated -> (isPathCreated
                                                 ?  raCtx.selfTell(new ZooKeeperRootPathsCreated())
                                                 :  CompletableFuture.completedStage(raCtx.rescheduleMessage(init,
                                                                                                             getConfig().getReconnectionDelay())))
                                                                     .thenAccept(n -> {}));
    }

    private void onRootPathsCreated(ReActorContext raCtx, ZooKeeperRootPathsCreated created) {
        if (serviceDiscovery != null) {
            raCtx.getReActorSystem().getSystemRemotingRoot().tell(raCtx.getSelf(),
                                                                  new RegistryDriverInitComplete());
            return;
        }
        CompletableFuture.supplyAsync(() -> ServiceDiscoveryBuilder.builder(ServicePublicationRequest.class)
                                                                   .basePath(CLUSTER_REGISTRY_SERVICES_ROOT_PATH)
                                                                   .client(Objects.requireNonNull(asyncClient)
                                                                                  .unwrap())
                                                                   .build(),
                                      getConfig().getAsyncExecutionService())
                         .thenAccept(serviceDiscovery -> this.serviceDiscovery = serviceDiscovery)
                         .thenAccept(noVal -> raCtx.getReActorSystem().getSystemRemotingRoot()
                                                   .tell(raCtx.getSelf(), new RegistryDriverInitComplete()));
    }

    private boolean isCuratorClientMissing() { return asyncClient == null; }

    private boolean isServiceDiscoveryClientMissing() { return serviceDiscovery == null; }

    private void shutdownZookeeperConnection() throws IOException {
        if (serviceDiscovery != null) {
            Objects.requireNonNull(serviceDiscovery).close();
        }
        if (curatorCache != null) {
            Objects.requireNonNull(curatorCache).close();
        }
        if (asyncClient != null) {
            Objects.requireNonNull(asyncClient).unwrap().close();
        }
        this.serviceDiscovery = null;
        this.curatorCache = null;
        this.asyncClient = null;
    }

    private static CompletionStage<Boolean> createPathIfRequired(AsyncCuratorFramework asyncClient,
                                                                CreateMode creationMode, String pathToCreate,
                                                                byte[] payload) {
        return checkPathIfExists(asyncClient, pathToCreate)
                          .thenCompose(exists -> exists
                                                 ? CompletableFuture.completedFuture(true)
                                                 : createPath(asyncClient, creationMode, pathToCreate, payload))
                          .thenApply(Objects::nonNull)
                          .exceptionally(error -> false);
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

    private static CompletionStage<Boolean> createPath(AsyncCuratorFramework asyncClient, CreateMode creationMode,
                                                      String pathToCreate, byte[] payload) {
        return asyncClient.create()
                          .withOptions(Set.of(CreateOption.createParentsIfNeeded), creationMode)
                          .forPath(pathToCreate, payload)
                          .toCompletableFuture()
                          .thenApply(Objects::nonNull)
                          .exceptionally(error -> false);
    }

    private static TreeCacheListener getTreeListener(ReActorSystem reActorSystem, ReActorRef driverReActor) {
        return (curatorFramework, treeCacheEvent) ->
                ifNotDelivered(cacheEventsRouter(curatorFramework, treeCacheEvent, reActorSystem, driverReActor),
                              error -> reActorSystem.logError("Error handling zookeeper event {}",
                                                              treeCacheEvent.toString(), error));
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

    private static void onConnectionStateChange(ReActorContext raCtx, CuratorFramework curator,
                                                @Nullable CuratorCache curatorCache, ConnectionState newState) {

        switch (newState) {
            case LOST, SUSPENDED -> raCtx.getReActorSystem().getSystemRemotingRoot()
                                         .tell(raCtx.getSelf(), new RegistryConnectionLost());
            case RECONNECTED -> Optional.ofNullable(curatorCache)
                                        .map(CuratorCache::stream)
                                        .ifPresent(children -> children.forEachOrdered(child -> refreshGate(raCtx.getSelf(),
                                                                                                            raCtx.getReActorSystem(),
                                                                                                            curator,
                                                                                                            child)));
            case CONNECTED, READ_ONLY -> {}
        }
    }

    private static void refreshGate(ReActorRef zkDriver, ReActorSystem reActorSystem, CuratorFramework curator,
                                    ChildData childData) {
        cacheEventsRouter(curator, new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED, childData),
                          reActorSystem, zkDriver);

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
                                           .id(serviceName + "@" + localReActorSystemId.getReActorSystemName())
                                           .build());
    }

    //Side effect on the input properties
    private static Properties patchServiceProperties(Properties serviceProperties,
                                                     String key, Object value) {
        serviceProperties.merge(key, value, (oldObj, newObj) -> value);
        return serviceProperties;
    }
}
