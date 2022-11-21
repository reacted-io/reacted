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
import io.reacted.core.messages.serviceregistry.DuplicatedPublicationError;
import io.reacted.core.messages.serviceregistry.FilterServiceDiscoveryRequest;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdCancellationRequest;
import io.reacted.core.messages.serviceregistry.ReActorSystemChannelIdPublicationRequest;
import io.reacted.core.messages.serviceregistry.RegistryConnectionLost;
import io.reacted.core.messages.serviceregistry.RegistryDriverInitComplete;
import io.reacted.core.messages.serviceregistry.RegistryGateRemoved;
import io.reacted.core.messages.serviceregistry.RegistryGateUpserted;
import io.reacted.core.messages.serviceregistry.RegistryServicePublicationFailed;
import io.reacted.core.messages.serviceregistry.ServiceCancellationRequest;
import io.reacted.core.messages.serviceregistry.ServicePublicationRequest;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryComplete;
import io.reacted.core.messages.serviceregistry.SynchronizationWithServiceRegistryRequest;
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
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
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
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@NonNullByDefault
public class ZooKeeperDriver extends ServiceRegistryDriver<ZooKeeperDriverConfig.Builder, ZooKeeperDriverConfig> {
    /*
        Reacted Cluster Root -----> Reactor Systems ----------> Reactor System Name {Instance Marker}
                             |
                             |
                             -----> Reactor Systems Gates ---> Reactor System Name # Channel Id { Channel Properties}
                             |
                             |
                             |----> Services { Service Publication Request }
     */
    private static final String REACTED_CLUSTER_ROOT = ZKPaths.makePath("", "REACTED_CLUSTER_ROOT");
    private static final String REACTOR_SYSTEMS_GATES_PATH = ZKPaths.makePath(REACTED_CLUSTER_ROOT,
                                                                              "REACTOR_SYSTEMS_GATES");
    private static final String REACTOR_SYSTEMS_PATH = ZKPaths.makePath(REACTED_CLUSTER_ROOT, "REACTOR_SYSTEMS");
    private static final String SERVICES_PATH = ZKPaths.makePath(REACTED_CLUSTER_ROOT, "REACTED_SERVICES");

    private static final String NEW_REACTOR_SYSTEM_PATH_FORMAT = ZKPaths.makePath(REACTOR_SYSTEMS_PATH, "%s");
    private static final String REACTORSYSTEM_TO_CHANNEL_SEPARATOR = "#";
    private static final String NEW_REACTOR_SYSTEM_GATE_PATH_FORMAT = ZKPaths.makePath(REACTOR_SYSTEMS_GATES_PATH,
                                                                                       "%s" +
                                                                                       REACTORSYSTEM_TO_CHANNEL_SEPARATOR +
                                                                                       "%s");

    private static final byte[] NO_PAYLOAD = new byte[0];
    private final UUID reActorSystemInstanceMarker;
    @Nullable
    private volatile ServiceDiscovery<ServicePublicationRequest> serviceDiscovery;
    @Nullable
    private volatile AsyncCuratorFramework asyncClient;
    @Nullable
    private volatile CuratorCache curatorCache;

    public ZooKeeperDriver(ZooKeeperDriverConfig config) {
        super(config);
        this.reActorSystemInstanceMarker = UUID.randomUUID();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConfig());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ZooKeeperDriver &&
               getConfig().equals(((ZooKeeperDriver) o).getConfig());
    }

    @Nonnull
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
    private void onServiceCancellationRequest(ReActorContext raCtx, ServiceCancellationRequest cancellationRequest) {
        runAsync(() -> getServiceInstance(cancellationRequest.getServiceName(),
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
        runAsync(() -> getServiceInstance(serviceName, raCtx.getReActorSystem()
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
        runAsync(() -> queryZooKeeper(raCtx, Objects.requireNonNull(this.serviceDiscovery), request.getSearchFilter()))
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
        Objects.requireNonNull(asyncClient)
                              .delete()
                              .forPath(ZooKeeperDriver.getGatePublicationPath(cancelRequest.getReActorSystemId(),
                                                                              cancelRequest.getChannelId()));
    }

    private void onChannelIdPublicationRequest(ReActorContext raCtx,
                                               ReActorSystemChannelIdPublicationRequest pubRequest) {
        try {
            createPathIfRequired(Objects.requireNonNull(asyncClient),
                                 CreateMode.EPHEMERAL,
                                 ZooKeeperDriver.getGatePublicationPath(
                                     pubRequest.getReActorSystemId(),
                                     pubRequest.getChannelId()),
                                 ObjectUtils.toBytes(pubRequest.getChannelIdData()))
                .toCompletableFuture()
                .exceptionally(encodeError -> {
                    raCtx.logError("Permanent error, unable to encode channel properties {}",
                                   pubRequest.getChannelIdData(), encodeError);
                    return false;
                });
        } catch (IOException serializationError) {
            raCtx.logError("Unable to serialize gate properties {}", pubRequest.getChannelId(), serializationError);
        }
    }

    private void onSynchronizationWithRegistryRequest(ReActorContext raCtx,
                                                      SynchronizationWithServiceRegistryRequest subRequest) {
        CompletableFuture<Void> cacheStarted = CompletableFuture.completedFuture(null);
        if (curatorCache == null) {
            this.curatorCache = CuratorCache.builder(Objects.requireNonNull(asyncClient).unwrap(), REACTED_CLUSTER_ROOT)
                                            .withExceptionHandler(error -> raCtx.logError("ZooKeeper Cache error",
                                                                                          error))
                                            .build();
            Objects.requireNonNull(curatorCache).listenable()
                        .addListener(CuratorCacheListener.builder()
                                                         .forTreeCache(Objects.requireNonNull(asyncClient).unwrap(),
                                                                       getTreeListener(raCtx.getReActorSystem(),
                                                                                       raCtx.getSelf()))
                                                         .build(), getConfig().getAsyncExecutionService());
            cacheStarted = runAsync(() -> Objects.requireNonNull(curatorCache).start());
        }
        cacheStarted.thenAccept(noVal -> raCtx.getReActorSystem()
                                              .getSystemRemotingRoot()
                                              .tell(raCtx.getSelf(), new SynchronizationWithServiceRegistryComplete()));
    }

    private void onStop(ReActorContext raCtx) {

        Try.ofRunnable(this::shutdownZookeeperConnection)
           .ifError(error -> raCtx.logError("Error stopping service registry", error));
        raCtx.getReActorSystem().getSystemRemotingRoot()
             .tell(raCtx.getSelf(), new RegistryConnectionLost());
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        if (asyncClient == null) {
            this.asyncClient = createCuratorAsyncClient(raCtx);
        }
        asyncClient.unwrap().start();
        try {
            asyncClient.unwrap().blockUntilConnected();
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            raCtx.stop();
            return;
        }
        CompletableFuture.allOf(createPathIfRequired(asyncClient, CreateMode.PERSISTENT, REACTOR_SYSTEMS_PATH, NO_PAYLOAD),
                                createPathIfRequired(asyncClient, CreateMode.PERSISTENT, SERVICES_PATH, NO_PAYLOAD),
                                createPathIfRequired(asyncClient, CreateMode.PERSISTENT, REACTOR_SYSTEMS_GATES_PATH, NO_PAYLOAD))
                         .thenAccept(pathCreated -> raCtx.selfTell(new ZooKeeperRootPathsCreated()))
                         .exceptionally(initError -> {
                             raCtx.logError("Unable to init {} ", ZooKeeperDriver.class.getSimpleName(), initError);
                             raCtx.rescheduleMessage(init, getConfig().getReconnectionDelay());
                             return null;
                         });
    }

    private AsyncCuratorFramework createCuratorAsyncClient(ReActorContext raCtx) {
        var retryPolicy = new ExponentialBackoffRetry((int) getConfig().getReconnectionDelay().toMillis(),
                                                      getConfig().getMaxReconnectionAttempts());
        int sessionTimeout = (int)getConfig().getSessionTimeout().toMillis();
        int connectionTimeout = (int) getConfig().getConnectionTimeout().toMillis();
        var curatorClient = CuratorFrameworkFactory.newClient(getConfig().getConnectionString(),
                                                              sessionTimeout, connectionTimeout, retryPolicy);
        curatorClient.getConnectionStateListenable()
                     .addListener((curator, newState) -> onConnectionStateChange(raCtx, curator, curatorCache,
                                                                                 newState,
                                                                                 reActorSystemInstanceMarker),
                                  getConfig().getAsyncExecutionService());
        return AsyncCuratorFramework.wrap(curatorClient);
    }

    private void onRootPathsCreated(ReActorContext raCtx, ZooKeeperRootPathsCreated created) {
        isReActorSystemUnique(raCtx.getReActorSystem().getLocalReActorSystemId(),
                              Objects.requireNonNull(asyncClient), reActorSystemInstanceMarker)
                .thenApply(isUnique -> isUnique
                                       ? serviceDiscovery == null
                                         ? setupServiceDiscovery(raCtx)
                                         : raCtx.getReActorSystem().getSystemRemotingRoot().tell(raCtx.getSelf(),
                                                                                                 new RegistryDriverInitComplete())
                                       : onDuplicateReactorSystem(raCtx));

    }

    private CompletableFuture<Boolean> isReActorSystemUnique(ReActorSystemId reActorSystemId,
                                                                    AsyncCuratorFramework asyncClient,
                                                                    UUID reActorSystemInstanceMarker) {
        return isReActorSystemUnique(getReactorSystemPublicationPath(reActorSystemId),
                                     Objects.requireNonNull(asyncClient),
                                     reActorSystemInstanceMarker);
    }
    private CompletableFuture<Boolean> isReActorSystemUnique(String reActorSystemEntryPath,
                                                                    AsyncCuratorFramework curatorAsyncClient,
                                                                    UUID reActorSystemInstanceMarker) {
        return createPath(curatorAsyncClient, CreateMode.EPHEMERAL, reActorSystemEntryPath,
                          reActorSystemInstanceMarker.toString().getBytes())
                .toCompletableFuture()
                .thenCompose(hasBeenCreated -> hasBeenCreated
                                               ? CompletableFuture.completedFuture(true)
                                               : isThisDriverReconnecting(reActorSystemEntryPath,
                                                                          curatorAsyncClient,
                                                                          reActorSystemInstanceMarker));
    }

    private static CompletableFuture<Boolean> isThisDriverReconnecting(String reActorSystemPath,
                                                                       AsyncCuratorFramework curatorAsyncClient,
                                                                       UUID reActorSystemInstanceMarker) {
        return curatorAsyncClient.getData()
                                 .forPath(reActorSystemPath)
                                 .toCompletableFuture()
                                 .thenApply(remoteMarkerData -> isThisInstanceMarker(remoteMarkerData,
                                                                                     reActorSystemInstanceMarker));
    }

    private static boolean isThisInstanceMarker(@Nullable byte[] remoteMarkerData,
                                                UUID reActorSystemInstanceMarker) {
        return remoteMarkerData != null &&
               Try.of(() -> UUID.fromString(new String(remoteMarkerData)))
                  .map(reActorSystemInstanceMarker::equals)
                  .orElse(false);
    }
    private CompletionStage<DeliveryStatus> setupServiceDiscovery(ReActorContext raCtx) {
        return runAsync(() -> ServiceDiscoveryBuilder.builder(ServicePublicationRequest.class)
                                                     .basePath(SERVICES_PATH)
                                                     .client(Objects.requireNonNull(asyncClient).unwrap())
                                                     .build())
                        .thenAccept(serviceDiscovery -> this.serviceDiscovery = serviceDiscovery)
                        .thenAccept(noVal -> UnChecked.runnable(serviceDiscovery::start).run())
                        .thenApply(noVal -> raCtx.getReActorSystem()
                                                 .getSystemRemotingRoot()
                                                 .tell(raCtx.getSelf(), new RegistryDriverInitComplete()));
    }
    private static DeliveryStatus onDuplicateReactorSystem(ReActorContext raCtx) {
        return raCtx.getReActorSystem()
                    .getSystemRemotingRoot()
                    .tell(raCtx.getSelf(), new DuplicatedPublicationError());
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

    private CompletableFuture<Boolean> createPathIfRequired(AsyncCuratorFramework asyncClient,
                                                            CreateMode creationMode, String pathToCreate,
                                                            @Nullable byte[] payload) {
        return createPath(asyncClient, creationMode, pathToCreate, payload)
                .exceptionally(NodeExistsException.class::isInstance);
    }

    private static void onSpuriousMessage(ReActorContext raCtx, Object message) {
        raCtx.logError("Unrecognized message received in {}", ZooKeeperDriver.class.getSimpleName(),
                       new IllegalStateException(message.toString()));
    }

    private CompletableFuture<Boolean> createPath(AsyncCuratorFramework asyncClient, CreateMode creationMode,
                                                  String pathToCreate, @Nullable byte[] payload) {
        return asyncClient.create()
                          .withOptions(Set.of(CreateOption.createParentsIfNeeded), creationMode)
                          .forPath(pathToCreate, payload)
                          .toCompletableFuture()
                          .thenApply(Objects::nonNull);
    }
    private static TreeCacheListener getTreeListener(ReActorSystem reActorSystem, ReActorRef driverReActor) {
        return (curatorFramework, treeCacheEvent) -> {
            DeliveryStatus deliveryStatus = cacheEventsRouter(treeCacheEvent, reActorSystem, driverReActor);
            if (!deliveryStatus.isSent()) {
                reActorSystem.logError("Error handling zookeeper event {} {}" ,
                                       treeCacheEvent.toString(), deliveryStatus);
            }
        };
    }

    private static DeliveryStatus
    cacheEventsRouter(TreeCacheEvent treeCacheEvent, ReActorSystem reActorSystem,
                      ReActorRef driverReActor) {
        //Lack of data? Ignore the update
        if (treeCacheEvent.getData() == null || treeCacheEvent.getData().getPath() == null) {
            return DeliveryStatus.DELIVERED;
        }

        return switch (treeCacheEvent.getType()) {
            case CONNECTION_SUSPENDED, CONNECTION_LOST, INITIALIZED -> DeliveryStatus.DELIVERED;
            case CONNECTION_RECONNECTED -> reActorSystem.getSystemRemotingRoot()
                    .tell(driverReActor, new RegistryDriverInitComplete());
            case NODE_ADDED, NODE_UPDATED -> ZooKeeperDriver.shouldProcessUpdate(treeCacheEvent.getData().getPath())
                    ? ZooKeeperDriver.upsertGate(reActorSystem,
                    driverReActor,
                    treeCacheEvent.getData())
                    : DeliveryStatus.DELIVERED;
            case NODE_REMOVED -> ZooKeeperDriver.shouldProcessUpdate(treeCacheEvent.getData().getPath())
                    ? ZooKeeperDriver.removeGate(reActorSystem, driverReActor,
                    treeCacheEvent.getData())
                    : DeliveryStatus.DELIVERED;
        };
    }

    private static DeliveryStatus removeGate(ReActorSystem reActorSystem, ReActorRef driverReActor,
                                             ChildData childData) {
        ZKPaths.PathAndNode reActorSystemNameAndChannelId = ZooKeeperDriver.getGateUpsertPath(childData.getPath());
        Optional<ChannelId> channelId = ChannelId.fromToString(reActorSystemNameAndChannelId.getNode());

        return channelId.map(channel -> reActorSystem.getSystemRemotingRoot()
                                                     .tell(driverReActor,
                                                           new RegistryGateRemoved(reActorSystemNameAndChannelId.getPath().substring(1),
                                                                                   channel)))
                        .orElse(DeliveryStatus.NOT_DELIVERED);
    }

    private static DeliveryStatus upsertGate(ReActorSystem reActorSystem, ReActorRef driverReActor,
                                                              ChildData nodeData) {
        ZKPaths.PathAndNode reActorSystemNameAndChannelId = ZooKeeperDriver.getGateUpsertPath(nodeData.getPath());
        String reActorSystemName = reActorSystemNameAndChannelId.getPath().substring(1);
        Optional<ChannelId> channelId = ChannelId.fromToString(reActorSystemNameAndChannelId.getNode());
        Try<Properties> properties = Try.of(() -> ObjectUtils.fromBytes(nodeData.getData()));

        return channelId.map(channel -> properties.map(props -> reActorSystem.getSystemRemotingRoot()
                                                                             .tell(driverReActor,
                                                                                   new RegistryGateUpserted(reActorSystemName,
                                                                                                            channel,
                                                                                                            props)))
                                                  .orElse(DeliveryStatus.NOT_DELIVERED))
                        .orElse(DeliveryStatus.NOT_DELIVERED);
    }

    private void onConnectionStateChange(ReActorContext raCtx, CuratorFramework curator,
                                         @Nullable CuratorCache curatorCache, ConnectionState newState,
                                         UUID reActorSystemInstanceMarker) {
        switch (newState) {
            case LOST, SUSPENDED -> raCtx.getReActorSystem()
                                         .getSystemRemotingRoot()
                                         .tell(raCtx.getSelf(), new RegistryConnectionLost());
            case RECONNECTED -> isReActorSystemUnique(raCtx.getReActorSystem().getLocalReActorSystemId(),
                    AsyncCuratorFramework.wrap(curator),
                    reActorSystemInstanceMarker)
                    .thenAccept(isUnique -> {
                        if (isUnique) {
                            refreshDriverStatus(curatorCache, raCtx);
                        } else {
                            onDuplicateReactorSystem(raCtx);
                        }
                    });
        }
    }

    private static void refreshDriverStatus(@Nullable CuratorCache curatorCache, ReActorContext driver) {
        Optional.ofNullable(curatorCache)
                .map(CuratorCache::stream)
                .ifPresent(children -> children.forEachOrdered(child -> refreshGate(driver.getSelf(),
                                                                                    driver.getReActorSystem(),
                                                                                    child)));
    }
    private static void refreshGate(ReActorRef zkDriver, ReActorSystem reActorSystem,
                                    ChildData childData) {
        cacheEventsRouter(new TreeCacheEvent(TreeCacheEvent.Type.NODE_ADDED, childData),
                          reActorSystem, zkDriver);
    }

    private static String getReactorSystemPublicationPath(ReActorSystemId reActorSystemId) {
        return String.format(NEW_REACTOR_SYSTEM_PATH_FORMAT, reActorSystemId.getReActorSystemName());

    }
    private static String getGatePublicationPath(ReActorSystemId reActorSystemId, ChannelId channelId) {
        return String.format(NEW_REACTOR_SYSTEM_GATE_PATH_FORMAT, reActorSystemId.getReActorSystemName(),
                             channelId);
    }
    private static boolean shouldProcessUpdate(String updatedPath) {

        if (updatedPath.length() > REACTOR_SYSTEMS_GATES_PATH.length() &&
            updatedPath.startsWith(REACTOR_SYSTEMS_GATES_PATH)) {
            ZKPaths.PathAndNode pathAndNode = ZooKeeperDriver.getGateUpsertPath(updatedPath);
            return ChannelId.fromToString(pathAndNode.getNode()).isPresent();
        }
        return false;
    }
    private static ZKPaths.PathAndNode getGateUpsertPath(String gateUpdateFullPath) {
        String[] reactorSystemToChannelId = gateUpdateFullPath.substring(REACTOR_SYSTEMS_GATES_PATH.length())
                                                              .split(REACTORSYSTEM_TO_CHANNEL_SEPARATOR);
        return new ZKPaths.PathAndNode(reactorSystemToChannelId[0], reactorSystemToChannelId[1]);
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
    private <ReturnT> CompletableFuture<ReturnT> runAsync(Supplier<ReturnT> task) {
        return CompletableFuture.supplyAsync(task, getConfig().getAsyncExecutionService());
    }
    private CompletableFuture<Void> runAsync(Runnable task) {
        return CompletableFuture.runAsync(task, getConfig().getAsyncExecutionService());
    }
    //Side effect on the input properties
    private static Properties patchServiceProperties(Properties serviceProperties,
                                                     @SuppressWarnings("SameParameterValue") String key,
                                                     Object value) {
        serviceProperties.merge(key, value, (oldObj, newObj) -> value);
        return serviceProperties;
    }
}
