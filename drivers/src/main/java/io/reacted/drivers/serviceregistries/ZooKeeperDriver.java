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
import io.reacted.core.drivers.serviceregistries.ServiceRegistryInitData;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.serviceregistry.FilterServiceDiscoveryRequest;
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
import org.apache.curator.x.async.WatchMode;
import org.apache.curator.x.async.api.ExistsOption;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nonnull;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    @Nullable
    private ScheduledExecutorService timerService;
    @Nullable
    private ServiceDiscovery<ServicePublicationRequest> serviceDiscovery;
    @Nullable
    private AsyncCuratorFramework asyncClient;
    @Nullable
    private TreeCache reActedHierarchy;

    public ZooKeeperDriver(ZooKeeperDriverCfg cfg) {
        super(cfg);
    }

    @Override
    public void onServiceRegistryInit(@Nonnull ServiceRegistryInitData initData) {
        this.timerService = initData.getTimerService();
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
        getServiceInstance(cancellationRequest.getServiceName(),
                           raCtx.getReActorSystem().getLocalReActorSystemId(), (ServicePublicationRequest)null)
                .ifSuccessOrElse(this.serviceDiscovery::unregisterService,
                                 error -> raCtx.logError("Unable to unregister service {}",
                                                         cancellationRequest.toString(), error));
    }

    private void onServicePublicationRequest(ReActorContext raCtx, ServicePublicationRequest serviceInfo) {
        if (this.serviceDiscovery == null) {
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
        getServiceInstance(serviceName, raCtx.getReActorSystem().getLocalReActorSystemId(), serviceInfo)
                .ifSuccess(this.serviceDiscovery::registerService)
                .ifError(registeringError -> raCtx.reply(new RegistryServicePublicationFailed(serviceName,
                                                                                              registeringError)));
    }

    private void onServiceDiscovery(ReActorContext raCtx, ServiceDiscoveryRequest request) {
        if (this.serviceDiscovery == null) {
            return;
        }

        ServiceDiscoverySearchFilter filter = request.getSearchFilter();

        var nameMatchingServices = Try.of(() -> this.serviceDiscovery.queryForInstances(filter.getServiceName()))
           .orElse(List.of(),
                   error -> raCtx.logError("Error discovering service {}",
                                           request.getSearchFilter().getServiceName(), error));
        var servicesFound = nameMatchingServices.stream()
                                                .map(serviceInstance -> new FilterItem(serviceInstance.getPayload().getServiceGate(),
                                                                                       patchServiceProperties(serviceInstance.getPayload().getServiceProperties(),
                                                                                                              ServiceDiscoverySearchFilter.FIELD_NAME_IP_ADDRESS,
                                                                                                              serviceInstance.getAddress())))
                                                .collect(Collectors.toUnmodifiableSet());
        raCtx.getReActorSystem().getSystemRemotingRoot()
             .aTell(raCtx.getSender(), new FilterServiceDiscoveryRequest(filter, servicesFound))
             .thenAcceptAsync(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                .ifError(error -> raCtx.logDebug("Service discovery reply not delivered to",
                                                                                                 error)));
    }

    private void onChannelIdCancellationRequest(ReActorContext raCtx,
                                                ReActorSystemChannelIdCancellationRequest cancelRequest) {
        if (this.asyncClient == null) {
            return;
        }
        this.asyncClient.delete().forPath(ZooKeeperDriver.getGatePublicationPath(cancelRequest.getReActorSystemId(),
                                                                                 cancelRequest.getChannelId()));
    }

    private void onChannelIdPublicationRequest(ReActorContext raCtx,
                                               ReActorSystemChannelIdPublicationRequest pubRequest) {
        if (this.client == null) {
            Try.of(() -> Objects.requireNonNull(this.timerService).schedule(() -> raCtx.selfTell(pubRequest),
                                                                            getConfig().getReattemptOnFailureInterval().toMillis(),
                                                                            TimeUnit.MILLISECONDS))
               .ifError(error -> { raCtx.logError("Critic! {} cannot reschedule a channel publication, it will now " +
                                                  "shut down", getConfig().getReActorName(), error);
                                  raCtx.stop(); });
            return;
        }

        try {

            this.client.create()
                       .creatingParentsIfNeeded()
                       .withMode(CreateMode.EPHEMERAL)
                       .forPath(ZooKeeperDriver.getGatePublicationPath(pubRequest.getReActorSystemId(),
                                                                       pubRequest.getChannelId()),
                                encodeProperties(pubRequest.getChannelIdData()));
        } catch (KeeperException.NodeExistsException alreadyPublished) {
            raCtx.logError("Trying to publish on something already there? ", alreadyPublished);
        } catch (Exception nodeCreationError) {
            raCtx.logError("Unable to publish ReActorSystem ", nodeCreationError);
        }
    }

    private void onSynchronizationWithRegistryRequest(ReActorContext raCtx,
                                                      SynchronizationWithServiceRegistryRequest subRequest) {
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
                                                                  new SynchronizationWithServiceRegistryComplete());
        } catch (Exception subscriptionError) {
            raCtx.logError("Error starting registry subscription", subscriptionError);
            Try.of(() -> Objects.requireNonNull(this.timerService).schedule(() -> raCtx.selfTell(subRequest),
                                                    getConfig().getReattemptOnFailureInterval().toMillis(),
                                                    TimeUnit.MILLISECONDS))
               .ifError(error -> { raCtx.logError("Critic! {} cannot reschedule a synchronization, it will now " +
                                                  "shut down", getConfig().getReActorName(), error);
                                   raCtx.stop(); });
        }
    }

    private void onStop(ReActorContext raCtx) {
        Try.ofRunnable(this::shutdownZookeeperConnection)
           .ifError(error -> raCtx.logError("Error stopping service registry", error));
    }

    private void onInit(ReActorContext raCtx) {
        this.asyncClient = AsyncCuratorFramework.wrap(CuratorFrameworkFactory.newClient(getZkConnectionString(),
                                                                                        new ExponentialBackoffRetry(1000,
                                                                                                                    20)));


        this.asyncClient.checkExists()
                        .withOptions(Set.of(ExistsOption.createParentsIfNeeded))
                        .forPath(CLUSTER_REGISTRY_REACTORSYSTEMS_ROOT_PATH)
                        .thenAcceptAsync(stat -> stat.)

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
        var serviceDiscovery = servicesPathCreation.map(success -> ServiceDiscoveryBuilder.builder(ServicePublicationRequest.class)
                                                                                          .basePath(CLUSTER_REGISTRY_SERVICES_ROOT_PATH)
                                                                                          .client(this.client)
                                                                                          .build());
        this.serviceDiscovery = serviceDiscovery.orElse(null,
                                                        error -> { raCtx.logError("Error initializing {} driver",
                                                                                  ZooKeeperDriver.class.getSimpleName(),
                                                                                  error); raCtx.stop(); });
        if (this.serviceDiscovery != null) {
            raCtx.getReActorSystem().getSystemRemotingRoot().tell(raCtx.getSelf(), new RegistryDriverInitComplete());
        }
    }

    private static void onSpuriousMessage(ReActorContext raCtx, Object message) {
        raCtx.logError("Unrecognized message received in {}", ZooKeeperDriver.class.getSimpleName(),
                       new IllegalStateException(message.toString()));
    }

    private static TreeCacheListener getTreeListener(ReActorSystem reActorSystem, ReActorRef driverReActor) {
        return (curatorFramework, treeCacheEvent) ->
                cacheEventsRouter(curatorFramework, treeCacheEvent, reActorSystem, driverReActor)
                        .thenAcceptAsync(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                           .ifError(error -> reActorSystem.logError("Error handling zookeeper event {}",
                                                                                                                    treeCacheEvent.toString(), error)));
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

    private void shutdownZookeeperConnection() throws IOException {
        if (this.serviceDiscovery != null) {
            this.serviceDiscovery.close();
        }
        if (this.reActedHierarchy != null) {
            this.reActedHierarchy.close();
        }
        if (this.client != null) {
            this.client.close();
        }
    }
}
