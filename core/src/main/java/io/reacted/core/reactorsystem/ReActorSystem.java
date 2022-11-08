/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.drivers.NullLocalDriverConfig;
import io.reacted.core.config.reactors.ReActiveEntityConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.ReActorServiceConfig;
import io.reacted.core.config.reactors.ServiceRegistryConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.LoopbackDriver;
import io.reacted.core.drivers.system.NullDriver;
import io.reacted.core.drivers.system.NullLocalDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.exceptions.ReActorRegistrationException;
import io.reacted.core.exceptions.ReActorSystemInitException;
import io.reacted.core.exceptions.ReActorSystemStructuralInconsistencyError;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.mailboxes.NullMailbox;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActedDebug;
import io.reacted.core.messages.reactors.ReActedError;
import io.reacted.core.messages.reactors.ReActedInfo;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.messages.services.ServiceDiscoveryRequest;
import io.reacted.core.messages.services.ServiceDiscoverySearchFilter;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactors.systemreactors.DeadLetter;
import io.reacted.core.reactors.systemreactors.RemotingRoot;
import io.reacted.core.reactors.systemreactors.SystemLogger;
import io.reacted.core.reactors.systemreactors.SystemMonitor;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.services.Service;
import io.reacted.core.typedsubscriptions.SubscriptionsManager;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.typedsubscriptions.TypedSubscriptionsManager;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@NonNullByDefault
public class ReActorSystem {
    public static final ReActorSystem NO_REACTOR_SYSTEM = new ReActorSystem();
    private static final int SYSTEM_TASK_SCHEDULER_POOL_SIZE = 2;
    // Service discovery always hits the LOCAL services of the LOCAL service registry driver. There is no reason
    // to wait indefinitely for an answer from local resources
    private static final Duration SERVICE_DISCOVERY_TIMEOUT = Duration.ofSeconds(10);
    private static final Logger LOGGER = LoggerFactory.getLogger(ReActorSystem.class);
    private static final Serializable REACTOR_INIT = new ReActorInit();
    private static final DispatcherConfig SYSTEM_DISPATCHER_CONFIG = DispatcherConfig.newBuilder()
                                                                                     .setDispatcherName(
                                                                                         Dispatcher.DEFAULT_DISPATCHER_NAME)
                                                                                     .setBatchSize(
                                                                                         Dispatcher.DEFAULT_DISPATCHER_BATCH_SIZE)
                                                                                     .setDispatcherThreadsNum(
                                                                                         Dispatcher.DEFAULT_DISPATCHER_THREAD_NUM)
                                                                                     .build();

    private final Set<ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>>> reActorSystemDrivers;
    /* All the reactors spawned by a specific reactor system instance */
    private final Map<ReActorId, ReActorContext> reactorsByReactorId;
    private final Map<Long, ReActorContext> reactorsBySchedulationId;
    /* All the reactors that listen for a specific message type are saved here */
    private final SubscriptionsManager typedSubscriptionsManager;
    private final RegistryGatesCentralizedManager gatesCentralizedManager;
    private final Map<String, Dispatcher> dispatchers;
    private final ReActorSystemConfig systemConfig;
    private final AtomicLong newSeqNum;
    private final Message reActorStop;
    private final ReActorSystemId localReActorSystemId;
    private final int maximumReActorsNumForThisSystem;

    /**
     * The fields below can be null only before a successful init completion
     */
    @Nullable
    private ScheduledExecutorService systemSchedulingService;
    @Nullable
    private ExecutorService msgFanOutPool;
    @Nullable
    private ReActorRef reActorSystemRoot;
    @Nullable
    private ReActorRef init;
    @Nullable
    private ReActorRef systemReActorsRoot;
    @Nullable
    private ReActorRef systemRemotingRoot;
    @Nullable
    private ReActorRef userReActorsRoot;
    @Nullable
    private ReActorRef systemDeadLetters;
    @Nullable
    private ReActorRef systemLoggingReActor;
    @Nullable
    private ReActorRef systemMonitor;

    private ReActorSystem() {
        this.maximumReActorsNumForThisSystem = 0;
        this.reActorSystemDrivers = Set.of();
        this.reactorsByReactorId = Map.of();
        this.reactorsBySchedulationId = Map.of();
        this.localReActorSystemId = ReActorSystemId.NO_REACTORSYSTEM_ID;
        this.typedSubscriptionsManager = new SubscriptionsManager() { };
        this.dispatchers = Map.of();
        this.systemConfig = ReActorSystemConfig.newBuilder()
                                               .setReactorSystemName(ReActorSystemId.NO_REACTORSYSTEM_ID_NAME)
                                               .setLocalDriver(new NullLocalDriver(NullLocalDriverConfig.newBuilder()
                                                                                                        .setChannelName("NO COMMUNICATION DRIVER")
                                                                                                        .build()))
                                               .build();
        systemConfig.getLocalDriver().initDriverCtx(this);
        this.gatesCentralizedManager = new RegistryGatesCentralizedManager(localReActorSystemId,
                                                                           new LoopbackDriver<>(this, getSystemConfig().getLocalDriver()));
        this.newSeqNum = new AtomicLong(Long.MAX_VALUE);
        this.reActorStop = new Message(ReActorRef.NO_REACTOR_REF, ReActorRef.NO_REACTOR_REF,
                                       Long.MIN_VALUE, localReActorSystemId, AckingPolicy.NONE,
                                       new ReActorStop());
    }

    public ReActorSystem(ReActorSystemConfig config) {
        this.maximumReActorsNumForThisSystem = config.getMaximumReActorsNum();
        this.systemConfig = Objects.requireNonNull(config);
        this.localReActorSystemId = new ReActorSystemId(config.getReActorSystemName());
        this.gatesCentralizedManager = new RegistryGatesCentralizedManager(localReActorSystemId,
                                                                           new LoopbackDriver<>(this, getSystemConfig().getLocalDriver()));
        this.reActorSystemDrivers = new CopyOnWriteArraySet<>();
        this.reactorsByReactorId = new ConcurrentHashMap<>(maximumReActorsNumForThisSystem, 0.1f);
        this.reactorsBySchedulationId = new ConcurrentHashMap<>(maximumReActorsNumForThisSystem, 0.1f);
        this.typedSubscriptionsManager = new TypedSubscriptionsManager();
        this.dispatchers = new ConcurrentHashMap<>(10, 0.5f);
        this.newSeqNum = new AtomicLong(0);
        this.reActorStop = new Message(ReActorRef.NO_REACTOR_REF, ReActorRef.NO_REACTOR_REF, Long.MIN_VALUE,
                                       localReActorSystemId, AckingPolicy.NONE, new ReActorStop());
    }
    /**
     * @return The configuration for the reactor system
     */
    public ReActorSystemConfig getSystemConfig() { return systemConfig; }

    /**
     * @return A reference to a reactor that swallows every message it receives
     */
    public ReActorRef getSystemSink() {
        return Objects.requireNonNull(init);
    }

    /**
     * @return A reference to the root of all the reactors created by a user
     */
    public ReActorRef getUserReActorsRoot() {
        return Objects.requireNonNull(userReActorsRoot);
    }

    /**
     * @return A reference to the root of all the system reactors. A system reactor is a reactor that
     * has some use within the reactor system itself
     */
    public ReActorRef getSystemReActorsRoot() { return Objects.requireNonNull(systemReActorsRoot); }

    /**
     * @return A {@link ReActorRef} to the System Reactor responsible for managing remoting
     */
    public ReActorRef getSystemRemotingRoot() {
        return Objects.requireNonNull(systemRemotingRoot);
    }

    /**
     * @return The Typed Sniff Subscription fan out pool
     */
    public ExecutorService getMsgFanOutPool() {
        return Objects.requireNonNull(msgFanOutPool);
    }

    /**
     * @return A reference to the centralized logging reactor
     */
    public ReActorRef getSystemLogger() {
        return Objects.requireNonNull(systemLoggingReActor);
    }

    /**
     * @return A {@link ReActorRef} to the {@link SystemMonitor}
     */
    public ReActorRef getSystemMonitor() { return Objects.requireNonNull(systemMonitor); }

    /**
     *
     * @param anyRef Any {@link ReActorRef}
     * @return True is the specified argument is a system Dead Letters
     */
    public boolean isSystemDeadLetters(ReActorRef anyRef) {
        return Objects.requireNonNull(anyRef).equals(systemDeadLetters);
    }

    /**
     * Log an error using the centralized logging system
     *
     * @param errorDescription Sl4j Format error description
     * @param args             Arguments for Sl4j
     */
    public void logError(String errorDescription, Serializable ...args) {
        if (getSystemLogger().tell(getSystemSink(), new ReActedError(errorDescription, args))
                             .isNotSent()) {
                LOGGER.error("Unable to log error: {}", errorDescription);
                LOGGER.error(errorDescription, (Object) args);
        }
    }

    /**
     * Log a debug line using the centralized logging system
     *
     * @param format Sl4j error description
     * @param args   Arguments for Sl4j
     */
    public void logDebug(String format, Serializable ...args) {
        if (getSystemLogger().tell(getSystemSink(),
                                   new ReActedDebug(Objects.requireNonNull(format),
                                                    Objects.requireNonNull(args))).isNotSent()) {
            LOGGER.error("Unable to log {}", format);
            LOGGER.debug(format, (Object) args);
        }
    }

    /**
     * Log a info line using the centralized logging system
     *
     * @param format Sl4j format error description
     * @param args   Arguments for Sl4j
     */
    public void logInfo(String format, Serializable ...args) {
        if (getSystemLogger().tell(getSystemSink(),
                                   new ReActedInfo(Objects.requireNonNull(format),
                                                   Objects.requireNonNull(args))).isNotSent()) {
                LOGGER.error("Unable to log {}", format);
                LOGGER.info(format, (Object) args);
            }
    }

    public DeliveryStatus toDeadLetters(Serializable payload) {
        return toDeadLetters(ReActorRef.NO_REACTOR_REF, payload);
    }

    public DeliveryStatus toDeadLetters(Message message) {
        return isSystemDeadLetters(message.getSender())
               ? DeliveryStatus.NOT_SENT
               : toDeadLetters(message.getSender(), message.getPayload());
    }
    public DeliveryStatus toDeadLetters(ReActorRef sender, Serializable payload) {
        DeliveryStatus deliveryStatus = Objects.requireNonNull(systemDeadLetters)
                                               .route(sender, new DeadMessage(payload));
        if (deliveryStatus.isNotSent()) {
            LOGGER.error("Unable to send from {} to DeadLetters payload {}",
                         sender, payload);
        }
        return deliveryStatus;
    }

    /**
     * Generates a numeric ID
     * @return an incremental number guaranteed to be unique for the reactor system
     */
    public long getNewSeqNum() { return newSeqNum.getAndIncrement(); }

    /**
     * Returns the identifier of the local {@link ReActorSystem}
     * @return {@link ReActorSystemId} of the local {@link ReActorSystem}
     */
    public ReActorSystemId getLocalReActorSystemId() { return localReActorSystemId; }

    /**
     * Returns the {@link SubscriptionsManager} responsible for the typed subscriptions
     * @return The typed subscriptions manager
     */
    public SubscriptionsManager getTypedSubscriptionsManager() { return typedSubscriptionsManager; }

    /**
     * Register a route towards a specific {@link ReActorSystem}
     * @param reActorSystemId Identifier of the {@link ReActorSystem}
     * @param channelId Identifier of the new channel
     * @param channelProperties Specific properties required for using the specified channel
     * @param registryDriver Service registry responsible for monitoring the availability of this
     *                       ReActorSystem on the specified channel
     */
    public void registerNewRoute(ReActorSystemId reActorSystemId, ChannelId channelId,
                                 Properties channelProperties, ReActorRef registryDriver) {
        ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> driverForChannelId;
        driverForChannelId = getReActorSystemDrivers().stream()
                                                      .filter(driver -> driver.getChannelId().equals(channelId))
                                                      .findAny()
                                                      .orElse(null);
        gatesCentralizedManager.registerNewRoute(reActorSystemId,
                                                 driverForChannelId == null
                                                 ? NullDriver.NULL_DRIVER
                                                 : driverForChannelId, channelId, channelProperties, registryDriver);
    }

    /**
     * Forget how to reach a given reactor system through a specific channel.
     * @param reActorSystemId Destination {@link ReActorSystem}
     * @param channelId {@link ChannelId} to forget about
     */
    public void unregisterRoute(ReActorSystemId reActorSystemId, ChannelId channelId) {
        gatesCentralizedManager.unregisterRoute(reActorSystemId, channelId);
    }

    /**
     * Register a new driver within the reactor system.
     *
     * @param anyDriver A ReActed driver
     * @return A successful Try on success, a failed one containing the exception that caused the error otherwise
     */
    public Try<Void> registerReActorSystemDriver(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver) {
        return getReActorSystemDrivers().contains(anyDriver)
               ? Try.ofFailure(new IllegalArgumentException())
               : anyDriver.initDriverCtx(this)
                          .peekSuccess(noVal -> reActorSystemDrivers.add(anyDriver));
    }

    /**
     * Unregister from the system a driver
     *
     * @param anyDriver The driver instance we want to unregister
     * @return A future that will contain once completed the outcome of the operation
     */
    public CompletionStage<Try<Void>>
    unregisterReActorSystemDriver(ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> anyDriver) {
        reActorSystemDrivers.remove(anyDriver);
        gatesCentralizedManager.unregisterRoute(anyDriver);
        var stopProcess = anyDriver.stopDriverCtx(this);
        stopProcess.thenAccept(stopAttempt -> stopAttempt.ifError(error -> LOGGER.error("Error stopping driver {}",
                                                                                        anyDriver.getChannelId(),
                                                                                        error)));
        return stopProcess;
    }

    /**
     * Tries to find a route for the specified pair reactor system / channel id.
     * It if it is not possible, an alternate route is attempted.
     *
     * @param reActorSystemId Target reactor system
     * @param preferredChannelId Preferred {@link ChannelId} to use
     * @return A route/reference towards the requested reactor system
     */
    @Nullable
    public ReActorSystemRef findGate(ReActorSystemId reActorSystemId, ChannelId preferredChannelId) {
        return gatesCentralizedManager.findGate(reActorSystemId, preferredChannelId);
    }

    /**
     * Activate the reactorsystem. Before this call, the reactorsystem is not operational and does not require an
     * explicit shutdown
     *
     * @return An inited {@link ReActorSystem}
     */
    public ReActorSystem initReActorSystem() {
        Try.ofRunnable(this::initSystem)
           .peekFailure(error -> shutDown())
           .orElseSneakyThrow();
        return this;
    }

    /**
     * Shutdown the reactorsystem. All the reactors will be halted through a stop() according to the stop() hierarchical
     * semantic
     */
    public void shutDown() {

        Try.of(() -> stopUserReActors().toCompletableFuture().join())
           .ifError(error -> LOGGER.error("Error stopping user reactors", error));

        Try.of(() -> stopRemotingDrivers().toCompletableFuture().join())
           .ifSuccessOrElse(stopAttempt -> stopAttempt.ifError(error -> LOGGER.error("Error stopping remote drivers",
                                                                                     error)),
                            joinError -> LOGGER.error("Error waiting for remoting drivers to stop", joinError));

        Try.ofRunnable(() -> stop(getSystemRemotingRoot().getReActorId())
                                .map(CompletionStage::toCompletableFuture)
                                .ifPresent(CompletableFuture::join))
           .ifError(error -> LOGGER.error("Error stopping service registry drivers"));

        Try.of(() -> stopSystemReActors().toCompletableFuture().join())
           .ifError(error -> LOGGER.error("Error stopping system reactors", error));

        Try.of(() -> stopReActorSystem().toCompletableFuture().join())
           .ifError(error -> LOGGER.error("Error stopping init hierarchy", error));

        this.init = null;

        Try.of(() -> stopLocalDriver().toCompletableFuture().join())
           .ifSuccessOrElse(stopAttempt -> stopAttempt.ifError(error -> LOGGER.error("Error stopping local drivers",
                                                                                     error)),
                            error -> LOGGER.error("Error waiting for local drivers to stop", error));

        reActorSystemDrivers.clear();
        stopSystemTimer();
        stopFanOutPool();
        stopDispatchers();
    }

    /**
     * Request a reactor reference for the specified service.
     *
     * @param searchFilter A {@link BasicServiceDiscoverySearchFilter} describing the feature of the services that should be
     *                     found
     * @return On success a future containing the result of the request
     * On failure a future containing the exception that caused the failure
     */
    public CompletionStage<ServiceDiscoveryReply>
    serviceDiscovery(ServiceDiscoverySearchFilter searchFilter) {
        return serviceDiscovery(searchFilter, "");
    }

    /**
     * Request a reactor reference for the specified service.
     *
     * @param searchFilter A {@link BasicServiceDiscoverySearchFilter} describing the feature of the services that should be
     *                     found
     * @param requestId An extra id that will be used together with {@link ServiceDiscoverySearchFilter#getDiscoveryRequestId}
     *                  to create a uniquely identified request
     * @return On success a future containing the result of the request
     * On failure a future containing the exception that caused the failure
     */
    public CompletionStage<ServiceDiscoveryReply>
    serviceDiscovery(ServiceDiscoverySearchFilter searchFilter, String requestId) {
        return getSystemSink().ask(new ServiceDiscoveryRequest(Objects.requireNonNull(searchFilter)),
                                   ServiceDiscoveryReply.class, SERVICE_DISCOVERY_TIMEOUT,
                                   searchFilter.getDiscoveryRequestId() + "-" + requestId);
    }

    /**
     * Request a reactor reference for the specified service
     *
     * @param searchFilter A {@link BasicServiceDiscoverySearchFilter} describing the feature of the services that should
     *                     be found
     * @param requester     source of this request
     * @return The outcome of the request
     */
    @SuppressWarnings("UnusedReturnValue")
    public DeliveryStatus serviceDiscovery(ServiceDiscoverySearchFilter searchFilter,
                                           ReActorRef requester) {
        return broadcastToLocalSubscribers(Objects.requireNonNull(requester),
                                           new ServiceDiscoveryRequest(Objects.requireNonNull(searchFilter)));
    }

    /**
     * Create a new reactor
     *
     * @param reActor a generic reactor
     * @return A successful Try containing the ReActorRef for the new reactor on success, a failed Try on failure
     */
    public Try<ReActorRef> spawn(ReActor reActor) {
        return spawn(Objects.requireNonNull(Objects.requireNonNull(reActor).getReActions()),
                     Objects.requireNonNull(reActor.getConfig()));
    }

    /**
     * Create a new reactor
     *
     * @param reActions     reactor behavior
     * @param reActorConfig reactor configuration
     * @return A successful Try containing the ReActorRef for the new reactor on success,
     * a failed Try on failure
     */
    public Try<ReActorRef> spawn(ReActions reActions,
                                 ReActiveEntityConfig<? extends ReActiveEntityConfig.Builder<?, ?>,
                                                      ? extends ReActiveEntityConfig<?, ?>> reActorConfig) {
        return spawnChild(Objects.requireNonNull(reActions, "ReActions cannot be null"),
                          Objects.requireNonNull(userReActorsRoot, "System not inited correctly"),
                          Objects.requireNonNull(reActorConfig, "ReActorConfig cannot be null"));
    }

    /**
     * Create a new reactor
     *
     * @param reActiveEntity a reactive entity
     * @param reActorConfig reactor configuration
     * @return A successful Try containing the ReActorRef for the new reactor on success,
     * a failed Try on failure
     */
    public Try<ReActorRef> spawn(ReActiveEntity reActiveEntity,
                                 ReActiveEntityConfig<? extends ReActiveEntityConfig.Builder<?, ?>,
                                                      ? extends ReActiveEntityConfig<?, ?>> reActorConfig) {
        return spawnChild(Objects.requireNonNull(Objects.requireNonNull(reActiveEntity, "ReActor cannot be null")
                                                        .getReActions(), "ReActions cannot be null"),
                          Objects.requireNonNull(userReActorsRoot),
                          Objects.requireNonNull(reActorConfig, "ReActor config cannot be null"));
    }

    /**
     * Create a new reactor child of the specified reactor
     *
     * @param reActions     reactor behavior
     * @param reActorConfig reactor configuration
     * @param father        father of the new reactor
     * @return A successful Try containing the ReActorRef for the new reactor on success,
     * a failed Try on failure
     */
    public Try<ReActorRef> spawnChild(ReActions reActions, ReActorRef father,
                                      ReActiveEntityConfig<? extends ReActiveEntityConfig.Builder<?, ?>,
                                                           ? extends ReActiveEntityConfig<?, ?>> reActorConfig) {
        Try<ReActorRef> spawned = spawn(getLoopback(), Objects.requireNonNull(reActions, "ReActions cannot be null"),
                                        Objects.requireNonNull(father, "Father ReActor cannot be null"),
                                        Objects.requireNonNull(reActorConfig, "ReActor config cannot be null"));
        spawned.ifSuccess(initMe -> initMe.tell(getSystemSink(), REACTOR_INIT));
        return spawned;
    }

    /**
     * Create a new service. Services are reactors automatically backed up by a router
     *
     * @param serviceConfig service config
     * @return A successful Try containing the ReActorRef for the new service on success, a failed try on failure
     */
    public <ServiceConfigBuilderT extends ReActorServiceConfig.Builder<ServiceConfigBuilderT, ServiceConfigT>,
            ServiceConfigT extends ReActorServiceConfig<ServiceConfigBuilderT, ServiceConfigT>>
    Try<ReActorRef> spawnService(ServiceConfigT serviceConfig) {
        return spawn(new Service<>(Objects.requireNonNull(serviceConfig)).getReActions(), serviceConfig);
    }


    /**
     * Create a new service. Services are reactors automatically backed up by a router
     *
     * @param serviceConfig service config
     * @param father This service will be created as a child of the specified ReActor
     * @return A successful Try containing the ReActorRef for the new service on success, a failed try on failure
     */
    public <ServiceCfgBuilderT extends ReActorServiceConfig.Builder<ServiceCfgBuilderT, ServiceCfgT>,
            ServiceCfgT extends ReActorServiceConfig<ServiceCfgBuilderT, ServiceCfgT>>
    Try<ReActorRef> spawnService(ServiceCfgT serviceConfig, ReActorRef father) {
        return spawnChild(new Service<>(Objects.requireNonNull(serviceConfig)).getReActions(),
                                        father, serviceConfig);
    }

    public ReActorSystemRef getLoopback() { return gatesCentralizedManager.getLoopBack(); }

    /**
     * Sends a message to all the local subscribers for the message type
     * @param msgSender A {@link ReActorRef} defining the sender of this message
     * @param payload The payload that should be broadcasted
     * @param <PayLoadT> Any {@link Serializable} object
     * @return A {@link CompletionStage} that is going to be completed when the message is sent
     */
    public <PayLoadT extends Serializable> DeliveryStatus
    broadcastToLocalSubscribers(ReActorRef msgSender, PayLoadT payload) {
        return getSystemSink().tell(Objects.requireNonNull(msgSender), Objects.requireNonNull(payload));
    }

    /**
     * Sends a message to all the remote subscribers for the message type
     * @param payload The payload that should be broadcasted
     * @param <PayLoadT> Any {@link Serializable} object
     */
    public <PayLoadT extends Serializable> void broadcastToRemoteSubscribers(PayLoadT payload) {
        broadcastToRemoteSubscribers(ReActorRef.NO_REACTOR_REF, payload);
    }

    /**
     * Sends a message to all the remote subscribers for the message type
     * @param msgSender A {@link ReActorRef} defining the sender of this message
     * @param payload The payload that should be broadcasted
     * @param <PayLoadT> Any {@link Serializable} object
     */
    public <PayLoadT extends Serializable> void broadcastToRemoteSubscribers(ReActorRef msgSender, PayLoadT payload) {
        gatesCentralizedManager.findAllGates().stream()
                               .filter(Predicate.not(getLoopback()::equals))
                               .map(remoteGate -> new ReActorRef(ReActorId.NO_REACTOR_ID, remoteGate))
                               .forEach(remoteGate -> remoteGate.tell(Objects.requireNonNull(msgSender),
                                                                      Objects.requireNonNull(payload)));
    }

    /**
     * Sends a message to all the remote subscribers for the message type
     * @param channelId {@link ChannelId} towards the message should be broadcasted to
     * @param payload The payload that should be broadcasted
     * @param <PayLoadT> Any {@link Serializable} object
     */
    public <PayLoadT extends Serializable> void broadcastToAllSubscribers(ChannelId channelId, PayLoadT payload) {
        broadcastToAllSubscribers(channelId, getSystemSink(), payload);
    }

    /**
     * Sends a message to all the remote subscribers for the message type
     * @param channelId {@link ChannelId} towards the message should be broadcasted to
     * @param msgSender A {@link ReActorRef} defining the sender of this message
     * @param payload The payload that should be broadcasted
     * @param <PayLoadT> Any {@link Serializable} object
     */
    public <PayLoadT extends Serializable> void broadcastToAllSubscribers(ChannelId channelId, ReActorRef msgSender,
                                                                          PayLoadT payload) {
        gatesCentralizedManager.findAllGates(channelId).stream()
                               .map(remoteGate -> new ReActorRef(ReActorId.NO_REACTOR_ID, remoteGate))
                               .forEach(remoteGate -> remoteGate.tell(Objects.requireNonNull(msgSender),
                                                                      Objects.requireNonNull(payload)));
    }

    @Nullable
    public ReActorContext getReActorCtx(ReActorId reActorId) {
        return reactorsByReactorId.get(Objects.requireNonNull(reActorId));
    }

    @Nullable
    public ReActorContext getReActorCtx(long schedulationId) {
        return reactorsBySchedulationId.get(schedulationId);
    }

    //Create a ReActorRef with the appropriate driver attached for the specified reactor system / channel id
    //This allow location transparent communication with the ReActorRef identified by the input argument
    public static Set<ReActorRef> getRoutedReference(ReActorRef referenceWithNoRoute, ReActorSystem reActorSystem) {
        return reActorSystem.findGates(referenceWithNoRoute.getReActorSystemRef().getReActorSystemId()).stream()
                            .map(reActorSystemRef -> new ReActorRef(referenceWithNoRoute.getReActorId(),
                                                                    reActorSystemRef))
                            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Request a reactor to stop.
     * @param reActorToStop {@link ReActorId} of the local reactor to stop
     * @return If the reactor exists, a completion stage that is going to be complete once the hierarchy has been
     * terminated. An empty optional otherwise
     */
    public Optional<CompletionStage<Void>> stop(ReActorId reActorToStop) {
        return Optional.ofNullable(getReActorCtx(reActorToStop)).map(ReActorContext::stop);
    }

    public ScheduledExecutorService getSystemSchedulingService() {
        return Objects.requireNonNull(systemSchedulingService);
    }

    public void flushRemoteGatesForDriver(ReActorRef registryDriver) {
        gatesCentralizedManager.unregisterSource(registryDriver);
    }

    /**
     * Checks if the {@link ReActor} pointed by a {@link ReActorRef} is local to this
     * {@link ReActorSystem}
     * @param reActorRef Any {@link ReActorRef}
     * @return true is the pointed {@link ReActor} is local, false otherwise
     */
    public boolean isLocal(ReActorRef reActorRef) {
        return reActorRef.getReActorSystemRef() == getLoopback();
    }

    Set<ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>>> getReActorSystemDrivers() {
        return Set.copyOf(reActorSystemDrivers);
    }

    //Runtime update of the typed sniff subscriptions for a given reactor
    //Guarded by structural lock on target actor
    void updateMessageInterceptors(ReActorContext targetActor, TypedSubscription[] oldIntercepted,
                                   TypedSubscription[] newIntercepted) {

        Arrays.stream(oldIntercepted)
              .forEach(typedSubscription -> typedSubscriptionsManager.removeSubscription(typedSubscription.getPayloadType(),
                                                                                         typedSubscription.getSubscriptionPolicy(),
                                                                                         targetActor));

        Arrays.stream(newIntercepted)
              .forEach(typedSubscription -> typedSubscriptionsManager.addSubscription(typedSubscription.getPayloadType(),
                                                                                      typedSubscription.getSubscriptionPolicy(),
                                                                                      targetActor));
    }

    private Collection<ReActorSystemRef> findGates(ReActorSystemId reActorSystemId) {
        return gatesCentralizedManager.findGates(reActorSystemId);
    }

    /**
     * Exception is sneaky thrown
     */
    private void initSystem() {

        if (createAllDispatchers(getSystemConfig().getDispatchersConfigs(), this)
                .anyMatch(Predicate.not(this::registerDispatcher))) {
            throw new ReActorSystemInitException("Unable to register system dispatcher");
        }

        this.systemSchedulingService = createSystemScheduleService(getSystemConfig().getReActorSystemName(),
                                                                   SYSTEM_TASK_SCHEDULER_POOL_SIZE);

        this.msgFanOutPool = createFanOutPool(getLocalReActorSystemId().getReActorSystemName(),
                                              getSystemConfig().getMsgFanOutPoolSize());

        registerReActorSystemDriver(gatesCentralizedManager.getLoopbackDriver()).orElseSneakyThrow();

        registerReActorSystemDriver(NullDriver.NULL_DRIVER).orElseSneakyThrow();

        gatesCentralizedManager.registerNewRoute(ReActorSystemId.NO_REACTORSYSTEM_ID, NullDriver.NULL_DRIVER,
                                                 NullDriver.NULL_DRIVER.getChannelId(), new Properties(),
                                                 ReActorRef.NO_REACTOR_REF);

        spawnReActorSystemReActors();
        initAllDispatchers(dispatchers.values(), getSystemSink(), systemConfig.isRecordedExecution(),
                           this::unRegisterReActor);

        initReActorSystemReActors();

        getSystemConfig().getRemotingDrivers().forEach(remotingDriver -> this.registerReActorSystemDriver(remotingDriver)
                                                                             .orElseSneakyThrow());

        initServiceRegistryDrivers(getSystemConfig().getServiceRegistryDrivers());
    }

    /* SneakyThrows */

    /**
     * @throws Exception a SneakyThrown exception
     */
    @SuppressWarnings("JavaDoc")
    private void initServiceRegistryDrivers(Collection<ServiceRegistryDriver<? extends ServiceRegistryConfig.Builder<?, ?>,
                                                                             ? extends ServiceRegistryConfig<?, ?>>> drivers) {
        drivers.forEach(driver -> spawnChild(driver.getReActions(), getSystemRemotingRoot(), driver.getConfig())
                .orElseSneakyThrow());
    }

    /**
     * @throws ReActorSystemInitException If unable to deliver reactor init message
     */
    private void initReActorSystemReActors() {
        if (reactorsByReactorId.values().stream()
                               .map(ReActorContext::getSelf)
                               .map(reactor -> reactor.tell(getSystemSink(), REACTOR_INIT))
                               .anyMatch(Predicate.not(DeliveryStatus::isSent))) {
            throw new ReActorSystemInitException("Unable to init all system reactors");
        }
    }

    /**
     * @throws ReActorSystemStructuralInconsistencyError if no {@link Dispatcher} is found
     * @throws ReActorRegistrationException if a {@link ReActor} with a duplicated name is found
     */
    private void spawnReActorSystemReActors() {
        this.init = spawnInit();
        this.reActorSystemRoot = spawnReActorsRoot(init);
        this.systemRemotingRoot = spawnRemotingRoot(reActorSystemRoot);
        this.systemReActorsRoot = spawnSystemActorsRoot(reActorSystemRoot);
        this.systemDeadLetters = spawnSystemDeadLetters(systemReActorsRoot);
        this.systemMonitor = spawnSystemMonitor(systemReActorsRoot);
        this.systemLoggingReActor = spawnSystemLogging(systemReActorsRoot);
        this.userReActorsRoot = spawnUserActorsRoot(reActorSystemRoot);
    }

    private CompletionStage<Try<Void>> stopLocalDriver() {
        return getNonRemoteDrivers().stream()
                                    .map(this::unregisterReActorSystemDriver)
                                    .reduce((f, s) -> f.thenCompose(fResult -> s))
                                    .orElse(CompletableFuture.completedFuture(Try.ofSuccess(null)));
    }

    private CompletionStage<Try<Void>> stopRemotingDrivers() {
        var localDrivers = getNonRemoteDrivers();
        return reActorSystemDrivers.stream()
                                   .filter(driver -> localDrivers.stream()
                                                                 .noneMatch(localDriver -> localDriver.equals(driver)))
                                   .map(this::unregisterReActorSystemDriver)
                                   .reduce((f, s) -> f.thenCompose(fResult -> s))
                                   .orElse(CompletableFuture.completedFuture(Try.ofSuccess(null)));
    }

    private List<ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>>> getNonRemoteDrivers() {
        return Stream.concat(Stream.of(NullDriver.NULL_DRIVER),
                             gatesCentralizedManager.findGates(localReActorSystemId).stream()
                                                    .map(ReActorSystemRef::getBackingDriver))
                     .collect(Collectors.toUnmodifiableList());
    }

    private CompletionStage<Void> stopUserReActors() {
        if (userReActorsRoot == null) {
            return CompletableFuture.completedFuture(null);
        }
        var userRoot = userReActorsRoot;
        this.userReActorsRoot = null;
        return stopSystemRoot(userRoot);
    }

    private CompletionStage<Void> stopSystemReActors() {
        if (reActorSystemRoot == null) {
            return CompletableFuture.completedFuture(null);
        }
        var systemRoot = reActorSystemRoot;
        this.reActorSystemRoot = null;
        return stopSystemRoot(systemRoot);
    }

    private CompletionStage<Void> stopReActorSystem() {
        return init == null
               ? CompletableFuture.completedFuture(null)
               : stopSystemRoot(init);
    }

    private CompletionStage<Void> stopSystemRoot(ReActorRef reActorRoot) {
        //Kill all the user actors hierarchy
        ReActorContext rootCtx = getReActorCtx(reActorRoot.getReActorId());
        return rootCtx != null
               ? rootCtx.stop()
               : CompletableFuture.completedFuture(null);
    }

    private void stopDispatchers() {
        dispatchers.values().forEach(Dispatcher::stopDispatcher);
        dispatchers.clear();
    }

    private void stopFanOutPool() {
        if (msgFanOutPool != null) {
            msgFanOutPool.shutdownNow();
            this.msgFanOutPool = null;
        }
    }

    private void stopSystemTimer() {
        if (systemSchedulingService != null) {
            systemSchedulingService.shutdownNow();
            this.systemSchedulingService = null;
        }
    }

    private boolean registerDispatcher(Dispatcher newDispatcher) {
        return dispatchers.putIfAbsent(newDispatcher.getName(), newDispatcher) == null;
    }

    private ReActorRef spawnInit() {
        /* we manually build the first reactor */
        return createReActorCtx(getLoopback(), ReActions.NO_REACTIONS,
                                new ReActorRef(ReActorId.NO_REACTOR_ID, getLoopback()),
                                ReActorId.NO_REACTOR_ID,
                                ReActorConfig.newBuilder()
                                             .setMailBoxProvider(ctx -> new NullMailbox())
                                             .setReActorName(ReActorId.NO_REACTOR_ID.getReActorName())
                                             .build())
                .filter(reActor -> registerNewReActor(reActor, reActor),
                        () -> new ReActorRegistrationException(ReActorId.NO_REACTOR_ID.getReActorName()))
                .map(ReActorContext::getSelf)
                .orElseSneakyThrow();
    }

    private ReActorRef spawnSystemActorsRoot(ReActorRef rootActor) {
        return spawn(getLoopback(), ReActions.NO_REACTIONS, rootActor,
                     ReActorConfig.newBuilder()
                                  .setMailBoxProvider(ctx -> new NullMailbox())
                                  .setReActorName("SystemActorsRoot")
                                  .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnRemotingRoot(ReActorRef rootActor) {
        return spawn(getLoopback(),
                     new RemotingRoot(localReActorSystemId,
                                      getSystemConfig().getRemotingDrivers()).getReActions(),
                     rootActor, ReActorConfig.newBuilder()
                                             .setReActorName("SystemRemotingRoot")
                                             .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnSystemDeadLetters(ReActorRef systemActorsRoot) {
        return spawn(getLoopback(), DeadLetter.DEADLETTERS, systemActorsRoot,
                     ReActorConfig.newBuilder()
                                  .setReActorName("DeadLetters")
                                  .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnSystemMonitor(ReActorRef systemActorsRoot) {
        return spawn(getLoopback(), new SystemMonitor(getSystemConfig().getSystemMonitorRefreshInterval(),
                                                      getSystemSchedulingService()).getReActions(),
                     systemActorsRoot, ReActorConfig.newBuilder()
                                                    .setReActorName("SystemMonitor")
                                                    .setMailBoxProvider(ctx -> new BoundedBasicMbox(1))
                                                    .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnSystemLogging(ReActorRef systemActorsRoot) {
        return spawn(getLoopback(), SystemLogger.SYSTEM_LOGGER, systemActorsRoot,
                     ReActorConfig.newBuilder()
                                  .setReActorName("SystemLogging")
                                  .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnReActorsRoot(ReActorRef systemActorsRoot) {
        return spawn(getLoopback(), ReActions.NO_REACTIONS, systemActorsRoot,
                     ReActorConfig.newBuilder()
                                  .setMailBoxProvider(ctx -> new NullMailbox())
                                  .setReActorName("ReActorSystemRoot")
                                  .build()).orElseSneakyThrow();
    }

    private ReActorRef spawnUserActorsRoot(ReActorRef actorSystemRootActor) {
        return spawn(getLoopback(), ReActions.NO_REACTIONS, actorSystemRootActor,
                     ReActorConfig.newBuilder()
                                  .setMailBoxProvider(ctx -> new NullMailbox())
                                  .setReActorName("UserActorsRoot")
                                  .build()).orElseSneakyThrow();
    }

    private Try<ReActorRef> spawn(ReActorSystemRef spawnerAs, ReActions reActions,
                                  ReActorRef parent, ReActiveEntityConfig<?, ?> reActorConfig) {

        var reActorCtx = createReActorCtx(spawnerAs, reActions, parent,
                                          new ReActorId(parent.getReActorId(), reActorConfig.getReActorName()),
                                          reActorConfig);
        return reActorCtx.flatMap(newReActor -> registerNewReActor(parent, newReActor))
                         .map(ReActorContext::getSelf);
    }

    private Try<ReActorContext> createReActorCtx(ReActorSystemRef spawnerAs, ReActions reActions, ReActorRef parent,
                                                 ReActorId newReActorId, ReActiveEntityConfig<?, ?> reActorConfig) {

        return Try.of(() -> getDispatcher(reActorConfig.getDispatcherName())
                            .orElseThrow(() -> new ReActorSystemStructuralInconsistencyError("Dispatcher " +
                                                                                             reActorConfig.getDispatcherName() +
                                                                                             " not found")))
                  .flatMap(dispatcher -> Try.of(() -> ReActorContext.newBuilder()
                                                                    .setReactorRef(new ReActorRef(newReActorId, spawnerAs))
                                                                    .setMbox(reActorConfig.getMailBoxProvider())
                                                                    .setParentActor(parent)
                                                                    .setReActorSystem(this)
                                                                    .setDispatcher(dispatcher)
                                                                    .setSubscriptions(reActorConfig.getTypedSubscriptions())
                                                                    .setReActions(reActions)
                                                                    .build()));
    }

    private Try<ReActorContext> registerNewReActor(ReActorRef parent, ReActorContext newReActor) {
        ReActorContext parentCtx = getReActorCtx(parent.getReActorId());
        if (parentCtx == null || reactorsByReactorId.size() >= getSystemConfig().getMaximumReActorsNum()) {
            return Try.ofFailure(new ReActorRegistrationException(newReActor.getSelf()
                                                                            .getReActorId()
                                                                            .getReActorName()));
        }
        return Try.of(() -> registerNewReActor(parentCtx, newReActor))
                  .filter(Try::identity, () -> new ReActorRegistrationException(newReActor.getSelf()
                                                                                          .getReActorId()
                                                                                          .getReActorName()))
                  .map(registered -> newReActor);
    }

    private Optional<CompletionStage<Void>> unRegisterReActor(ReActorContext stopMe) {
        Optional<CompletionStage<Void>> stopHook = Optional.empty();
        ReActorContext parentCtx = getReActorCtx(stopMe.getParent().getReActorId());
        if (parentCtx != null) {
            parentCtx.unregisterChild(stopMe.getSelf());
        }
        //Avoid spawning a child while it's being stopped
        stopMe.getStructuralLock().writeLock().lock();
        try {
            //If it is already stopped don't process further, otherwise the remove could remove the name
            //of a new reactor with the same name that has just been spawned
            if (!stopMe.getHierarchyTermination().toCompletableFuture().isDone() &&
                reactorsByReactorId.remove(stopMe.getSelf().getReActorId()) != null) {
                reactorsBySchedulationId.remove(stopMe.getReActorSchedulationId());
                updateMessageInterceptors(stopMe, stopMe.getTypedSubscriptions(),
                                          TypedSubscription.NO_SUBSCRIPTIONS);
                Try.ofRunnable(() -> stopMe.reAct(reActorStop))
                   .ifError(error -> stopMe.logError("Unable to properly stop reactor: ", error));
                var allChildrenTerminated = allChildrenTerminationFuture(stopMe.getChildren(), this);
                CompletableFuture<Void> myTerminationHook = stopMe.getHierarchyTermination()
                                                                  .toCompletableFuture();
                allChildrenTerminated.thenAcceptAsync(lastChild -> myTerminationHook.complete(null));
                stopHook = Optional.of(myTerminationHook);
            }
        } finally {
            stopMe.getStructuralLock().writeLock().unlock();
        }
        return stopHook;
    }

    private static CompletionStage<Void> allChildrenTerminationFuture(Collection<ReActorRef> children,
                                                                      ReActorSystem reActorSystem) {

        return children.stream()
                       .map(ReActorRef::getReActorId)
                       .map(reActorSystem::getReActorCtx)
                       .filter(Objects::nonNull)
                       //exploit the dispatcher for stopping the actor
                       .map(ReActorContext::stop)
                       .reduce((firstChild, secondChild) -> firstChild.thenComposeAsync(res -> secondChild))
                       //no children no party
                       .orElse(CompletableFuture.completedFuture(null));
    }

    private boolean registerNewReActor(ReActorContext parentReActorCtx, ReActorContext newActor) {
        boolean hasBeenRegistered = false;
        boolean isSelfAdd = parentReActorCtx == newActor;

        parentReActorCtx.getStructuralLock().writeLock().lock();

        try {
            if ((isSelfAdd || reactorsByReactorId.containsKey(parentReActorCtx.getSelf().getReActorId())) &&
                reactorsByReactorId.putIfAbsent(newActor.getSelf().getReActorId(), newActor) == null) {
                reactorsBySchedulationId.put(newActor.getReActorSchedulationId(), newActor);
                //Do not add an actor to its own children
                if (!isSelfAdd) {
                    parentReActorCtx.registerChild(newActor.getSelf());
                }
                updateMessageInterceptors(newActor, newActor.getTypedSubscriptions(), newActor.getTypedSubscriptions());
                hasBeenRegistered = true;
            }
        } finally {
            parentReActorCtx.getStructuralLock().writeLock().unlock();
        }
        return hasBeenRegistered;
    }

    private Optional<Dispatcher> getDispatcher(String dispatcherName) {
        return Optional.ofNullable(dispatchers.get(dispatcherName));
    }
    private static void initAllDispatchers(Collection<Dispatcher> dispatchers, ReActorRef systemSink,
                                           boolean recordedExecution,
                                           Function<ReActorContext, Optional<CompletionStage<Void>>> reActorUnregister) {
        dispatchers.forEach(dispatcher -> dispatcher.initDispatcher(systemSink, recordedExecution,
                                                                    reActorUnregister));
    }

    private static Stream<Dispatcher> createAllDispatchers(Collection<DispatcherConfig> configuredDispatchers,
                                                           ReActorSystem reActorSystem) {
        return Stream.concat(Stream.of(SYSTEM_DISPATCHER_CONFIG), configuredDispatchers.stream())
                     .map(dispatcherCfg -> new Dispatcher(dispatcherCfg, reActorSystem));
    }

    @SuppressWarnings("SameParameterValue")
    private static ScheduledExecutorService createSystemScheduleService(String reactorSystemName,
                                                                        int schedulePoolSize) {
        var taskSchedulerProps = new ThreadFactoryBuilder()
                .setNameFormat(reactorSystemName + "-schedule_service-%d")
                .setUncaughtExceptionHandler((thread, error) -> LOGGER.error("Unexpected in scheduled task", error))
                .build();
        return Executors.newScheduledThreadPool(schedulePoolSize, taskSchedulerProps);
    }

    private static ExecutorService createFanOutPool(String reActorSystemName, int poolSize) {
        ThreadFactory fanOutThreads = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(reActorSystemName + "-fanout-%d")
                .setUncaughtExceptionHandler((thread, error) -> LOGGER.error("Critic! FanOut Thread Terminated", error))
                .build();
        return Executors.newFixedThreadPool(poolSize, fanOutThreads);
    }
}
