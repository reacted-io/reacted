/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.DataLink;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NonNullByDefault
public abstract class ReActorSystemDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>> {
    public static final ThreadLocal<DriverCtx> REACTOR_SYSTEM_CTX = new InheritableThreadLocal<>();
    protected static final Logger LOGGER = LoggerFactory.getLogger(ReActorSystemDriver.class);
    private final ConfigT driverConfig;
    private final Cache<Long, CompletableFuture<Try<DeliveryStatus>>> pendingAcksTriggers;
    @Nullable
    private ScheduledFuture<?> cacheMaintenanceTask;
    @Nullable
    private ReActorSystem localReActorSystem;
    @Nullable
    private ExecutorService driverThread;

    protected ReActorSystemDriver(ConfigT config) {
        this.driverConfig = Objects.requireNonNull(config);
        this.pendingAcksTriggers = CacheBuilder.newBuilder()
                                               .expireAfterWrite(config.getAtellAutomaticFailureTimeout()
                                                                       .toMillis(), TimeUnit.MILLISECONDS)
                                               .initialCapacity(10_000_000)
                                               .removalListener((RemovalListener<Long,
                                                                 CompletableFuture<Try<DeliveryStatus>>>)
                                                                        ReActorSystemDriver::expireOnTimeout)
                                               .build();
    }

    public abstract void initDriverLoop(ReActorSystem localReActorSystem) throws Exception;
    public abstract CompletionStage<Try<Void>> cleanDriverLoop();
    public abstract UnChecked.CheckedRunnable getDriverLoop();
    public abstract ChannelId getChannelId();
    public abstract Properties getChannelProperties();
    public abstract Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message);
    public abstract CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message);
    public abstract boolean channelRequiresDeliveryAck();

    public ConfigT getDriverConfig() { return driverConfig; }

    /**
     * @param src source of the message
     * @param dst destination of the message
     * @param ackingPolicy A {@link AckingPolicy} defining how or if this message should be ack-ed
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return a completion stage that is going to be completed on error or when the message is successfully delivered
     *         to the target mailbox
     */
    public abstract <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
    tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);
    public abstract  <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
    tell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
         TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message);
    public abstract <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>>
    route(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);

    @Nullable
    public CompletionStage<Try<DeliveryStatus>> removePendingAckTrigger(long msgSeqNum) {
        var ackTrigger = pendingAcksTriggers.getIfPresent(msgSeqNum);
        pendingAcksTriggers.invalidate(msgSeqNum);
        return ackTrigger;
    }

    public CompletionStage<Try<DeliveryStatus>> newPendingAckTrigger(long msgSeqNum) {
        CompletableFuture<Try<DeliveryStatus>> newTrigger = new CompletableFuture<>();
        pendingAcksTriggers.put(msgSeqNum, newTrigger);
        return newTrigger;
    }

    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        this.localReActorSystem = localReActorSystem;
        ThreadFactory driverThreadDetails = new ThreadFactoryBuilder()
                .setNameFormat(localReActorSystem.getLocalReActorSystemId().getReActorSystemName() + "-" +
                               getChannelId() + "-" + getClass().getSimpleName() + "-driver-%d")
                .setUncaughtExceptionHandler((thread, error) ->
                                                     localReActorSystem.logError("Uncaught error in driver thread {} ",
                                                                                 thread.getName(), error))
                .build();
        this.driverThread = Executors.newFixedThreadPool(1, driverThreadDetails);
        this.cacheMaintenanceTask = localReActorSystem.getSystemSchedulingService()
                                                      .scheduleWithFixedDelay(pendingAcksTriggers::cleanUp,
                                                                               getDriverConfig().getAckCacheCleanupInterval()
                                                                                                .toMillis(),
                                                                               getDriverConfig().getAckCacheCleanupInterval()
                                                                                                .toMillis(),
                                                                               TimeUnit.MILLISECONDS);

        Try<Void> initDriver = CompletableFuture.runAsync(() -> REACTOR_SYSTEM_CTX.set(new DriverCtx(localReActorSystem, this)),
                                                          driverThread)
                                                .thenApplyAsync(vV -> Try.ofRunnable(() -> initDriverLoop(localReActorSystem)),
                                                                driverThread)
                                                .join();
        initDriver.ifSuccessOrElse(vV -> CompletableFuture.supplyAsync(() -> Try.ofRunnable(getDriverLoop())
                                                                                .peekFailure(error -> LOGGER.error("Driver body failed:", error))
                                                                                .ifError(error -> stopDriverCtx(localReActorSystem)),
                                                                       driverThread),
                                   error -> { LOGGER.error("Driver {} init failed", getClass().getSimpleName(), error);
                                              stopDriverCtx(localReActorSystem); });
        return initDriver;
    }

    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        Objects.requireNonNull(cacheMaintenanceTask).cancel(true);
        Objects.requireNonNull(driverThread).shutdownNow();
        return cleanDriverLoop();
    }

    public static Optional<DriverCtx> getDriverCtx() {
        return Optional.ofNullable(REACTOR_SYSTEM_CTX.get());
    }

    public ReActorSystem getLocalReActorSystem() { return Objects.requireNonNull(localReActorSystem); }

    public static boolean isLocalReActorSystem(ReActorSystemId loopback, ReActorSystemId other) {
        return loopback.equals(other);
    }

    protected static boolean isMessageComingFromLocalReActorSystem(ReActorSystemId localReActorSystemId,
                                                                   DataLink msgDataLink) {
        return localReActorSystemId.equals(msgDataLink.getGeneratingReActorSystem());
    }

    protected static boolean isAckRequired(boolean isAckRequiredByChannel, AckingPolicy messageAckingPolicy) {
        return messageAckingPolicy != AckingPolicy.NONE && isAckRequiredByChannel;
    }

    protected static Try<DeliveryStatus> sendDeliveryAck(ReActorSystemId localReActorSystemId,
                                                         long ackSeqNum,
                                                         ReActorSystemDriver<? extends ChannelDriverConfig<?, ?>> gate,
                                                         Try<DeliveryStatus> deliveryResult, Message originalMessage) {
        var statusUpdatePayload = new DeliveryStatusUpdate(originalMessage.getSequenceNumber(),
                                                           deliveryResult.orElse(DeliveryStatus.NOT_DELIVERED));
        /* An ack has to be sent not to the nominal sender, but to the reactorsystem that actually generated the message
           because that is the one that is actually waiting for an ACK
         */
        var destSystem = gate.getLocalReActorSystem()
                             .findGate(originalMessage.getDataLink().getGeneratingReActorSystem(),
                                       gate.getChannelId())
                             //The point of the below statement is just returning empty properties because
                             //the gate and so the channel id are already known
                             //As a future optimization this could be changed to NullReActorSystemRef, but if it's
                             //not required the below approach is the type-clean approach
                             .orElseGet(() -> new ReActorSystemRef(gate, new Properties(),
                                                                   originalMessage.getDataLink()
                                                                                  .getGeneratingReActorSystem()));

        return gate.sendMessage(ReActorContext.NO_REACTOR_CTX,
                                new Message(ReActorRef.NO_REACTOR_REF,
                                            new ReActorRef(ReActorId.NO_REACTOR_ID, destSystem),
                                            ackSeqNum, localReActorSystemId, AckingPolicy.NONE, statusUpdatePayload));
    }

    private static void
    expireOnTimeout(RemovalNotification<Long, CompletableFuture<Try<DeliveryStatus>>> notification) {
        if (notification.getCause() != RemovalCause.EXPLICIT) {
            notification.getValue().completeAsync(() -> Try.ofFailure(new TimeoutException()));
        }
    }
}
