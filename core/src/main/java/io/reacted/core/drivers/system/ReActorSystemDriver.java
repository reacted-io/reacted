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
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.NullReActorSystemRef;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import io.reacted.patterns.UnChecked.TriConsumer;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
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
    @Nullable
    public static final TriConsumer<ReActorId, Serializable, ReActorRef> DO_NOT_PROPAGATE = null;
    public static final ThreadLocal<DriverCtx> REACTOR_SYSTEM_CTX = new InheritableThreadLocal<>();
    protected static final Logger LOGGER = LoggerFactory.getLogger(ReActorSystemDriver.class);
    @SuppressWarnings("unchecked")
    protected static final CompletionStage<DeliveryStatus>[] DELIVERY_RESULT_CACHE = Arrays.stream(DeliveryStatus.values())
                                                                                           .map(CompletableFuture::completedStage)
                                                                                           .toArray(CompletionStage[]::new);
    private final ConfigT driverConfig;
    private final Cache<Long, CompletableFuture<DeliveryStatus>> pendingAcksTriggers;
    @Nullable
    private ScheduledFuture<?> cacheMaintenanceTask;
    @Nullable
    private ReActorSystem localReActorSystem;
    @Nullable
    private ExecutorService driverThread;

    protected ReActorSystemDriver(ConfigT config) {
        this.driverConfig = Objects.requireNonNull(config,
                                                   "Driver config cannot be null");
        this.pendingAcksTriggers = CacheBuilder.newBuilder()
                                               .expireAfterWrite(config.getApublishAutomaticFailureTimeout()
                                                                       .toMillis(), TimeUnit.MILLISECONDS)
                                               .initialCapacity(config.getAckCacheSize())
                                               .removalListener((RemovalListener<Long,
                                                                 CompletableFuture<DeliveryStatus>>)
                                                                        ReActorSystemDriver::expireOnTimeout)
                                               .build();
    }

    public abstract void initDriverLoop(ReActorSystem localReActorSystem) throws Exception;
    public abstract CompletionStage<Try<Void>> cleanDriverLoop();
    public abstract UnChecked.CheckedRunnable getDriverLoop();
    public abstract ChannelId getChannelId();
    public abstract Properties getChannelProperties();
    /**
     * @throws io.reacted.core.exceptions.DeliveryException when a driver specific delivery error occurs
     */
    public abstract <PayloadT extends Serializable> DeliveryStatus
    sendMessage(ReActorRef src, ReActorContext destinationCtx, ReActorRef destination, long seqNum,
                ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message);
    public <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    sendAsyncMessage(ReActorRef src, ReActorContext destinationCtx, ReActorRef destination,
                     long seqNum, ReActorSystemId reActorSystemId,
                     AckingPolicy ackingPolicy, PayloadT message) {
        return DELIVERY_RESULT_CACHE[sendMessage(src, destinationCtx, destination, seqNum, reActorSystemId,
                                                 ackingPolicy, message).ordinal()];
    }
    public ConfigT getDriverConfig() { return driverConfig; }

    /**
     * Sends a message through this driver
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link DeliveryStatus} representing the outcome of the operation. Different drivers
     * may offer different guarantees regarding the returned {@link DeliveryStatus}. The common
     * baseline for this method is providing delivery guarantee to the local driver bus
     */
    public abstract <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef src, ReActorRef dst, PayloadT message);

    /**
     * Sends a message through this driver
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param propagateToSubscribers a consumer that will be called for every new message will be invoked
     *                               for propagating that message to its type-subscribers.
     * @see io.reacted.core.typedsubscriptions.TypedSubscription
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link DeliveryStatus} representing the outcome of the operation. Different drivers
     * may offer different guarantees regarding the returned {@link DeliveryStatus}. The common
     * baseline for this method is providing delivery guarantee to the local driver bus
     */
    public abstract  <PayloadT extends Serializable> DeliveryStatus publish(ReActorRef src, ReActorRef dst,
                                                                            @Nullable TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message);
    /**
     * Sends a message through this driver. Type subscribers will not be notified
     * @see io.reacted.core.typedsubscriptions.TypedSubscription
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link DeliveryStatus} representing the outcome of the operation. Different drivers
     * may offer different guarantees regarding the returned {@link DeliveryStatus}. The common
     * baseline for this method is providing delivery guarantee to the local driver bus
     */
    public abstract <PayloadT extends Serializable> DeliveryStatus tell(ReActorRef src, ReActorRef dst, PayloadT message);
    /**
     * Sends a message through this driver requiring an ack as a confirmation of the delivery into the target reactor's
     * mailbox. Type subscribers will not be notified.
     * @see io.reacted.core.typedsubscriptions.TypedSubscription
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param ackingPolicy the {@link AckingPolicy} that should be used for managing the ack control for this message
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link CompletionStage} that is going to be completed when an ack from the destination reactor system
     * is received containing the outcome of the delivery of the message into the target actor mailbox
     */
    public abstract <PayloadT extends Serializable> CompletionStage<DeliveryStatus> atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);
    /**
     * Sends a message through this driver requiring an ack as a confirmation of the delivery into the target reactor's
     * mailbox.
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param ackingPolicy the {@link AckingPolicy} that should be used for managing the ack control for this message
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link CompletionStage} that is going to be completed when an ack from the destination reactor system
     * is received containing the outcome of the delivery of the message into the target actor mailbox
     */
    public abstract <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);
    /**
     * Sends a message through this driver requiring an ack as a confirmation of the delivery into the target reactor's
     * mailbox.
     *
     * @param src source of the message
     * @param dst destination of the message
     * @param ackingPolicy the {@link AckingPolicy} that should be used for managing the ack control for this message
     * @param propagateToSubscribers a consumer that will be called for every new message will be invoked
     *                               for propagating that message to its type-subscribers.
     * @see io.reacted.core.typedsubscriptions.TypedSubscription
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return A {@link CompletionStage} that is going to be completed when an ack from the destination reactor system
     * is received containing the outcome of the delivery of the message into the target actor mailbox
     */
    public abstract  <PayloadT extends Serializable> CompletionStage<DeliveryStatus> apublish(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
                                                                                              TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message);

    protected <PayloadT extends Serializable> void offerMessage(ReActorRef source, ReActorRef destination,
                                                                long sequenceNumber,
                                                                ReActorSystemId fromReActorSystemId,
                                                                AckingPolicy ackingPolicy,
                                                                PayloadT payload) {
        getLocalReActorSystem().logError("Invalid message offering {}", payload,
                                         new NotImplementedException());
    }
    @Nullable
    public CompletionStage<DeliveryStatus> removePendingAckTrigger(long msgSeqNum) {
        var ackTrigger = pendingAcksTriggers.getIfPresent(msgSeqNum);
        pendingAcksTriggers.invalidate(msgSeqNum);
        return ackTrigger;
    }

    public CompletionStage<DeliveryStatus> newPendingAckTrigger(long msgSeqNum) {
        CompletableFuture<DeliveryStatus> newTrigger = new CompletableFuture<>();
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

    @Nullable
    public static DriverCtx getDriverCtx() {
        return REACTOR_SYSTEM_CTX.get();
    }

    public ReActorSystem getLocalReActorSystem() { return Objects.requireNonNull(localReActorSystem); }

    public static boolean isLocalReActorSystem(ReActorSystemId loopback, ReActorSystemId other) {
        return loopback.equals(other);
    }

    protected static boolean isMessageComingFromLocalReActorSystem(ReActorSystemId localReActorSystemId,
                                                                   ReActorSystemId fromReActorSystemId) {
        return localReActorSystemId.equals(fromReActorSystemId);
    }

    protected static DeliveryStatus
    sendDeliveryAck(ReActorSystem localReActorSystem, ChannelId gateChannelId,
                    DeliveryStatus deliveryResult, long originalSequenceNumber,
                    ReActorSystemId fromReActorSystemId) {
        var statusUpdatePayload = new DeliveryStatusUpdate(originalSequenceNumber,
                                                           deliveryResult,
                                                           localReActorSystem.getLocalReActorSystemId(),
                                                           gateChannelId);
        /* An ack has to be sent not to the nominal sender, but to the reactorsystem that actually generated the message
           because that is the one that is actually waiting for an ACK.
           Here we are supporting asymmetric routes: theoretically this ack could go back to the
           original sender using a different driver
         */
        var destSystem = localReActorSystem.findGate(fromReActorSystemId, gateChannelId);
        if (destSystem == null) {
            destSystem = NullReActorSystemRef.NULL_REACTOR_SYSTEM_REF;
        }
        var destReActor = new ReActorRef(ReActorId.NO_REACTOR_ID, destSystem);
        return destReActor.tell(ReActorRef.NO_REACTOR_REF, statusUpdatePayload);
    }

    private static void
    expireOnTimeout(RemovalNotification<Long, CompletableFuture<DeliveryStatus>> notification) {
        if (notification.getCause() != RemovalCause.EXPLICIT) {
            notification.getValue().completeExceptionally(new TimeoutException());
        }
    }
}
