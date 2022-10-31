/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import com.google.common.cache.*;
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
import io.reacted.core.reactorsystem.*;
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
import java.util.concurrent.*;

@NonNullByDefault
public abstract class ReActorSystemDriver<ConfigT extends ChannelDriverConfig<?, ConfigT>> {
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
                                               .expireAfterWrite(config.getAtellAutomaticFailureTimeout()
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
     * @throws io.reacted.core.exceptions.DeliveryException
     */
    public abstract DeliveryStatus sendMessage(ReActorContext destination, Message message);
    public CompletionStage<DeliveryStatus> sendAsyncMessage(ReActorContext destination, Message message) {
        return DELIVERY_RESULT_CACHE[sendMessage(destination, message).ordinal()];
    }

    public ConfigT getDriverConfig() { return driverConfig; }

    /**
     * @param src source of the message
     * @param dst destination of the message
     * @param message payload
     * @param <PayloadT> any Serializable object
     * @return a completion stage that is going to be completed on error or when the message is successfully delivered
     *         to the target mailbox
     */
    public abstract <PayloadT extends Serializable> DeliveryStatus
    tell(ReActorRef src, ReActorRef dst, PayloadT message);
    public abstract  <PayloadT extends Serializable> DeliveryStatus
    tell(ReActorRef src, ReActorRef dst,
         TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message);
    public abstract <PayloadT extends Serializable> DeliveryStatus
    route(ReActorRef src, ReActorRef dst, PayloadT message);
    public abstract <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);
    public abstract  <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    atell(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy,
         TriConsumer<ReActorId, Serializable, ReActorRef> propagateToSubscribers, PayloadT message);
    public abstract <PayloadT extends Serializable> CompletionStage<DeliveryStatus>
    aroute(ReActorRef src, ReActorRef dst, AckingPolicy ackingPolicy, PayloadT message);

    protected void offerMessage(Message message) {
        getLocalReActorSystem().logError("Invalid message offering {}", message,
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
                                                                   DataLink msgDataLink) {
        return localReActorSystemId.equals(msgDataLink.getGeneratingReActorSystem());
    }

    protected static DeliveryStatus
    sendDeliveryAck(ReActorSystem localReActorSystem, ChannelId gateChannelId,
                    DeliveryStatus deliveryResult, Message originalMessage) {
        var statusUpdatePayload = new DeliveryStatusUpdate(originalMessage.getSequenceNumber(),
                                                           deliveryResult,
                                                           localReActorSystem.getLocalReActorSystemId(),
                                                           gateChannelId);
        /* An ack has to be sent not to the nominal sender, but to the reactorsystem that actually generated the message
           because that is the one that is actually waiting for an ACK.
           Here we are supporting asymmetric routes: theoretically this ack could go back to the
           original sender using a different driver
         */
        var destSystem = localReActorSystem.findGate(originalMessage.getDataLink()
                                                                    .getGeneratingReActorSystem(),
                                                     gateChannelId);
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
