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
import io.reacted.core.config.drivers.ReActedDriverCfg;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
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
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NonNullByDefault
public abstract class ReActorSystemDriver<DriverCfgT extends ReActedDriverCfg<?, DriverCfgT>> {
    public static final ThreadLocal<DriverCtx> REACTOR_SYSTEM_CTX = new InheritableThreadLocal<>();
    protected static final Logger LOGGER = LoggerFactory.getLogger(ReActorSystem.class);
    private final DriverCfgT driverConfig;
    private final Cache<Long, CompletableFuture<Try<DeliveryStatus>>> pendingAcksTriggers;
    @Nullable
    private ReActorSystem localReActorSystem;
    @Nullable
    private ExecutorService driverThread;

    protected ReActorSystemDriver(DriverCfgT driverCfg) {
        this.driverConfig = Objects.requireNonNull(driverCfg);
        this.pendingAcksTriggers = CacheBuilder.newBuilder()
                                               .expireAfterWrite(driverCfg.getAtellAutomaticFailureTimeout()
                                                                          .toMillis(), TimeUnit.MILLISECONDS)
                                               .initialCapacity(10_000_000)
                                               .removalListener(new RemovalListener<Long, CompletableFuture<Try<DeliveryStatus>>>() {
                                                   @Override
                                                   public void onRemoval(RemovalNotification<Long, CompletableFuture<Try<DeliveryStatus>>> notification) {
                                                       if (notification.getCause() != RemovalCause.EXPLICIT) {
                                                           notification.getValue()
                                                                       .completeAsync(() -> Try.ofFailure(new TimeoutException()));
                                                       }
                                                   }
                                               }).build();
    }

    abstract public void initDriverLoop(ReActorSystem localReActorSystem) throws Exception;
    abstract public CompletionStage<Try<Void>> cleanDriverLoop();
    abstract public UnChecked.CheckedRunnable getDriverLoop();
    abstract public ChannelId getChannelId();
    abstract public Properties getChannelProperties();
    abstract public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message);
    abstract public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message);
    abstract public boolean channelRequiresDeliveryAck();

    public DriverCfgT getDriverConfig() { return driverConfig; }

    /**
     * @param src source of the message
     * @param dst destination of the message
     * @param ackingPolicy An {@link AckingPolicy} defining how or if this message should be ack-ed
     * @param message payload
     * @return a completion stage that is going to be completed on error or when the message is successfully delivered
     *         to the target mailbox
     */
    abstract public <PayloadT extends Serializable> CompletionStage<Try<DeliveryStatus>> tell(ReActorRef src,
                                                                                              ReActorRef dst,
                                                                                              AckingPolicy ackingPolicy,
                                                                                              PayloadT message);

    public Optional<CompletableFuture<Try<DeliveryStatus>>> removePendingAckTrigger(long msgSeqNum) {
        var ackTrigger = this.pendingAcksTriggers.getIfPresent(msgSeqNum);
        this.pendingAcksTriggers.invalidate(msgSeqNum);
        return Optional.ofNullable(ackTrigger);
    }

    public CompletionStage<Try<DeliveryStatus>> newPendingAckTrigger(long msgSeqNum) {
        CompletableFuture<Try<DeliveryStatus>> newTrigger = new CompletableFuture<>();
        this.pendingAcksTriggers.put(msgSeqNum, newTrigger);
        return newTrigger;
    }

    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        this.localReActorSystem = localReActorSystem;
        ThreadFactory driverThreadDetails = new ThreadFactoryBuilder()
                .setNameFormat(localReActorSystem.getLocalReActorSystemId().getReActorSystemName() + "-" +
                               getChannelId() + "-" + getClass().getSimpleName() + "-driver-%d")
                .setUncaughtExceptionHandler((thread, error) -> localReActorSystem.logError("Uncaught error in driver thread {} ",
                                                                                            thread.getName(), error))
                .build();
        this.driverThread = Executors.newFixedThreadPool(1, driverThreadDetails);

        Try<Void> initDriver = CompletableFuture.runAsync(() -> REACTOR_SYSTEM_CTX.set(new DriverCtx(localReActorSystem,
                                                                                                     this)),
                                                          driverThread)
                                                .thenApplyAsync(vV -> Try.ofRunnable(() -> initDriverLoop(localReActorSystem)),
                                                                driverThread)
                                                .join();
        initDriver.peekFailure(error -> LOGGER.error("Driver {} init failed",getClass().getSimpleName(), error))
                  .peekFailure(error -> stopDriverCtx(localReActorSystem))
                  .ifSuccess(vV -> CompletableFuture.supplyAsync(() -> Try.ofRunnable(getDriverLoop()), driverThread)
                                                    .thenAccept(retVal -> retVal.peekFailure(error -> LOGGER.error("Driver body failed:", error))
                                                                                .ifError(error -> stopDriverCtx(localReActorSystem))));
        return initDriver;
    }

    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        Objects.requireNonNull(this.driverThread).shutdownNow();
        return this.cleanDriverLoop();
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

    protected static boolean isTypeSniffed(ReActorSystem localReActorSystem, Class<? extends Serializable> payloadType) {
        var subscribersGroup = localReActorSystem.getTypedSubscribers().getKeyGroup(payloadType);
        return !(subscribersGroup.get(TypedSubscriptionPolicy.LOCAL).isEmpty() &&
                 subscribersGroup.get(TypedSubscriptionPolicy.REMOTE).isEmpty());
    }

    protected static boolean isAckRequired(boolean isAckRequiredByChannel, AckingPolicy messageAckingPolicy) {
        return messageAckingPolicy != AckingPolicy.NONE && isAckRequiredByChannel;
    }

    protected static boolean isAckRequired(AckingPolicy ackingPolicy, Properties senderChannelProperties) {
        String isAckRequiredByChannelProperty = (String)senderChannelProperties.getOrDefault(ReActedDriverCfg.IS_DELIVERY_ACK_REQUIRED_BY_CHANNEL_PROPERTY_NAME,
                                                                                             "false");

        return isAckRequired(isAckRequiredByChannelProperty.compareToIgnoreCase("true") == 0, ackingPolicy);
    }

    protected static Try<DeliveryStatus> sendDeliveyAck(ReActorSystemId localReActorSystemId,
                                                        long ackSeqNum,
                                                        ReActorSystemDriver<? extends ReActedDriverCfg<?, ?>> gate,
                                                        Try<DeliveryStatus> deliveryResult, Message originalMessage) {
        var statusUpdatePayload = new DeliveryStatusUpdate(originalMessage.getSequenceNumber(),
                                                           deliveryResult.orElse(DeliveryStatus.NOT_DELIVERED));
        return gate.sendMessage(ReActorContext.NO_REACTOR_CTX,
                                new Message(ReActorRef.NO_REACTOR_REF,
                                            new ReActorRef(ReActorId.NO_REACTOR_ID,
                                                           originalMessage.getSender().getReActorSystemRef()),
                                            ackSeqNum, localReActorSystemId, AckingPolicy.NONE, statusUpdatePayload));
    }
}
