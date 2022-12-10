/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.MailBox;
import io.reacted.core.mailboxes.NullMailbox;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.typedsubscriptions.TypedSubscriptionsManager;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

@NonNullByDefault
public class ReActorContext {
    public static final ReActorContext NO_REACTOR_CTX = ReActorContext.newBuilder()
                                                                      .setMbox(raCtx -> new NullMailbox())
                                                                      .setParentActor(ReActorRef.NO_REACTOR_REF)
                                                                      .setReactorRef(ReActorRef.NO_REACTOR_REF)
                                                                      .setReActions(ReActions.NO_REACTIONS)
                                                                      .setSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                                                      .setDispatcher(Dispatcher.NULL_DISPATCHER)
                                                                      .setReActorSystem(ReActorSystem.NO_REACTOR_SYSTEM)
                                                                      .build();
    private final MailBox actorMbox;
    private final ReActorRef reactorRef;
    private final ReActorSystem reActorSystem;
    private final Set<ReActorRef> children;
    private final ReActorRef parent;
    private final Dispatcher dispatcher;
    private final AtomicBoolean isScheduled;
    private final ReadWriteLock structuralLock;
    private final CompletionStage<Void> hierarchyTermination;
    private final AtomicLong msgExecutionId;
    private final ReActions reActions;
    private final long reActorSchedulationId;

    private TypedSubscription[] typedSubscriptions;

    private volatile boolean stop = false;
    private volatile boolean isAcquired = false;

    private ReActorRef lastMsgSender = ReActorRef.NO_REACTOR_REF;

    private ReActorContext(Builder reActorCtxBuilder) {
        this.actorMbox = Objects.requireNonNull(Objects.requireNonNull(reActorCtxBuilder.mboxProvider)
                                                       .apply(this));
        this.reactorRef = Objects.requireNonNull(reActorCtxBuilder.reactorRef);
        this.reActorSystem = Objects.requireNonNull(reActorCtxBuilder.reActorSystem);
        this.children = ConcurrentHashMap.newKeySet();
        this.parent = Objects.requireNonNull(reActorCtxBuilder.parent);
        this.dispatcher = Objects.requireNonNull(reActorCtxBuilder.dispatcher);
        this.isScheduled = new AtomicBoolean(false);
        this.structuralLock = new ReentrantReadWriteLock();
        this.typedSubscriptions = Objects.requireNonNull(reActorCtxBuilder.typedSubscriptions).length == 0
                                  ? TypedSubscription.NO_SUBSCRIPTIONS
                                  : TypedSubscriptionsManager.getNormalizedSubscriptions(reActorCtxBuilder.typedSubscriptions);
        this.hierarchyTermination = new CompletableFuture<>();
        this.msgExecutionId = new AtomicLong();
        this.reActions = Objects.requireNonNull(reActorCtxBuilder.reActions);
        this.reActorSchedulationId = ReActorCounter.INSTANCE.nextSchedulationId();
    }
    public static Builder newBuilder() { return new Builder(); }

    public ReActorRef getSelf() { return reactorRef; }

    public ReActorSystem getReActorSystem() { return reActorSystem; }

    public Set<ReActorRef> getChildren() { return children; }

    public ReActorRef getParent() { return parent; }

    public Dispatcher getDispatcher() { return dispatcher; }

    public MailBox getMbox() { return actorMbox; }
    public CompletionStage<Void> getHierarchyTermination() { return hierarchyTermination; }

    public long getNextMsgExecutionId() { return msgExecutionId.getAndIncrement(); }

    public boolean acquireScheduling() {
        return isScheduled.compareAndSet(false, true);
    }

    public boolean releaseScheduling() { return isScheduled.compareAndSet(true, false); }

    @SuppressWarnings("UnusedReturnValue")
    public boolean acquireCoherence() { return !isAcquired; }

    public void releaseCoherence() { this.isAcquired = false; }

    public void refreshInterceptors(TypedSubscription... newInterceptedClasses) {

        getStructuralLock().writeLock().lock();
        try {
            getReActorSystem().updateMessageInterceptors(this, typedSubscriptions, newInterceptedClasses);
            this.typedSubscriptions = newInterceptedClasses;
        } finally {
            getStructuralLock().writeLock().unlock();
        }
    }

    public TypedSubscription[] getTypedSubscriptions() {
        TypedSubscription[] interceptedMsgTypes;

        getStructuralLock().readLock().lock();
        interceptedMsgTypes = Arrays.copyOf(typedSubscriptions, typedSubscriptions.length);
        getStructuralLock().readLock().unlock();

        return interceptedMsgTypes;
    }

    public boolean reschedule() { return getDispatcher().dispatch(this); }

    public DeliveryStatus reply(Serializable anyPayload) { return reply(getSelf(), anyPayload); }

    public DeliveryStatus reply(ReActorRef sender, Serializable anyPayload) {
        return getSender().publish(sender, anyPayload);
    }

    public Try<ScheduledFuture<DeliveryStatus>>
    rescheduleMessage(Serializable messageToBeRescheduled, Duration inHowLong) {
        ReActorRef sender = getSender();
        return Try.of(() -> getReActorSystem().getSystemSchedulingService()
                                              .schedule(() -> getSelf().tell(sender, messageToBeRescheduled),
                                                       inHowLong.toMillis(), TimeUnit.MILLISECONDS));
    }

    /**
     * Reply sending a message to the sender of the last message processed by this reactor using {@link ReActorRef#apublish(Serializable)}
     * @param anyPayload payload to be sent
     * @return a {@link CompletionStage}&lt;{@link Try}&lt;{@link DeliveryStatus}&gt;&gt; returned by {@link ReActorRef#apublish(ReActorRef, Serializable)}
     */
    public CompletionStage<DeliveryStatus> areply(Serializable anyPayload) {
        return getSender().apublish(anyPayload);
    }

    /**
     * {@link ReActorRef#publish(Serializable)} to the current reactor the specified message setting itself as sender for the message
     * @param anyPayload message that should be self-sent
     * @return A {@link DeliveryStatus}
     * complete
     */
    public DeliveryStatus selfPublish(Serializable anyPayload) {
        return getSelf().publish(getSelf(), anyPayload);
    }

    /**
     * {@link ReActorRef#tell(Serializable)} to the current reactor the specified message setting itself as sender for the message
     * @param anyPayload message that should be self-sent
     * @return A {@link DeliveryStatus}
     * complete
     */
    public DeliveryStatus selfTell(Serializable anyPayload) {
        return getSelf().tell(getSelf(), anyPayload);
    }

    /**
     * Spawn a new {@link ReActor} child of the spawning one
     * @param reActor the new {@link ReActor} definition
     * @return a {@link Try}&lt;{@link ReActorRef}&gt; pointing to the new {@link ReActor}
     */
    public Try<ReActorRef> spawnChild(ReActor reActor) {
        return getReActorSystem().spawnChild(reActor.getReActions(), getSelf(), reActor.getConfig());
    }

    /**
     * Spawn a new {@link ReActor} child of the spawning one
     * @param reActiveEntity the {@link ReActiveEntity} definition for the new {@link ReActor}
     * @param reActorConfig the {@link ReActorConfig} for the new {@link ReActor}
     * @return a {@link Try}&lt;{@link ReActorRef}&gt; containing a {@link ReActorRef} pointing to the new {@link ReActor}
     */
    public Try<ReActorRef> spawnChild(ReActiveEntity reActiveEntity, ReActorConfig reActorConfig) {
        return getReActorSystem().spawnChild(reActiveEntity.getReActions(), getSelf(), reActorConfig);
    }

    /**
     * Spawn a new {@link ReActor} child of the spawning one
     * @param reActions the {@link ReActions} for the new {@link ReActor}
     * @param reActorConfig the {@link ReActorConfig} for the new {@link ReActor}
     * @return a {@link Try}&lt;{@link ReActorRef}&gt; containing a {@link ReActorRef} pointing to the new {@link ReActor}
     */
    public Try<ReActorRef> spawnChild(ReActions reActions, ReActorConfig reActorConfig) {
        return getReActorSystem().spawnChild(reActions, getSelf(), reActorConfig);
    }

    /**
     * Set the message subscriptions rules for this reactor to enable passive message sniffing
     * @param newTypedSubscriptions {@link TypedSubscription} array
     */
    public final void setTypedSubscriptions(TypedSubscription ...newTypedSubscriptions) {
        refreshInterceptors(Objects.requireNonNull(newTypedSubscriptions).length == 0
                            ? TypedSubscription.NO_SUBSCRIPTIONS
                            : TypedSubscriptionsManager.getNormalizedSubscriptions(newTypedSubscriptions));
    }

    /**
     * Add the specified {@link TypedSubscription}s to the current set
     * @param typedSubscriptionsToAdd {@link TypedSubscription}s to add
     *
     */
    public final void addTypedSubscriptions(TypedSubscription ...typedSubscriptionsToAdd) {
        setTypedSubscriptions(TypedSubscriptionsManager.getNormalizedSubscriptions(Stream.concat(Arrays.stream(typedSubscriptionsToAdd),
                                                                                                 Arrays.stream(getTypedSubscriptions()))
                                                                                         .toArray(TypedSubscription[]::new)));
    }
    /**
     * Request termination for this reactor and the underlying hierarchy
     * @return a {@link CompletionStage} that is going to be completed when the last reactor in the hierarchy
     * is terminated
     */
    public CompletionStage<Void> stop() {
        this.stop = true;
        reschedule();
        return getHierarchyTermination();
    }

    public boolean isStop() { return stop; }

    /**
     * Send a logging request for info level to the centralized logger reactor
     *
     * @param descriptionFormat description in sl4j format
     * @param args arguments list
     */
    public void logInfo(String descriptionFormat, Serializable ...args) {
        getReActorSystem().logInfo(descriptionFormat, args);
    }

    /**
     * Send a logging request for error level to the centralized logger reactor
     *
     * @param descriptionFormat description in sl4j format
     * @param args arguments list
     */
    public void logError(String descriptionFormat, Serializable ...args) {
        getReActorSystem().logError(descriptionFormat, args);
    }

    /**
     * Send a logging request for debug level to the centralized logger reactor
     *
     * @param descriptionFormat description in sl4j format
     * @param args arguments list
     */
    public void logDebug(String descriptionFormat, Serializable ...args) {
        getReActorSystem().logDebug(descriptionFormat, args);
    }

    public void reAct(Message msg) {
        this.lastMsgSender = msg.getSender();
        BiConsumer<ReActorContext, Serializable> reAction = reActions.getReAction(msg.getPayload());
        reAction.accept(this, msg.getPayload());
    }

    /**
     * Get the sender of the last message processed by this reactor
     * @return {@link ReActorRef} to the sender
     */
    public ReActorRef getSender() {
        return lastMsgSender;
    }

    public long getReActorSchedulationId() { return reActorSchedulationId; }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof ReActorContext that)) return false;
        return getSelf().equals(that.getSelf());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSelf());
    }

    ReadWriteLock getStructuralLock() { return structuralLock; }

    @SuppressWarnings("UnusedReturnValue")
    boolean registerChild(ReActorRef childActor) {
        return children.add(childActor);
    }

    @SuppressWarnings("UnusedReturnValue")
    boolean unregisterChild(ReActorRef childActor) {
        return children.remove(childActor);
    }

    @SuppressWarnings("NotNullFieldNotInitialized")
    public static class Builder {
        private Function<ReActorContext, MailBox> mboxProvider;
        private ReActorRef reactorRef;
        private ReActorSystem reActorSystem;
        private ReActorRef parent;
        private TypedSubscription[] typedSubscriptions;
        private Dispatcher dispatcher;
        private ReActions reActions;

        public final Builder setMbox(Function<ReActorContext, MailBox> actorMboxProvider) {
            this.mboxProvider = actorMboxProvider;
            return this;
        }

        public final Builder setReactorRef(ReActorRef reactorRef) {
            this.reactorRef = reactorRef;
            return this;
        }

        public final Builder setReActorSystem(ReActorSystem reActorSystem) {
            this.reActorSystem = reActorSystem;
            return this;
        }

        public final Builder setParentActor(ReActorRef parentActor) {
            this.parent = parentActor;
            return this;
        }

        public final Builder setSubscriptions(TypedSubscription... typedSubscriptions) {
            this.typedSubscriptions = typedSubscriptions;
            return this;
        }

        public final Builder setDispatcher(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public final Builder setReActions(ReActions reActions) {
            this.reActions = reActions;
            return this;
        }

        public ReActorContext build() {
            return new ReActorContext(this);
        }
    }
}
