/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.config.ConfigUtils;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.streams.messages.PublisherShutdown;
import io.reacted.streams.messages.SubscriberComplete;
import io.reacted.streams.messages.SubscriptionReply;
import io.reacted.streams.messages.SubscriptionRequest;
import io.reacted.streams.messages.UnsubscriptionRequest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@NonNullByDefault
public class ReactedSubmissionPublisher<PayloadT extends Serializable> implements Flow.Publisher<PayloadT>,
                                                                                  AutoCloseable, Externalizable {
    private static final long FEED_GATE_OFFSET = SerializationUtils.getFieldOffset(ReactedSubmissionPublisher.class,
                                                                                   "feedGate")
                                                                   .orElseSneakyThrow();
    private static final long LOCAL_REACTOR_SYSTEM =
            SerializationUtils.getFieldOffset(ReactedSubmissionPublisher.class, "localReActorSystem")
                                                                       .orElseSneakyThrow();
    private final transient Set<ReActorRef> subscribers;
    private final transient ReActorSystem localReActorSystem;
    private final ReActorRef feedGate;

    /**
     * Creates a location oblivious data publisher with backpressure. Subscribers can slowdown
     * the producer or just drop data if they are best effort subscribers. Publisher is Serializable,
     * so it can be sent to any reactor over any gate and subscribers can simply join the stream.
     *
     * @param localReActorSystem ReActorSystem used to manage and control the data flow
     * @param feedName           Name of this feed. Feed name must be unique and if deterministic it allows
     *                           cold replay
     */
    public ReactedSubmissionPublisher(ReActorSystem localReActorSystem, String feedName) {
        this.localReActorSystem = Objects.requireNonNull(localReActorSystem);
        this.subscribers = ConcurrentHashMap.newKeySet(10);
        var feedGateCfg = ReActorConfig.newBuilder()
                                       .setReActorName(ReactedSubmissionPublisher.class.getSimpleName() + "-" +
                                                       Objects.requireNonNull(feedName))
                                       .setMailBoxProvider(BasicMbox::new)
                                       .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                                       .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                       .build();
        this.feedGate = localReActorSystem.spawnReActor(ReActions.newBuilder()
                                                                 .reAct(ReActorInit.class, ReActions::noReAction)
                                                                 .reAct(PublisherShutdown.class,
                                                                        ReactedSubmissionPublisher::onPublisherShutdown)
                                                                 .reAct(ReActorStop.class, this::onStop)
                                                                 .reAct(SubscriptionRequest.class,
                                                                        this::onSubscriptionRequest)
                                                                 .reAct(UnsubscriptionRequest.class,
                                                                        this::onUnSubscriptionRequest)
                                                                 .build(), feedGateCfg)
                                          .orElseThrow(IllegalArgumentException::new);
    }

    public ReactedSubmissionPublisher() {
        /* Required by Externalizable */
        this.subscribers = Set.of();
        this.feedGate = ReActorRef.NO_REACTOR_REF;
        this.localReActorSystem = null; //TODO null reactor system object
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.feedGate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorRef feedGate = new ReActorRef();
        feedGate.readExternal(in);
        this.setFeedGate(feedGate)
            .setLocalReActorSystem(RemotingDriver.getDriverCtx()
                                                 .map(DriverCtx::getLocalReActorSystem)
                                                 .orElseThrow());
    }

    @Override
    public void close() { this.feedGate.tell(feedGate, new PublisherShutdown()); }

    /**
     + Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     */
    @Override
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber) {
        subscribe(subscriber, UUID.randomUUID().toString());
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param subscriberName This name must be unique and if deterministic it allows cold replay
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, String subscriberName) {
        subscribe(subscriber, ForkJoinPool.commonPool(), Flow.defaultBufferSize(),
                  BackpressuringMbox.BEST_EFFORT_TIMEOUT, subscriberName);
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber           Java Flow compliant subscriber
     * @param asyncRetrierExecutor Executor where flow actions will be run on
     * @param bufferSize           How many elements can be buffered in the best effort subscriber
     * @param subscriberName       This name must be unique and if deterministic it allows cold replay
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, Executor asyncRetrierExecutor,
                          int bufferSize, String subscriberName) {
        subscribe(subscriber, asyncRetrierExecutor, bufferSize, BackpressuringMbox.BEST_EFFORT_TIMEOUT,
                  subscriberName);
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param bufferSize How many elements can be buffered in the best
     *                   effort subscriber
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize) {
        subscribe(subscriber, ForkJoinPool.commonPool(), bufferSize, BackpressuringMbox.BEST_EFFORT_TIMEOUT,
                  UUID.randomUUID().toString());
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param bufferSize     How many elements can be buffered in the best
     *                       effort subscriber
     * @param subscriberName This name must be unique and if deterministic
     *                       it allows cold replay
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize, String subscriberName) {
        subscribe(subscriber, ForkJoinPool.commonPool(), bufferSize, BackpressuringMbox.BEST_EFFORT_TIMEOUT,
                  subscriberName);
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an errpr
     * @throws IllegalArgumentException if duration is not bigger than zero
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, Duration backpressureErrorTimeout) {
        subscribe(subscriber, ForkJoinPool.commonPool(), Flow.defaultBufferSize(),
                  ConfigUtils.requiredCondition(backpressureErrorTimeout,
                                                Predicate.not(Duration::isZero),
                                                IllegalArgumentException::new), UUID.randomUUID().toString());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an errpr
     * @param subscriberName This name must be unique and if deterministic it allows cold replay
     * @throws IllegalArgumentException if duration is not bigger than zero
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, Duration backpressureErrorTimeout,
                          String subscriberName) {
        subscribe(subscriber, ForkJoinPool.commonPool(), Flow.defaultBufferSize(),
                  ConfigUtils.requiredCondition(backpressureErrorTimeout,
                                                Predicate.not(Duration::isZero),
                                                IllegalArgumentException::new), subscriberName);
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param bufferSize How many elements can be buffered in the best
     *                   effort subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an errpr
     * @throws IllegalArgumentException if duration is not bigger than zero
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize,
                          Duration backpressureErrorTimeout) {
        subscribe(subscriber, ForkJoinPool.commonPool(), bufferSize,
                  ConfigUtils.requiredCondition(backpressureErrorTimeout,
                                                Predicate.not(Duration::isZero),
                                                IllegalArgumentException::new), UUID.randomUUID().toString());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param bufferSize     How many elements can be buffered in the best
     *                       effort subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an errpr
     * @param subscriberName This name must be unique and if deterministic
     *                       it allows cold replay
     * @throws IllegalArgumentException if duration is not bigger than zero
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize,
                          Duration backpressureErrorTimeout, String subscriberName) {
        subscribe(subscriber, ForkJoinPool.commonPool(), bufferSize,
                  ConfigUtils.requiredCondition(backpressureErrorTimeout,
                                                Predicate.not(Duration::isZero),
                                                IllegalArgumentException::new), subscriberName);
    }

    /**
     * Register a generic subscriber to the stream.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber           Java Flow compliant subscriber
     * @param asyncRetrierExecutor Executor where flow actions will be run on
     * @param bufferSize           Consumer buffer. Updates messages exceeding this buffer size will cause
     *                             a drop or a wait signal for the producer, according to the subscription type
     * @param backpressureTimeout  For how long the subscription should attempt to deliver an update
     *                             to the subscriber. 
     * @param subscriberName       This name must be unique and if deterministic it allows cold replay
     */
    public void subscribe(Flow.Subscriber<? super PayloadT> subscriber, Executor asyncRetrierExecutor,
                          int bufferSize, Duration backpressureTimeout, String subscriberName) {

        var backpressureManager = new BackpressureManager<>(Objects.requireNonNull(subscriber), this.feedGate,
                                                            Objects.requireNonNull(asyncRetrierExecutor),
                                                            ConfigUtils.requiredInRange(bufferSize, 1,
                                                                                        Integer.MAX_VALUE,
                                                                                        IllegalArgumentException::new),
                                                            Objects.requireNonNull(backpressureTimeout));

        var subscriberCfg = ReActorConfig.newBuilder()
                                         .setReActorName(this.feedGate.getReActorId().getReActorName() +
                                                         "_subscriber_" + subscriberName + "_" +
                                                         this.feedGate.getReActorId().getReActorUuid().toString())
                                         .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                         .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                                         .setMailBoxProvider(backpressureManager.getManagerMailbox())
                                         .build();

        this.localReActorSystem.spawnChild(backpressureManager.getReActions(), localReActorSystem.getUserReActorsRoot(),
                                           subscriberCfg)
                               .orElseSneakyThrow();
    }

    /**
     * Submit a new message to the feed. The delivery order among the subscribers is not predictable
     * or consistent. If producer wants to regulate the production rate according to the consumers
     * speed (flow regulation backpressuring) the next message has to be sent when the previous one
     * has been delivered. Strict message ordering is guaranteed to be the same of submission.
     * This call is never blocking.
     *
     * @param message the message that should be propagated to the subscribers
     * @return a CompletionsStage that will be marked ad complete when the message has been
     * delivered to all the subscribers
     */
    public CompletionStage<Void> backpressurableSubmit(PayloadT message) {
        var deliveries = this.subscribers.stream()
                                         .map(subscribed -> subscribed.aTell(subscribed, message))
                                         .collect(Collectors.toUnmodifiableList());
        return deliveries.stream()
                         .reduce((first, second) -> first.thenCompose(delivery -> second))
                         .map(lastDelivery -> lastDelivery.thenAccept(lastRetVal -> {}))
                         .orElse(CompletableFuture.completedFuture(null));
    }

    public void submit(PayloadT message) {
        this.subscribers.forEach(subscribed -> subscribed.aTell(subscribed, message));
    }

    private static void onPublisherShutdown(ReActorContext raCtx, PublisherShutdown shutdownRequest) {
        raCtx.stop();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        this.subscribers.forEach(subscriber -> subscriber.tell(raCtx.getSelf(), new SubscriberComplete()));
        this.subscribers.clear();
    }

    private void onSubscriptionRequest(ReActorContext raCtx, SubscriptionRequest subscription) {
        subscription.getSubscriptionBackpressuringManager()
                    .aTell(raCtx.getSelf(),
                           new SubscriptionReply(this.subscribers.add(subscription.getSubscriptionBackpressuringManager())))
                    .thenAccept(delivery -> delivery.filter(DeliveryStatus::isDelivered, IllegalStateException::new)
                                                    .ifError(error -> raCtx.getReActorSystem()
                                                                           .logError("Unable to deliver subscription confirmation to %s",
                                                                                     error,
                                                                                     subscription.getSubscriptionBackpressuringManager())));
    }

    private void onUnSubscriptionRequest(ReActorContext raCtx, UnsubscriptionRequest unsubscriptionRequest) {
        this.subscribers.remove(unsubscriptionRequest.getSubscriptionBackpressuringManager());
    }

    private ReactedSubmissionPublisher<PayloadT> setFeedGate(ReActorRef feedGate) {
        return SerializationUtils.setObjectField(this, ReactedSubmissionPublisher.FEED_GATE_OFFSET, feedGate);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ReactedSubmissionPublisher<PayloadT> setLocalReActorSystem(ReActorSystem localReActorSystem) {
        return SerializationUtils.setObjectField(this, ReactedSubmissionPublisher.LOCAL_REACTOR_SYSTEM, localReActorSystem);
    }
}
