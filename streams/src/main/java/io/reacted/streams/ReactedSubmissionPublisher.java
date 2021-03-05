/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.streams.messages.PublisherInterrupt;
import io.reacted.streams.messages.PublisherShutdown;
import io.reacted.streams.messages.PublisherComplete;
import io.reacted.streams.messages.SubscriptionReply;
import io.reacted.streams.messages.SubscriptionRequest;
import io.reacted.streams.messages.UnsubscriptionRequest;

import javax.annotation.Nullable;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

@NonNullByDefault
public class ReactedSubmissionPublisher<PayloadT extends Serializable> implements Flow.Publisher<PayloadT>,
                                                                                  AutoCloseable, Externalizable {
    public static final Duration RELIABLE_SUBSCRIPTION = BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT;
    public static final Duration BEST_EFFORT_SUBSCRIPTION = BackpressuringMbox.BEST_EFFORT_TIMEOUT;
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
        this(localReActorSystem, feedName, Flow.defaultBufferSize());
    }

    /**
     * Creates a location oblivious data publisher with backpressure. Subscribers can slowdown
     * the producer or just drop data if they are best effort subscribers. Publisher is Serializable,
     * so it can be sent to any reactor over any gate and subscribers can simply join the stream.
     *
     * @param localReActorSystem ReActorSystem used to manage and control the data flow
     * @param feedName           Name of this feed. Feed name must be unique and if deterministic it allows
     *                           cold replay
     * @param bufferSize         Size of the buffer that holds the messages waiting to be sent
     */
    public ReactedSubmissionPublisher(ReActorSystem localReActorSystem, String feedName,
                                      int bufferSize) {
        this.localReActorSystem = Objects.requireNonNull(localReActorSystem);
        this.subscribers = ConcurrentHashMap.newKeySet(10);
        var feedGateConfig = ReActorConfig.newBuilder()
                                       .setReActorName(ReactedSubmissionPublisher.class.getSimpleName() + "-" +
                                                       Objects.requireNonNull(feedName))
                                       .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                    .setRealMailboxOwner(ctx)
                                                                                    .setBufferSize(bufferSize)
                                                                                    .setRequestOnStartup(bufferSize)
                                                                                    .setBackpressureTimeout(BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT)
                                                                                    .setNonDelayable(Set.of(ReActorInit.class, PublisherShutdown.class,
                                                                                                            PublisherInterrupt.class, ReActorStop.class,
                                                                                                            SubscriptionRequest.class, UnsubscriptionRequest.class))
                                                                                    .build())
                                       .build();
        this.feedGate = localReActorSystem.spawn(ReActions.newBuilder()
                                                          .reAct(ReActorInit.class, ReActions::noReAction)
                                                          .reAct(PublisherShutdown.class,
                                                                 (raCtx, shutdown) -> raCtx.stop())
                                                          .reAct(PublisherInterrupt.class,
                                                                 this::onInterrupt)
                                                          .reAct(ReActorStop.class, this::onStop)
                                                          .reAct(SubscriptionRequest.class,
                                                                 this::onSubscriptionRequest)
                                                          .reAct(UnsubscriptionRequest.class,
                                                                 this::onUnSubscriptionRequest)
                                                          .reAct(this::forwardToSubscribers)
                                                          .build(), feedGateConfig)
                                          .orElseThrow(IllegalArgumentException::new);
    }

    public ReactedSubmissionPublisher() {
        /* Required by Externalizable */
        this.subscribers = Set.of();
        this.feedGate = ReActorRef.NO_REACTOR_REF;
        //noinspection ConstantConditions
        this.localReActorSystem = null; //TODO null reactor system object
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(feedGate);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorRef feedGate = new ReActorRef();
        feedGate.readExternal(in);
        setFeedGate(feedGate).setLocalReActorSystem(RemotingDriver.getDriverCtx()
                                                                  .map(DriverCtx::getLocalReActorSystem)
                                                                  .orElseThrow());
    }

    /**
     *  Stop the publisher. All the subscribers will be able to consume all the messages already sent
     */
    @Override
    public void close() { feedGate.tell(feedGate, new PublisherShutdown()); }

    /**
     *  Stop the publisher. All the subscribers will be notified immediately of the termination
     */
    public void interrupt() { feedGate.tell(feedGate, new PublisherInterrupt()); }

    /**
     + Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     *
     * @param subscriber Java Flow compliant subscriber
     * @throws NullPointerException if subscriber is null
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
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, String subscriberName) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(Flow.defaultBufferSize())
                          .setBackpressureTimeout(BEST_EFFORT_SUBSCRIPTION)
                          .setSubscriberName(subscriberName)
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param bufferSize How many elements can be buffered in the best effort subscriber. <b>Positive</b> values only
     * @throws IllegalArgumentException if {@code bufferSize} is not positive
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(bufferSize)
                          .setBackpressureTimeout(BEST_EFFORT_SUBSCRIPTION)
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSubscriberName(UUID.randomUUID().toString())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     * Registers a best effort subscriber. All the updates sent to this subscriber that cannot be
     * processed will be lost. This subscriber consumption speed will not affect the producer,
     * but delivery speed to the subscriber could.
     * For the non lost updates, strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param bufferSize     How many elements can be buffered in the best effort subscriber. <b>Positive</b> values
     *                       only
     * @param subscriberName This name must be unique and if deterministic
     *                       it allows cold replay
     * @throws IllegalArgumentException if {@code bufferSize} is not positive
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize,
                                           String subscriberName) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(bufferSize)
                          .setBackpressureTimeout(BEST_EFFORT_SUBSCRIPTION)
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSubscriberName(subscriberName)
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an error
     * @param asyncBackpressurer the executor to use for async delivery, supporting creation of at least one independent
     *                           thread
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, Executor asyncBackpressurer,
                                           Duration backpressureErrorTimeout) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(Flow.defaultBufferSize())
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(asyncBackpressurer)
                          .setSubscriberName(UUID.randomUUID().toString())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an error
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber,
                                           Duration backpressureErrorTimeout) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(Flow.defaultBufferSize())
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSubscriberName(UUID.randomUUID().toString())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an error
     * @param subscriberName This name must be unique and if deterministic it allows cold replay
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber,
                                           Duration backpressureErrorTimeout, String subscriberName) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(Flow.defaultBufferSize())
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSubscriberName(subscriberName)
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber     Java Flow compliant subscriber
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an error
     * @param asyncBackpressurer the executor to use for async delivery, supporting creation of at least one independent
     *                           thread
     * @param subscriberName This name must be unique and if deterministic it allows cold replay
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber,
                                           Duration backpressureErrorTimeout, Executor asyncBackpressurer,
                                           String subscriberName) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(Flow.defaultBufferSize())
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(asyncBackpressurer)
                          .setSubscriberName(subscriberName)
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
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
     *                                 before signaling an error
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws IllegalArgumentException if {@code bufferSize} is not positive
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize,
                                           Duration backpressureErrorTimeout) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(bufferSize)
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(ForkJoinPool.commonPool())
                          .setSubscriberName(UUID.randomUUID().toString())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     + Registers a producer slowdown subscriber. Submissions towards this subscriber will be marked as completed only
     * when the message has been actually delivered or on error, allowing the producer to slow down the production rate.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriber Java {@link Flow} compliant subscriber
     * @param bufferSize How many elements can be buffered in the best effort subscriber. <b>Positive</b> values only
     * @param asyncBackpressurer the executor to use for async delivery, supporting creation of at least one independent
     *                           thread
     * @param backpressureErrorTimeout the subscriber will try to deliver the message for at max this amount of time
     *                                 before signaling an error
     * @throws IllegalArgumentException if duration is not bigger than zero
     * @throws IllegalArgumentException if {@code bufferSize} is not positive
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     * NOTE: this overload generates NON REPLAYABLE subscriptions
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, int bufferSize,
                                           Executor asyncBackpressurer, Duration backpressureErrorTimeout) {
        return subscribe(ReActedSubscription.<PayloadT>newBuilder()
                          .setSubscriber(subscriber)
                          .setBufferSize(bufferSize)
                          .setBackpressureTimeout(ObjectUtils.requiredCondition(Objects.requireNonNull(backpressureErrorTimeout),
                                                                                Predicate.not(Duration::isZero),
                                                                                IllegalArgumentException::new))
                          .setAsyncBackpressurer(asyncBackpressurer)
                          .setSubscriberName(UUID.randomUUID().toString())
                          .setSequencer(ReActedSubscription.NO_CUSTOM_SEQUENCER)
                          .build());
    }

    /**
     * Register a generic subscriber to the stream.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscription A {@link ReActedSubscription}
     * SneakyThrows any exception raised
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(ReActedSubscription<PayloadT> subscription) {
        CompletionStage<Void> subscriptionComplete = new CompletableFuture<>();
        var backpressureManager = new BackpressureManager<>(subscription, feedGate, subscriptionComplete);

        var subscriberConfig = ReActorConfig.newBuilder()
                                         .setReActorName(feedGate.getReActorId().getReActorName() +
                                                         "_subscriber_" + subscription.getSubscriberName() + "_" +
                                                         feedGate.getReActorId().getReActorUUID().toString())
                                         .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                         .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                         .setMailBoxProvider(backpressureManager.getManagerMailbox())
                                         .build();

        localReActorSystem.spawnChild(backpressureManager.getReActions(), localReActorSystem.getUserReActorsRoot(),
                                      subscriberConfig)
                          .orElseSneakyThrow();
        return subscriptionComplete;
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
        return feedGate.atell(message).thenAccept(delivery -> {});
    }

    public void submit(PayloadT message) {
        feedGate.tell(message)
                .toCompletableFuture()
                .thenAccept(delivery -> delivery.ifError(Throwable::printStackTrace));
    }

    private void forwardToSubscribers(ReActorContext raCtx, Serializable payload) {
        var deliveries = subscribers.stream()
                                    .map(subscribed -> subscribed.atell(subscribed, payload))
                                    .collect(Collectors.toUnmodifiableList());
        deliveries.stream()
                  .reduce((first, second) -> first.thenCompose(delivery -> second))
                  .ifPresent(lastDelivery -> lastDelivery.thenAccept(lastRetVal -> raCtx.getMbox().request(1)));
    }

    private void onInterrupt(ReActorContext raCtx, PublisherInterrupt interrupt) {
        subscribers.forEach(subscriber -> subscriber.tell(raCtx.getSelf(), interrupt));
        subscribers.clear();
        raCtx.stop();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        subscribers.forEach(subscriber -> subscriber.tell(raCtx.getSelf(), new PublisherComplete()));
        subscribers.clear();
    }

    private void onSubscriptionRequest(ReActorContext raCtx, SubscriptionRequest subscription) {
        var backpressuringManager = subscription.getSubscriptionBackpressuringManager();
        ifNotDelivered(backpressuringManager.atell(raCtx.getSelf(),
                                                   new SubscriptionReply(subscribers.add(backpressuringManager))),
                    error -> raCtx.logError("Unable to deliver subscription confirmation to {}",
                                            subscription.getSubscriptionBackpressuringManager(), error));
    }

    private void onUnSubscriptionRequest(ReActorContext raCtx, UnsubscriptionRequest unsubscriptionRequest) {
        subscribers.remove(unsubscriptionRequest.getSubscriptionBackpressuringManager());
    }

    private ReactedSubmissionPublisher<PayloadT> setFeedGate(ReActorRef feedGate) {
        return SerializationUtils.setObjectField(this, ReactedSubmissionPublisher.FEED_GATE_OFFSET, feedGate);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ReactedSubmissionPublisher<PayloadT> setLocalReActorSystem(ReActorSystem localReActorSystem) {
        return SerializationUtils.setObjectField(this, ReactedSubmissionPublisher.LOCAL_REACTOR_SYSTEM,
                                                 localReActorSystem);
    }

    public final static class ReActedSubscription<PayloadT> {
        @Nullable
        public static final ThreadPoolExecutor NO_CUSTOM_SEQUENCER = null;
        private final Flow.Subscriber<? super PayloadT> subscriber;
        private final int bufferSize;
        private final Duration backpressureTimeout;
        private final Executor asyncBackpressurer;
        private final String subscriberName;
        @Nullable
        private final ThreadPoolExecutor sequencer;

        private ReActedSubscription(Builder<PayloadT> builder) {
            this.subscriber = Objects.requireNonNull(builder.subscriber);
            this.bufferSize = ObjectUtils.requiredInRange(builder.bufferSize, 1, Integer.MAX_VALUE,
                                                          IllegalArgumentException::new);
            this.backpressureTimeout = ObjectUtils.requiredCondition(Objects.requireNonNull(builder.backpressureTimeout),
                                                                     timeout -> timeout.compareTo(RELIABLE_SUBSCRIPTION) <= 0 &&
                                                                                !timeout.isNegative(),
                                                                     IllegalArgumentException::new);
            this.asyncBackpressurer = Objects.requireNonNull(builder.asyncBackpressurer);
            this.subscriberName = Objects.requireNonNull(builder.subscriberName);
            this.sequencer = builder.sequencer != null
                             ? ObjectUtils.requiredCondition(builder.sequencer,
                                                             sequencer -> sequencer.getMaximumPoolSize() == 1,
                                                             IllegalArgumentException::new)
                             : null;
        }

        public Flow.Subscriber<? super PayloadT> getSubscriber() { return subscriber; }

        public int getBufferSize() { return bufferSize; }

        public Duration getBackpressureTimeout() { return backpressureTimeout; }

        public Executor getAsyncBackpressurer() { return asyncBackpressurer; }

        public String getSubscriberName() { return subscriberName; }

        @Nullable
        public ThreadPoolExecutor getSequencer() { return sequencer; }

        public static <PayloadT> Builder<PayloadT> newBuilder() { return new Builder<>(); }

        @SuppressWarnings("NotNullFieldNotInitialized")
        public static final class Builder<PayloadT> {
            private Flow.Subscriber<? super PayloadT> subscriber;
            private int bufferSize;
            private Duration backpressureTimeout;
            private Executor asyncBackpressurer;
            private String subscriberName;
            @Nullable
            private ThreadPoolExecutor sequencer = NO_CUSTOM_SEQUENCER;

            private Builder() { }

            /**
             *
             * @param subscriber Java {@link Flow} compliant subscriber
             * @return this builder
             */
            public Builder<PayloadT> setSubscriber(Flow.Subscriber<? super PayloadT> subscriber) {
                this.subscriber = subscriber;
                return this;
            }

            /**
             *
             * @param bufferSize Producer buffer size. Updates messages exceeding this buffer size will cause
             *                   a drop or a wait signal for the producer, according to the subscription type.
             *                   <b>Positive</b> values only
             * @return this builder
             */
            public Builder<PayloadT> setBufferSize(int bufferSize) {
                this.bufferSize = bufferSize;
                return this;
            }

            /**
             *
             * @param backpressureTimeout At most this timeout will be waited while attempting a delivery. Once this
             *                            timeout is expired, the message is dropped.
             *                            {@link ReactedSubmissionPublisher#BEST_EFFORT_SUBSCRIPTION} for best effort subscriptions.
             *                            If the submission buffer is full, the new messages will be discarder
             *                            {@link ReactedSubmissionPublisher#RELIABLE_SUBSCRIPTION} for subscriptions where
             *                            no message can be lost. Publisher will wait indefinitely.
             * @return this builder
             */
            public Builder<PayloadT> setBackpressureTimeout(Duration backpressureTimeout) {
                this.backpressureTimeout = backpressureTimeout;
                return this;
            }

            /**
             *
             * @param asyncBackpressurer the executor to use for async delivery, supporting creation of at least one
             *                           independent thread
             * @return this builder
             */
            public Builder<PayloadT> setAsyncBackpressurer(Executor asyncBackpressurer) {
                this.asyncBackpressurer = asyncBackpressurer;
                return this;
            }

            /**
             *
             * @param subscriberName This name must be unique and if deterministic it allows <b>replay</b>
             * @return this builder
             */
            public Builder<PayloadT> setSubscriberName(String subscriberName) {
                this.subscriberName = subscriberName;
                return this;
            }

            /**
             *
             * @param sequencer An optional *single* thread for asynchronously attempting the submission tasks. If not specified
             *                  a new thread will be automatically created for this
             * @return this builder
             */
            public Builder<PayloadT> setSequencer(@Nullable ThreadPoolExecutor sequencer) {
                this.sequencer = sequencer;
                return this;
            }

            /**
             *
             * @return a {@link ReActedSubscription}
             * @throws NullPointerException if any of the parameters is null
             * @throws IllegalArgumentException if any of the supplied values does not comply to the specified
             *                                  requirements
             */
            public ReActedSubscription<PayloadT> build() {
                return new ReActedSubscription<>(this);
            }
        }
    }
}
