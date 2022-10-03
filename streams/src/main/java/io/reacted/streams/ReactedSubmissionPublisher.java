/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.streams.messages.PublisherInterrupt;
import io.reacted.streams.messages.PublisherShutdown;
import io.reacted.streams.messages.PublisherComplete;
import io.reacted.streams.messages.SubscriptionReply;
import io.reacted.streams.messages.SubscriptionRequest;
import io.reacted.streams.messages.UnsubscriptionRequest;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.Flow.Subscriber;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

@NonNullByDefault
public class ReactedSubmissionPublisher<PayloadT extends Serializable> implements Flow.Publisher<PayloadT>,
                                                                                  AutoCloseable, Externalizable {
    private static final Duration BACKPRESSURE_DELAY_BASE = Duration.ofMillis(10);
    private static final String SUBSCRIPTION_NAME_FORMAT = "Backpressure Manager [%s] Subscription [%s]";
    private static final long FEED_GATE_OFFSET = SerializationUtils.getFieldOffset(ReactedSubmissionPublisher.class,
                                                                                   "feedGate")
                                                                   .orElseSneakyThrow();
    private static final long LOCAL_REACTOR_SYSTEM =
            SerializationUtils.getFieldOffset(ReactedSubmissionPublisher.class, "localReActorSystem")
                              .orElseSneakyThrow();
    private final transient Set<ReActorRef> subscribers;
    private final transient ReActorSystem localReActorSystem;
    private final ReActorRef feedGate;
    private Duration streamBackpressureTimeout = BACKPRESSURE_DELAY_BASE;

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
        var feedGateConfig = ReActorConfig.newBuilder()
                                       .setReActorName("Feed Gate: [" + Objects.requireNonNull(feedName, "Feed name cannot be null") + "]")
                                       .setMailBoxProvider(ctx -> BackpressuringMbox.newBuilder()
                                                                                    .setRealMailboxOwner(ctx)
                                                                                    .setBackpressuringThreshold(1000)
                                                                                    .setOutOfStreamControl(PublisherComplete.class,
                                                                                                           PublisherShutdown.class)
                                                                                    .setNonDelayable(ReActorInit.class,
                                                                                                     PublisherInterrupt.class, ReActorStop.class,
                                                                                                     SubscriptionRequest.class, UnsubscriptionRequest.class)
                                                                                    .build())
                                       .build();
        this.feedGate = localReActorSystem.spawn(ReActions.newBuilder()
                                                          .reAct(ReActorInit.class, ReActions::noReAction)
                                                          .reAct(PublisherShutdown.class, (raCtx, shutdown) -> raCtx.stop())
                                                          .reAct(PublisherInterrupt.class, this::onInterrupt)
                                                          .reAct(ReActorStop.class, this::onStop)
                                                          .reAct(SubscriptionRequest.class, this::onSubscriptionRequest)
                                                          .reAct(UnsubscriptionRequest.class, this::onUnSubscriptionRequest)
                                                          .reAct(this::forwardToSubscribers)
                                                          .build(),
                                                 feedGateConfig)
                                          .orElseThrow(IllegalArgumentException::new);
    }

    public ReactedSubmissionPublisher() {
        /* Required by Externalizable */
        this.subscribers = Set.of();
        this.feedGate = ReActorRef.NO_REACTOR_REF;
        this.localReActorSystem = ReActorSystem.NO_REACTOR_SYSTEM;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        feedGate.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorRef gate = new ReActorRef();
        gate.readExternal(in);
        DriverCtx driverCtx = ReActorSystemDriver.getDriverCtx();
        if (driverCtx == null) {
            throw new IllegalStateException("No Driver Context For Driver");
        }
        setFeedGate(gate).setLocalReActorSystem(driverCtx.getLocalReActorSystem());
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
     * @param subscriptionName This name must be unique and if deterministic it allows cold replay
     * @throws NullPointerException if any of the arguments is null
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(Flow.Subscriber<? super PayloadT> subscriber, String subscriptionName) {
        return subscribe(subscriber, Flow.defaultBufferSize(), subscriptionName);
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
        return subscribe(ReActedSubscriptionConfig.<PayloadT>newBuilder()
                                                  .setBufferSize(bufferSize)
                                                  .setSubscriberName(subscriberName)
                                                  .build(), subscriber);
    }

    /**
     * Register a generic subscriber to the stream.
     * Strict message ordering is guaranteed to be the same of submission
     *
     * @param subscriptionConfig A {@link ReActedSubscriptionConfig}
     * SneakyThrows any exception raised
     * @return A {@link CompletionStage} that is going to be complete when the subscription is complete
     */
    public CompletionStage<Void> subscribe(ReActedSubscriptionConfig<PayloadT> subscriptionConfig,
                                           Subscriber<? super PayloadT> subscriber) {
        CompletionStage<Void> subscriptionComplete = new CompletableFuture<>();
        var backpressureManager = new BackpressureManager<>(Objects.requireNonNull(subscriptionConfig,
                                                                                   "Subscription config cannot be null"),
                                                            Objects.requireNonNull(subscriber, "Subscriber cannot be null"),
                                                            feedGate, subscriptionComplete);

        var subscriberConfig = ReActorConfig.newBuilder()
                                            .setReActorName(String.format(SUBSCRIPTION_NAME_FORMAT,
                                                                          feedGate.getReActorId().getReActorName(),
                                                                          subscriptionConfig.getSubscriberName()))
                                            .setDispatcherName(Dispatcher.DEFAULT_DISPATCHER_NAME)
                                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                            .setMailBoxProvider(backpressureManager.getManagerMailbox())
                                            .build();

        localReActorSystem.spawnChild(backpressureManager.getReActions(),
                                      localReActorSystem.getUserReActorsRoot(),
                                      subscriberConfig)
                          .orElseSneakyThrow();
        return subscriptionComplete;
    }

    /**
     * Submit a new message to the feed. The delivery order among the subscribers is not predictable
     * or consistent. If producer wants to regulate the production rate according to the consumers
     * speed (flow regulation backpressuring) the next message has to be sent when the previous one
     * is not requesting for backpressure. Strict message ordering is guaranteed to be the same of submission.
     * This call is never blocking.
     *
     * @param message the message that should be propagated to the subscribers
     * @return a CompletionsStage that will be marked ad complete when the message has been
     * delivered to all the subscribers
     */

    public DeliveryStatus submit(PayloadT message) {
        return feedGate.tell(message);
    }

    private void forwardToSubscribers(ReActorContext raCtx, Serializable payload) {
        if (subscribers.isEmpty()) {
            return;
        }

        CompletionStage<DeliveryStatus>[] deliveries = new CompletionStage[subscribers.size()];
        ReActorRef[] subscribersRefs = new ReActorRef[subscribers.size()];
        Iterator<ReActorRef> subscribersIterator = subscribers.iterator();
        for (int subscriberIdx = 0; subscribersIterator.hasNext(); subscriberIdx++) {
            subscribersRefs[subscriberIdx] = subscribersIterator.next();
            deliveries[subscriberIdx] = subscribersRefs[subscriberIdx].atell(raCtx.getSelf(), payload);
        }
        CompletionStage<DeliveryStatus> result = deliveries[0];
        for(int subscriberIdx = 1; subscriberIdx < deliveries.length; subscriberIdx++) {
            result = combineStages(raCtx.getSelf(),
                                   result, subscribersRefs[subscriberIdx - 1],
                                   deliveries[subscriberIdx], subscribersRefs[subscriberIdx]);
        }
        result.handle((deliveryStatus, error) -> {
            if (deliveryStatus != DeliveryStatus.BACKPRESSURE_REQUIRED) {
                raCtx.getMbox().request(1);
                this.streamBackpressureTimeout = BACKPRESSURE_DELAY_BASE;
            } else {
                raCtx.getReActorSystem().getSystemSchedulingService()
                     .schedule(() -> raCtx.getMbox().request(1),
                               streamBackpressureTimeout.toMillis(),
                               TimeUnit.MILLISECONDS);
                streamBackpressureTimeout = streamBackpressureTimeout.multipliedBy(2);
            }
            return null;
        });
    }

    private CompletionStage<DeliveryStatus> combineStages(ReActorRef feedGate,
                                                          CompletionStage<DeliveryStatus> first,
                                                          ReActorRef firstRef,
                                                          CompletionStage<DeliveryStatus> second,
                                                          ReActorRef secondRef) {
        return first.handle((dStatus, error) -> {
            if (error != null) {
                feedGate.tell(feedGate, new UnsubscriptionRequest(firstRef));
                return DeliveryStatus.NOT_DELIVERED;
            }
            return dStatus;
        }).thenCompose(fStatus -> second.handle((sStatus, error) -> {
            if (error != null) {
                feedGate.tell(feedGate, new UnsubscriptionRequest(secondRef));
                sStatus = DeliveryStatus.NOT_DELIVERED;
            }
            return fStatus == DeliveryStatus.BACKPRESSURE_REQUIRED
                   ? fStatus
                   : sStatus;
        }));
    }

    private void onInterrupt(ReActorContext raCtx, PublisherInterrupt interrupt) {
        subscribers.forEach(subscriber -> subscriber.tell(raCtx.getSelf(), interrupt));
        subscribers.clear();
        raCtx.stop();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        subscribers.forEach(subscriber -> {
                       if(subscriber.route(raCtx.getSelf(), new PublisherComplete()).isNotSent()) {
                           raCtx.logError("Unable to stop subscriber {}", subscriber);
                       }
                   });
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

    public static final class ReActedSubscriptionConfig<PayloadT> {
        private final int bufferSize;
        private final String subscriberName;

        private ReActedSubscriptionConfig(Builder<PayloadT> builder) {
            this.bufferSize = ObjectUtils.requiredInRange(builder.bufferSize, 1, Integer.MAX_VALUE,
                                                          () -> new IllegalArgumentException("Invalid subscription buffer size"));

            this.subscriberName = Objects.requireNonNull(builder.subscriberName,
                                                         "Subscriber name cannot be null");
        }

        public int getBufferSize() { return bufferSize; }

        public String getSubscriberName() { return subscriberName; }

        public static <PayloadT> Builder<PayloadT> newBuilder() { return new Builder<>(); }

        @SuppressWarnings("NotNullFieldNotInitialized")
        public static class Builder<PayloadT> {
            private int bufferSize = Flow.defaultBufferSize();
            private String subscriberName;
            private Builder() { }

            /**
             *
             * @param bufferSize Producer buffer size. Updates messages exceeding this buffer size will cause
             *                   a drop or a wait signal for the producer, according to the subscription type.
             *                   <b>Positive</b> values only
             * @return this builder
             */
            public final Builder<PayloadT> setBufferSize(int bufferSize) {
                this.bufferSize = bufferSize;
                return this;
            }
            /**
             *
             * @param subscriberName This name must be unique and if deterministic it allows <b>replay</b>
             * @return this builder
             */
            public final Builder<PayloadT> setSubscriberName(String subscriberName) {
                this.subscriberName = subscriberName;
                return this;
            }

            /**
             *
             * @return a {@link ReActedSubscriptionConfig}
             * @throws NullPointerException if any of the parameters is null
             * @throws IllegalArgumentException if any of the supplied values does not comply to the specified
             *                                  requirements
             */
            public ReActedSubscriptionConfig<PayloadT> build() {
                return new ReActedSubscriptionConfig<>(this);
            }
        }
    }
}
