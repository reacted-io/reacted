/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.config.ConfigUtils;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

@NonNullByDefault
public class BackpressuringMbox implements MailBox {
    public static final Duration BEST_EFFORT_TIMEOUT = Duration.ZERO;
    public static final Duration RELIABLE_DELIVERY_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);
    private static final Logger LOGGER = LoggerFactory.getLogger(BackpressuringMbox.class);
    private final Duration backpressureTimeout;
    private final MailBox realMbox;
    private final SubmissionPublisher<DeliveryRequest> backpressurer;
    private final BackpressuringSubscriber reliableBackpressuringSubscriber;
    private final Set<Class<? extends Serializable>> notDelayed;
    private final Set<Class<? extends Serializable>> notBackpressurable;
    private final ExecutorService sequencer;
    private final boolean isPrivateSequencer;

    /*
     * BackpressuringMbox wrapper for any other mailbox type.
     *
     */
    private BackpressuringMbox(Builder builder) {
        ReActorContext mboxOwner = Objects.requireNonNull(builder.realMailboxOwner);
        this.backpressureTimeout = ConfigUtils.requiredCondition(Objects.requireNonNull(builder.backpressureTimeout),
                                                                 timeout -> timeout.compareTo(RELIABLE_DELIVERY_TIMEOUT) <= 0,
                                                                 () -> new IllegalArgumentException("Invalid backpressure timeout"));
        this.realMbox = Objects.requireNonNull(builder.realMbox);
        this.notDelayed = Objects.requireNonNull(builder.notDelayable);
        this.notBackpressurable = Objects.requireNonNull(builder.notBackpressurable);
        int bufferSize = ConfigUtils.requiredInRange(builder.bufferSize, 1, Integer.MAX_VALUE,
                                                     IllegalArgumentException::new);
        int requestOnStartup = ConfigUtils.requiredInRange(builder.requestOnStartup, 0, Integer.MAX_VALUE,
                                                           IllegalArgumentException::new);
        var deliveryThreadFactory = new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, throwable) -> LOGGER.error("Uncaught exception in {} delivery thread",
                                                                                 BackpressuringMbox.class.getSimpleName(),
                                                                                 throwable))
                .build();

        if (builder.sequencer == null) {
            this.isPrivateSequencer = true;
            this.sequencer =  Executors.newSingleThreadExecutor(deliveryThreadFactory);
        } else {
            ConfigUtils.requiredInRange(builder.sequencer.getMaximumPoolSize(), 0, 1,
                                        IllegalArgumentException::new);
            this.sequencer = builder.sequencer;
            this.isPrivateSequencer = false;
        }
        this.backpressurer = new SubmissionPublisher<>(Objects.requireNonNull(builder.ayncBackpressurer), bufferSize);
        this.reliableBackpressuringSubscriber = new BackpressuringSubscriber(requestOnStartup, mboxOwner,
                                                                             realMbox::deliver, this.backpressurer);
        this.backpressurer.subscribe(reliableBackpressuringSubscriber);
    }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public boolean isEmpty() { return realMbox.isEmpty(); }

    @Override
    public boolean isFull() { return realMbox.isFull(); }

    @Override
    public long getMsgNum() { return realMbox.getMsgNum() + Integer.max(this.backpressurer.estimateMaximumLag(), 0); }

    @Override
    public long getMaxSize() { return realMbox.getMaxSize(); }

    @Override
    public Message getNextMessage() { return this.realMbox.getNextMessage(); }

    @Override
    public DeliveryStatus deliver(Message message) { return realMbox.deliver(message); }

    @Override
    public CompletionStage<Try<DeliveryStatus>> asyncDeliver(Message message) {
        var payloadType = message.getPayload().getClass();
        if (shouldNotBeDelayed(payloadType)) {
            return CompletableFuture.completedFuture(Try.of(() -> deliver(message)));
        }
        CompletableFuture<Try<DeliveryStatus>> trigger = new CompletableFuture<>();
        if (isBestEffort(backpressureTimeout) && canBeBackPressured(payloadType)) {
            //It's never going to stop this one
            reliableDelivery(message, backpressureTimeout, trigger);
        } else {
            try {
                /* This one could stop but we cannot force the calling thread to stop */
                this.sequencer.execute(() -> reliableDelivery(message, shouldNotBeBackPressured(payloadType)
                                                                       ? RELIABLE_DELIVERY_TIMEOUT
                                                                       : backpressureTimeout, trigger));
            } catch (Exception anyException) {
                trigger.complete(Try.ofFailure(anyException));
            }
        }
        return trigger;
    }

    public void request(long messagesNum) {
        Objects.requireNonNull(this.reliableBackpressuringSubscriber).request(messagesNum);
    }

    @Override
    public void close() throws Exception {
        this.backpressurer.close();
        if (isPrivateSequencer) {
            this.sequencer.shutdownNow();
        }
        this.realMbox.close();
    }

    /*
     * Exploit Java Submission publisher to backpressure a fast producer.
     * returns a completable future that will be completed with the result of the actual delivery of the message
     * in the mailbox
     */
    private void reliableDelivery(Message message, Duration backpressureTimeout,
                                  CompletableFuture<Try<DeliveryStatus>> trigger) {
        try {
            var waitTime = this.backpressurer.offer(new DeliveryRequest(message, trigger),
                                                         backpressureTimeout.toNanos(), TimeUnit.NANOSECONDS,
                                                         BackpressuringMbox::onBackPressure);
            if (waitTime < 0) {
                trigger.complete(Try.ofSuccess(DeliveryStatus.BACKPRESSURED));
            } else {
                trigger.complete(Try.ofSuccess(DeliveryStatus.DELIVERED));
            }
        } catch (Exception anyException) {
           trigger.complete(Try.ofFailure(anyException));
        }
    }

    private boolean isBestEffort(Duration backpressureTimeout) {
        return backpressureTimeout.compareTo(BEST_EFFORT_TIMEOUT) == 0;
    }

    private boolean shouldNotBeDelayed(Class<? extends Serializable> payloadType) {
        return this.notDelayed.contains(payloadType);
    }

    private boolean canBeBackPressured(Class<? extends Serializable> payloadType) {
        return !shouldNotBeBackPressured(payloadType);
    }

    private boolean shouldNotBeBackPressured(Class<? extends Serializable> payloadType) {
        return this.notBackpressurable.contains(payloadType);
    }

    @SuppressWarnings("SameReturnValue")
    private static boolean onBackPressure(Flow.Subscriber<? super DeliveryRequest> subscriber,
                                          DeliveryRequest request) {
        request.pendingTrigger.complete(Try.ofSuccess(DeliveryStatus.BACKPRESSURED));
        return false;
    }

    static class DeliveryRequest {
        final Message deliveryPayload;
        final CompletableFuture<Try<DeliveryStatus>> pendingTrigger;

        private DeliveryRequest(Message deliveryPayload, CompletableFuture<Try<DeliveryStatus>> pendingTrigger) {
            this.deliveryPayload = deliveryPayload;
            this.pendingTrigger = pendingTrigger;
        }
    }

    public static class Builder {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private MailBox realMbox;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Duration backpressureTimeout;
        private int bufferSize;
        private int requestOnStartup;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Executor ayncBackpressurer;
        @Nullable
        private ThreadPoolExecutor sequencer;
        private ReActorContext realMailboxOwner;
        private Set<Class<? extends Serializable>> notDelayable = Set.of();
        private Set<Class<? extends Serializable>> notBackpressurable = Set.of();

        private Builder() { }

        /**
         *
         * @param realMbox Backing-up mailbox
         * @return this builder
         */
        public Builder setRealMbox(MailBox realMbox) {
            this.realMbox = realMbox;
            return this;
        }

        /**
         *
         * @param backpressureTimeout maximum time that should be waited while attempting to deliver a new
         *                            message to a saturated mailbox. 0 means immediate fail is delivery
         *                            is not possible. Maximum value: Long.MAX_VALUE nanoseconds
         * @return this builder
         */
        public Builder setBackpressureTimeout(Duration backpressureTimeout) {
            this.backpressureTimeout = backpressureTimeout;
            return this;
        }

        /**
         *
         * @param bufferSize how many updates we can cache befor beginning to backpressure new updates.
         *                   Must be a positive integer
         * @return this builder
         */
        public Builder setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         *
         * @param requestOnStartup how main messages should be automatically made deliverable on startup.
         *                         Same semantic of Java Flow Subscription.request
         * @return this builder
         */
        public Builder setRequestOnStartup(int requestOnStartup) {
            this.requestOnStartup = requestOnStartup;
            return this;
        }

        /**
         *
         * @param asyncBackpressurer   Executor used to perform the potentially blocking delivery attempt
         * @return this builder
         */
        public Builder setAsyncBackpressurer(Executor asyncBackpressurer) {
            this.ayncBackpressurer = asyncBackpressurer;
            return this;
        }

        /**
         * In order to be completely async, the submissions must be done using an executor. This class relies on
         * {@link SubmissionPublisher} for backpressuring and its {@link SubmissionPublisher#offer(Object, long, TimeUnit, BiPredicate)}
         * method may block. Such a block could case a block of the dispatcher too, leading to a deadlock.
         * This executor is used for asynchronously attempting an {@link SubmissionPublisher#offer(Object, long, TimeUnit, BiPredicate)}
         * without blocking the caller. If a {@code sequencer} is not provided, a new one will be automatically
         * created. Providing this can be useful when a lot of {@link BackpressuringMbox} are going to be created and
         * we are willing to lose some parallelism instead of creating a huge number of threads
         * @param sequencer an executor that ensures the sequentiality of the tasks submitted
         * @return this builder
         */
        public Builder setSequencer(@Nullable ThreadPoolExecutor sequencer) {
            this.sequencer = sequencer;
            return this;
        }

        /**
         *
         * @param notDelayable Message types that cannot be wait or backpressured. The delivery will be attempted
         *                     immediately
         * @return this builder
         */
        public Builder setNonDelayable(Set<Class<? extends Serializable>> notDelayable) {
            this.notDelayable = notDelayable;
            return this;
        }

        /**
         *
         * @param notBackpressurable Messages that cannot be lost. If a delivery cannot be done immediately the system
         *                           will wait till when necessary to deliver the message
         * @return this builder
         */
        public Builder setNonBackpressurable(Set<Class<? extends Serializable>> notBackpressurable) {
            this.notBackpressurable = notBackpressurable;
            return this;
        }

        /**
         *
         * @param realMailboxOwner {@link ReActorContext} of the reactor owning {@link BackpressuringMbox#realMbox}
         * @return this builder
         */
        public Builder setRealMailboxOwner(ReActorContext realMailboxOwner) {
            this.realMailboxOwner = realMailboxOwner;
            return this;
        }

        /**
         *
         * @return a {@link BackpressuringMbox}
         * @throws NullPointerException if any of the non null arguments is found to be null
         * @throws IllegalArgumentException if {@code sequencer} can create more than one thread
         *                                  if {@code bufferSize} is not positive
         *                                  if {@code requestOnStartup} is negative
         */
        public BackpressuringMbox build() { return new BackpressuringMbox(this); }
    }
}
