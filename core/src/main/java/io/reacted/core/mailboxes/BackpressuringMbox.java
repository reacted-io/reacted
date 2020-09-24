/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
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
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public class BackpressuringMbox implements MailBox, AutoCloseable {
    public static final Duration BEST_EFFORT_TIMEOUT = Duration.ofNanos(0);
    public static final Duration RELIABLE_DELIVERY_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);
    private static final Logger LOGGER = LoggerFactory.getLogger(BackpressuringMbox.class);
    private final Duration backpressureTimeout;
    private final MailBox realMbox;
    @Nullable
    private final SubmissionPublisher<DeliveryRequest> backpressurer;
    @Nullable
    private final BackpressuringSubscriber reliableBackpressuringSubscriber;
    private final Set<Class<? extends Serializable>> notDelayed;
    private final Set<Class<? extends Serializable>> notBackpressurable;
    private final ExecutorService asyncSerialExecutor;
    /**
     * BackpressuringMbox wrapper for any other mailbox type.
     *
     * @param realMbox            Backing-up mailbox
     * @param backpressureTimeout maximum time that should be waited whil attempting to deliver a new
     *                            message to a saturated mailbox. 0 means immediate fail is delivery
     *                            is not possible
     * @param bufferSize          how many updates we can cache befor beginning to backpressure new updates.
     *                            Must be a positive integer
     * @param requestOnStartup    how main messages should be automatically made deliverable on startup.
     *                            Same semantic of Java Flow Subscription.request
     * @param ayncBackpressurer   Executor used to perform the potentially blocking delivery attempt
     * @param notDelayed  Message types that cannot be backpressured by this wrapper
     */
    public BackpressuringMbox(MailBox realMbox, Duration backpressureTimeout, int bufferSize, int requestOnStartup,
                              Executor ayncBackpressurer, Set<Class<? extends Serializable>> notDelayed,
                              Set<Class<? extends Serializable>> notBackpressurable) {
        this.backpressureTimeout = Objects.requireNonNull(backpressureTimeout);
        this.realMbox = Objects.requireNonNull(realMbox);
        this.notDelayed = Objects.requireNonNull(notDelayed);
        this.notBackpressurable = Objects.requireNonNull(notBackpressurable);
        var deliveryThreadFactory = new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler((thread, throwable) -> LOGGER.error("Uncaught exception in {} delivery thread",
                                                                                 BackpressuringMbox.class.getSimpleName(),
                                                                                 throwable))
                .build();
        this.asyncSerialExecutor = Executors.newSingleThreadExecutor(deliveryThreadFactory);
        this.backpressurer = new SubmissionPublisher<>(ayncBackpressurer, bufferSize);
        this.reliableBackpressuringSubscriber = new BackpressuringSubscriber(requestOnStartup, realMbox::deliver,
                                                                             this.backpressurer);
        this.backpressurer.subscribe(reliableBackpressuringSubscriber);
    }

    @Override
    public boolean isEmpty() { return realMbox.isEmpty(); }

    @Override
    public boolean isFull() { return realMbox.isFull(); }

    @Override
    public long getMsgNum() { return realMbox.getMsgNum(); }

    @Override
    public long getMaxSize() { return realMbox.getMaxSize(); }

    @Nonnull
    @Override
    public Message getNextMessage() { return this.realMbox.getNextMessage(); }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) { return realMbox.deliver(message); }

    @Override
    public CompletionStage<Try<DeliveryStatus>> asyncDeliver(Message message) {
        var payloadType = message.getPayload().getClass();
        if (shouldNotBeDelayed(payloadType)) {
            return CompletableFuture.completedFuture(Try.ofSuccess(deliver(message)));
        }
        CompletableFuture<Try<DeliveryStatus>> trigger = new CompletableFuture<>();
        Try.ofRunnable(() -> this.asyncSerialExecutor.execute(() -> reliableDelivery(message,
                                                                                     shouldNotBeBackPressured(payloadType)
                                                                                     ? RELIABLE_DELIVERY_TIMEOUT
                                                                                     : backpressureTimeout, trigger)))
           .ifError(error -> trigger.complete(Try.ofFailure(error)));
        return trigger;
    }

    public void request(long messagesNum) {
        Objects.requireNonNull(this.reliableBackpressuringSubscriber).request(messagesNum);
    }

    @Override
    public void close() {
        if (this.backpressurer != null) {
            this.backpressurer.close();
        }
        this.asyncSerialExecutor.shutdownNow();
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
            }
        } catch (Throwable anyError) {
           trigger.complete(Try.ofFailure(anyError));
        }
    }

    private boolean shouldNotBeDelayed(Class<? extends Serializable> payloadType) {
        return this.notDelayed.contains(payloadType);
    }

    private boolean shouldNotBeBackPressured(Class<? extends Serializable> payloadType) {
        return this.notBackpressurable.contains(payloadType);
    }

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
}
