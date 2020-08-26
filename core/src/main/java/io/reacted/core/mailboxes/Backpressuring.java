/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@NonNullByDefault
public class Backpressuring implements MailBox, AutoCloseable {
    private static final CompletableFuture<Try<DeliveryStatus>> BACKPRESSURED =
            CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.BACKPRESSURED));
    private final long backpressuringTimeoutNanoseconds;
    private final boolean isBestEffort;
    private final MailBox realMbox;
    private final SubmissionPublisher<DeliveryRequest> backpressurer;
    private final AtomicReference<Flow.Subscription> subscription;
    private final Set<Class<? extends Serializable>> notBackpressurable;
    private final Executor asyncExecutor;
    private final AtomicLong bestEffortAllowedMessagesCount;
    private volatile CompletableFuture<Try<DeliveryStatus>> previousRequest;

    /**
     * Backpressuring wrapper for any other mailbox type.
     *
     * @param realMbox            Backing-up mailbox
     * @param backpressureTimeout maximum time that should be waited whil attempting to deliver a new
     *                            message to a saturated mailbox. 0 means immediate fail is delivery
     *                            is not possible
     * @param bufferSize          how many updates we can cache befor beginning to backpressure new updates.
     *                            1 <= bufferSize <= Integer.MAX_VALUE
     * @param requestOnStartup    how main messages should be automatically made deliverable on startup.
     *                            Same semantic of Java Flow Subscription.request
     * @param asyncExecutor       Executor used to perform the delivery attempt
     * @param notBackpressurable  Message types that cannot be backpressured by this wrapper
     */
    public Backpressuring(MailBox realMbox, Duration backpressureTimeout, int bufferSize, int requestOnStartup,
                          Executor asyncExecutor, Set<Class<? extends Serializable>> notBackpressurable) {
        this.previousRequest = CompletableFuture.supplyAsync(() -> Try.ofSuccess(DeliveryStatus.DELIVERED),
                                                             asyncExecutor);
        this.backpressuringTimeoutNanoseconds = Objects.requireNonNull(backpressureTimeout)
                                                       .toNanos();
        this.isBestEffort = isBestEffort(backpressureTimeout);
        this.realMbox = Objects.requireNonNull(realMbox);
        this.notBackpressurable = Objects.requireNonNull(notBackpressurable);
        //TODO extract this behavior with specialized backpressurers
        this.backpressurer = new SubmissionPublisher<>(Objects.requireNonNull(asyncExecutor), bufferSize);
        this.asyncExecutor = Objects.requireNonNull(asyncExecutor);
        this.subscription = new AtomicReference<>();
        this.bestEffortAllowedMessagesCount = new AtomicLong();
        this.backpressurer.subscribe(new Flow.Subscriber<>() {
            private volatile boolean isCompleted;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                Backpressuring.this.subscription.set(subscription);
                if (requestOnStartup > 0) {
                    Backpressuring.this.bestEffortAllowedMessagesCount.addAndGet(requestOnStartup);
                    subscription.request(requestOnStartup);
                }
            }

            @Override
            public void onNext(DeliveryRequest item) {
                var backpressuringTrigger = item.pendingTrigger;
                if (this.isCompleted) {
                    backpressuringTrigger.complete(Try.ofSuccess(DeliveryStatus.NOT_DELIVERED));
                } else {
                    backpressuringTrigger.complete(Try.of(() -> Backpressuring.this.deliver(item.deliveryPayload))
                                                      .peekFailure(error -> asyncExecutor.execute(() -> onError(error))));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (!this.isCompleted) {
                    this.isCompleted = true;
                    Backpressuring.this.backpressurer.close();
                }
            }

            @Override
            public void onComplete() {
                if(!this.isCompleted) {
                    this.isCompleted = true;
                }
            }
        });
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
        if (shouldNotBeBackpressured(message.getPayload()
                                            .getClass())) {
            return CompletableFuture.supplyAsync(() -> Try.ofSuccess(deliver(message)),
                                                 asyncExecutor);
            //return CompletableFuture.completedFuture(Try.of(() -> deliver(message)));
        }
        if (this.isBestEffort) {
            synchronized (this) {
                if (this.bestEffortAllowedMessagesCount.get() == 0) {
                    return BACKPRESSURED;
                }
                this.bestEffortAllowedMessagesCount.decrementAndGet();
            }
            return CompletableFuture.supplyAsync(() -> Try.ofSuccess(deliver(message)), asyncExecutor);
            //return CompletableFuture.completedFuture(Try.of(() -> deliver(message)));
        }

        CompletableFuture<Try<DeliveryStatus>> trigger = new CompletableFuture<>();
        synchronized (this) {
            Runnable asyncBackpressuredDeliveryAttempt = () -> Try.of(() -> this.backpressurer.offer(new DeliveryRequest(message, trigger),
                                                                                                     this.backpressuringTimeoutNanoseconds,
                                                                                                     TimeUnit.NANOSECONDS,
                                                                                                     Backpressuring::onBackPressure))
                                                                  .ifError(error -> trigger.complete(Try.ofFailure(error)));
            this.previousRequest.thenAccept(previousDeliveryResult -> this.asyncExecutor.execute(asyncBackpressuredDeliveryAttempt));
            this.previousRequest = trigger;
        }
        return trigger;
    }

    public void request(long messagesNum) {
        this.subscription.get().request(messagesNum);
        this.bestEffortAllowedMessagesCount.addAndGet(messagesNum);
    }

    @Override
    public void close() {
        this.backpressurer.close();
    }

    public static Duration getBackPressureTimeout() { return Duration.ZERO; }

    public static boolean isBestEffort(Duration backpressureTimeout) {
        return backpressureTimeout.isZero();
    }

    private boolean shouldNotBeBackpressured(Class<? extends Serializable> payloadType) {
        return this.notBackpressurable.contains(payloadType);
    }

    private static boolean onBackPressure(Flow.Subscriber<? super DeliveryRequest> subscriber,
                                          DeliveryRequest request) {
        request.pendingTrigger.complete(Try.ofSuccess(DeliveryStatus.BACKPRESSURED));
        return false;
    }

    private static class DeliveryRequest {
        private final Message deliveryPayload;
        private final CompletableFuture<Try<DeliveryStatus>> pendingTrigger;

        private DeliveryRequest(Message deliveryPayload, CompletableFuture<Try<DeliveryStatus>> pendingTrigger) {
            this.deliveryPayload = deliveryPayload;
            this.pendingTrigger = pendingTrigger;
        }
    }
}
