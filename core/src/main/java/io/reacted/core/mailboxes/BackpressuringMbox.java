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
import javax.annotation.Nullable;
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

@NonNullByDefault
public class BackpressuringMbox implements MailBox, AutoCloseable {
    public static final Duration BEST_EFFORT_TIMEOUT = Duration.ofNanos(0);
    private final Duration backpressureTimeout;
    private final MailBox realMbox;
    @Nullable
    private final SubmissionPublisher<DeliveryRequest> backpressurer;
    @Nullable
    private final BackpressuringSubscriber reliableBackpressuringSubscriber;
    private final Set<Class<? extends Serializable>> notDelayed;
    private final Set<Class<? extends Serializable>> notBackpressurable;
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
     * @param asyncExecutor       Executor used to perform the delivery attempt
     * @param notDelayed  Message types that cannot be backpressured by this wrapper
     */
    public BackpressuringMbox(MailBox realMbox, Duration backpressureTimeout, int bufferSize, int requestOnStartup,
                              Executor asyncExecutor, Set<Class<? extends Serializable>> notDelayed,
                              Set<Class<? extends Serializable>> notBackpressurable) {
        this.backpressureTimeout = Objects.requireNonNull(backpressureTimeout);
        this.realMbox = Objects.requireNonNull(realMbox);
        this.notDelayed = Objects.requireNonNull(notDelayed);
        this.notBackpressurable = Objects.requireNonNull(notBackpressurable);
        this.backpressurer = new SubmissionPublisher<>(Objects.requireNonNull(asyncExecutor), bufferSize);
        this.reliableBackpressuringSubscriber = new BackpressuringSubscriber(requestOnStartup, realMbox::deliver,
                                                                             asyncExecutor, this.backpressurer);
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
        return reliableDelivery(message, shouldNotBeBackPressured(payloadType)
                                         ? Duration.ofNanos(Long.MAX_VALUE)
                                         : backpressureTimeout);
    }

    public void request(long messagesNum) {
        Objects.requireNonNull(this.reliableBackpressuringSubscriber).request(messagesNum);
    }

    @Override
    public void close() {
        if (this.backpressurer != null) {
            this.backpressurer.close();
        }
    }

    /*
     * Exploit Java Submission publisher to backpressure a fast producer.
     * returns a completable future that will be completed with the result of the actual delivery of the message
     * in the mailbox
     */
    private CompletableFuture<Try<DeliveryStatus>> reliableDelivery(Message message, Duration backpressureTimeout) {
        CompletableFuture<Try<DeliveryStatus>> trigger = new CompletableFuture<>();
        Try.of(() -> this.backpressurer.offer(new DeliveryRequest(message, trigger),
                                              backpressureTimeout.toNanos(), TimeUnit.NANOSECONDS,
                                              BackpressuringMbox::onBackPressure))
           .ifError(error -> trigger.complete(Try.ofFailure(error)));
        return trigger;
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
