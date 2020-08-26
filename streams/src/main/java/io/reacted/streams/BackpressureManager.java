/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.mailboxes.Backpressuring;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.mailboxes.MailBox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.streams.messages.SubscriberComplete;
import io.reacted.streams.messages.SubscriberError;
import io.reacted.streams.messages.SubscriptionReply;
import io.reacted.streams.messages.SubscriptionRequest;
import io.reacted.streams.messages.UnsubscriptionRequest;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

@NonNullByDefault
public class BackpressureManager<PayloadT extends Serializable> implements Flow.Subscription, AutoCloseable {
    private static final int NO_EXTRA_BUFFERING_REQUIRED = 0;
    private final Flow.Subscriber<? super PayloadT> subscriber;
    private final ReActorRef feedGate;
    private final Executor subscriberExecutor;
    private final Backpressuring backpressuredMailbox;
    private CompletableFuture<Void> executionChainer = CompletableFuture.completedFuture(null);
    @Nullable
    private volatile ReActorContext backpressurerCtx;

    /**
     * Local/Remote subscribers subscription/unsubscription manager
     *
     * @param subscriber
     * @param feedGate
     * @param subscriberExecutor
     * @param bufferSize
     * @param backpressureTimeout
     */
    BackpressureManager(Flow.Subscriber<? super PayloadT> subscriber, ReActorRef feedGate,
                        Executor subscriberExecutor, int bufferSize, Duration backpressureTimeout) {
        this.subscriber = Objects.requireNonNull(subscriber);
        this.feedGate = Objects.requireNonNull(feedGate);
        this.subscriberExecutor = Objects.requireNonNull(subscriberExecutor);
        this.backpressuredMailbox = new Backpressuring(new BoundedBasicMbox(bufferSize),
                                                       Objects.requireNonNull(backpressureTimeout),
                                                       bufferSize,
                                                       Backpressuring.isBestEffort(backpressureTimeout)
                                                       ? bufferSize
                                                       : NO_EXTRA_BUFFERING_REQUIRED,
                                                       subscriberExecutor,
                                                       Set.of(ReActorInit.class, ReActorStop.class,
                                                              SubscriptionRequest.class, SubscriptionReply.class,
                                                              UnsubscriptionRequest.class, SubscriberError.class,
                                                              SubscriberComplete.class));
    }

    @Override
    public void request(long n) {
        if (n > 0) {
            this.backpressuredMailbox.request(n);
        } else if (this.backpressurerCtx != null) {
            errorTermination(Objects.requireNonNull(backpressurerCtx),
                             new IllegalArgumentException("non-positive " + "subscription " + "request"),
                             this.subscriber);
        }
    }

    @Override
    public void cancel() {
        if (this.backpressurerCtx != null) {
            close();
        }
    }

    @Override
    public void close() {
        if (this.backpressurerCtx != null) {
            var ctx = Objects.requireNonNull(this.backpressurerCtx);
            ctx.stop();
            this.feedGate.tell(ReActorRef.NO_REACTOR_REF, new UnsubscriptionRequest(ctx.getSelf()));
            this.backpressuredMailbox.close();
        }
    }

    Supplier<MailBox> getManagerMailbox() {
        return () -> this.backpressuredMailbox;
    }

    ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(ReActorStop.class, ReActions::noReAction)
                        .reAct(SubscriptionReply.class, this::onSubscriptionReply)
                        .reAct(SubscriberError.class, this::onSubscriberError)
                        .reAct(SubscriberComplete.class, this::onSubscriberComplete)
                        .reAct(this::forwarder)
                        .build();
    }

    private void forwarder(ReActorContext raCtx, Object anyPayload) {
        //noinspection unchecked
        this.executionChainer = this.executionChainer.thenAcceptAsync(voidRet -> Try.ofRunnable(() -> this.subscriber.onNext((PayloadT) anyPayload))
                                                                                    .ifError(error -> errorTermination(raCtx, error, this.subscriber)), subscriberExecutor);
    }

    private void onSubscriptionReply(ReActorContext raCtx, SubscriptionReply payload) {
        if (payload.isSuccess()) {
            executionChainer =
                    executionChainer.thenAcceptAsync(voidRet -> Try.ofRunnable(() -> this.subscriber.onSubscribe(this))
                                                                              .ifError(error -> errorTermination(raCtx, error, this.subscriber)), subscriberExecutor);
        } else {
            executionChainer = executionChainer.thenAcceptAsync(voidRet -> errorTermination(raCtx,
                                                                                            new RuntimeException(
                                                                                                    "RemoteRegistrationException"), this.subscriber));
        }
    }

    private void onSubscriberError(ReActorContext raCtx, SubscriberError error) {
        executionChainer = executionChainer.thenAcceptAsync(voidRet -> errorTermination(raCtx, error.getError(),
                                                                                        this.subscriber),
                                                            subscriberExecutor);
    }

    private void onSubscriberComplete(ReActorContext raCtx, SubscriberComplete subscriberComplete) {
        executionChainer = executionChainer.thenAcceptAsync(voidRet -> completeTermination(raCtx, this.subscriber),
                                                            subscriberExecutor);
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        this.backpressurerCtx = raCtx;
        this.feedGate.tell(raCtx.getSelf(), new SubscriptionRequest(raCtx.getSelf()))
                     .thenAccept(deliveryStatusTry -> deliveryStatusTry.filter(DeliveryStatus::isDelivered)
                                                                       .ifError(error -> {
                                                                           this.subscriber.onSubscribe(this);
                                                                           errorTermination(raCtx, error,
                                                                                            this.subscriber);
                                                                       }));

    }

    private void completeTermination(ReActorContext raCtx, Flow.Subscriber<? super PayloadT> localSubscriber) {
        close();
        Try.ofRunnable(localSubscriber::onComplete)
           .ifError(error -> raCtx.getReActorSystem().logError("Error in %s onComplete: ", error,
                                                               localSubscriber.getClass().getSimpleName()));
    }

    private void errorTermination(ReActorContext raCtx, Throwable handlingError,
                                  Flow.Subscriber<? super PayloadT> localSubscriber) {
        close();
        Try.ofRunnable(() -> localSubscriber.onError(handlingError))
           .ifError(error -> raCtx.getReActorSystem().logError("Error in %s onError: ", error,
                                                               localSubscriber.getClass().getSimpleName()));
    }
}
