/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.mailboxes.MailBox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.streams.ReactedSubmissionPublisher.ReActedSubscriptionConfig;
import io.reacted.streams.exceptions.RemoteRegistrationException;
import io.reacted.streams.messages.PublisherComplete;
import io.reacted.streams.messages.PublisherInterrupt;
import io.reacted.streams.messages.PublisherShutdown;
import io.reacted.streams.messages.SubscriberError;
import io.reacted.streams.messages.SubscriptionReply;
import io.reacted.streams.messages.SubscriptionRequest;
import io.reacted.streams.messages.UnsubscriptionRequest;

import java.time.Instant;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;

import static io.reacted.core.utils.ReActedUtils.ifNotDelivered;

@NonNullByDefault
public class BackpressureManager<PayloadT extends Serializable> implements Flow.Subscription,
                                                                           AutoCloseable,
                                                                           ReActiveEntity {
    private final Flow.Subscriber<? super PayloadT> subscriber;
    private final ReActorRef feedGate;
    private final BackpressuringMbox.Builder bpMailboxBuilder;
    private final CompletionStage<Void> onSubscriptionCompleteTrigger;
    @Nullable
    private MailBox backpressuringMbox;
    @Nullable
    private volatile ReActorContext backpressurerCtx;

    /**
     * It manages a backpressured reactive streams. A reactive stream can accept local (to the reactor system) or
     * remote subscribers
     *
     * @param subscription A {@link ReActedSubscriptionConfig} containing the details of the subscriber
     * @param feedGate source of data for the managed stream
     */
    BackpressureManager(ReActedSubscriptionConfig<PayloadT> subscription,
                        Subscriber<? super PayloadT> subscriber,
                        ReActorRef feedGate, CompletionStage<Void> onSubscriptionCompleteTrigger) {
        this.onSubscriptionCompleteTrigger = onSubscriptionCompleteTrigger;
        this.subscriber = subscriber;
        this.feedGate = Objects.requireNonNull(feedGate);
        this.bpMailboxBuilder = BackpressuringMbox.newBuilder()
                                                  .setRealMbox(new BoundedBasicMbox(subscription.getBufferSize()))
                                                  .setBackpressureTimeout(subscription.getBackpressureTimeout())
                                                  .setBufferSize(subscription.getBufferSize())
                                                  .setRequestOnStartup(0)
                                                  .setAsyncBackpressurer(subscription.getAsyncBackpressurer())
                                                  .setNonDelayable(Set.of(ReActorInit.class, ReActorStop.class,
                                                                          SubscriptionRequest.class,
                                                                          SubscriptionReply.class,
                                                                          UnsubscriptionRequest.class,
                                                                          SubscriberError.class,
                                                                          PublisherInterrupt.class))
                                                  .setNonBackpressurable(Set.of(PublisherComplete.class,
                                                                                PublisherShutdown.class))
                                                  .setSequencer(subscription.getSequencer());
    }

    @Override
    public void request(long elements) {
        if (elements <= 0) {
            errorTermination(Objects.requireNonNull(backpressurerCtx),
                             new IllegalArgumentException("non-positive subscription request"), subscriber);
        } else {
            if (backpressurerCtx != null && backpressuringMbox != null) {
                backpressuringMbox.request(elements);
            }
        }
    }

    @Override
    public void cancel() { close(); }

    @Override
    public void close() {
        if (backpressurerCtx != null) {
            Objects.requireNonNull(backpressurerCtx).stop();
        }
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(ReActorStop.class, this::onStop)
                        .reAct(SubscriptionReply.class, this::onSubscriptionReply)
                        .reAct(SubscriberError.class, this::onSubscriberError)
                        .reAct(PublisherComplete.class, this::onPublisherComplete)
                        .reAct(PublisherInterrupt.class, this::onPublisherInterrupt)
                        .reAct(this::forwarder)
                        .build();
    }

    Function<ReActorContext, MailBox> getManagerMailbox() {
        return mboxOwner -> bpMailboxBuilder.setRealMailboxOwner(mboxOwner).build();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        feedGate.route(ReActorRef.NO_REACTOR_REF, new UnsubscriptionRequest(raCtx.getSelf()));
    }

    private void forwarder(ReActorContext raCtx, Object anyPayload) {
        try {
            //noinspection unchecked
            subscriber.onNext((PayloadT) anyPayload);
        } catch (Exception anyException) {
           errorTermination(raCtx, anyException, subscriber);
        }
    }

    private void onSubscriptionReply(ReActorContext raCtx, SubscriptionReply payload) {
        onSubscriptionCompleteTrigger.toCompletableFuture().complete(null);
        if (payload.isSuccess()) {
            Try.ofRunnable(() -> subscriber.onSubscribe(this))
               .ifError(error -> errorTermination(raCtx, error, subscriber));
        } else {
            errorTermination(raCtx, new RemoteRegistrationException(), subscriber);
        }
    }

    private void onSubscriberError(ReActorContext raCtx, SubscriberError error) {
        errorTermination(raCtx, error.getError(), subscriber);
    }

    private void onPublisherComplete(ReActorContext raCtx, PublisherComplete publisherComplete) {
        completeTermination(raCtx, subscriber);
    }

    private void onPublisherInterrupt(ReActorContext raCtx, PublisherInterrupt interrupt) {
        completeTermination(raCtx, subscriber);
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        this.backpressurerCtx = raCtx;
        this.backpressuringMbox = raCtx.getMbox();

        Consumer<Throwable> onSubscriptionError;
        onSubscriptionError = error -> { subscriber.onSubscribe(this);
                                         errorTermination(raCtx, error, subscriber); };
        if (feedGate.route(raCtx.getSelf(), new SubscriptionRequest(raCtx.getSelf())).isNotSent()) {
            onSubscriptionError.accept(new DeliveryException());
        }
    }

    private void completeTermination(ReActorContext raCtx,
                                     Flow.Subscriber<? super PayloadT> localSubscriber) {
        close();
        Try.ofRunnable(localSubscriber::onComplete)
           .ifError(error -> raCtx.logError("Error in {} onComplete: ",
                                            localSubscriber.getClass().getSimpleName(), error));
    }

    private void errorTermination(ReActorContext raCtx, Throwable handlingError,
                                  Flow.Subscriber<? super PayloadT> localSubscriber) {
        close();
        Try.ofRunnable(() -> localSubscriber.onError(handlingError))
           .ifError(error -> raCtx.logError("Error in {} onError: ",
                                            localSubscriber.getClass().getSimpleName(), error));
    }
}
