/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NonNullByDefault
public class Ask<ReplyT extends Serializable> implements ReActor {
    private final ScheduledExecutorService scheduledExecutorService;
    private final Duration askTimeout;
    private final Class<ReplyT> expectedReplyType;
    private final CompletableFuture<Try<ReplyT>> completionTrigger;
    private final String requestName;
    private final ReActorRef target;
    private final Serializable request;
    @Nullable
    private ScheduledFuture<?> askExpirationTask;

    public Ask(ScheduledExecutorService scheduledExecutorService, Duration askTimeout, Class<ReplyT> expectedReplyType,
               CompletableFuture<Try<ReplyT>> completionTrigger, String requestName, ReActorRef target,
               Serializable request) {
        this.scheduledExecutorService = Objects.requireNonNull(scheduledExecutorService);
        this.askTimeout = Objects.requireNonNull(askTimeout);
        this.expectedReplyType = Objects.requireNonNull(expectedReplyType);
        this.completionTrigger = Objects.requireNonNull(completionTrigger);
        this.requestName = Objects.requireNonNull(requestName);
        this.target = Objects.requireNonNull(target);
        this.request = Objects.requireNonNull(request);
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(expectedReplyType, this::onExpectedReply)
                        .reAct(ReActorStop.class, this::onStop)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestName + "|" + target.getReActorId().getReActorUUID() + "|" +
                                            request.getClass().getSimpleName() + "|" +
                                            expectedReplyType.getSimpleName())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        try {
            this.askExpirationTask = this.scheduledExecutorService.schedule(getOnTimeoutExpireTask(raCtx, completionTrigger),
                                                                            askTimeout.toMillis(), TimeUnit.MILLISECONDS);
            target.tell(raCtx.getSelf(), request)
                  .thenAccept(delivery -> delivery.filter(DeliveryStatus::isDelivered, DeliveryException::new)
                                                  .ifError(error -> { raCtx.stop();
                                                                      this.completionTrigger.completeAsync(() -> Try.ofFailure(error)); }));
        } catch (RejectedExecutionException poolCannotHandleRequest) {
            this.completionTrigger.completeAsync(() -> Try.ofFailure(poolCannotHandleRequest));
            raCtx.stop();
        }
    }

    private void onExpectedReply(ReActorContext raCtx, ReplyT reply) {
        this.completionTrigger.completeAsync(() -> Try.ofSuccess(reply));
        raCtx.stop();
    }

    private void onStop(ReActorContext raCtx, ReActorStop stop) {
        if (this.askExpirationTask != null) {
            this.askExpirationTask.cancel(true);
        }
    }

    private static <ReplyT extends Serializable> Runnable
    getOnTimeoutExpireTask(ReActorContext raCtx, CompletableFuture<Try<ReplyT>> askFuture) {
        return () -> { raCtx.stop();
                       askFuture.completeAsync(() -> Try.ofFailure(new TimeoutException())); };
    }
}
