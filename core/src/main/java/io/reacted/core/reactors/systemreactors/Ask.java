/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.utils.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NonNullByDefault
public class Ask<ReplyT extends Serializable> implements ReActor {
    private final Duration askTimeout;
    private final Class<ReplyT> expectedReplyType;
    private final CompletableFuture<Try<ReplyT>> completionTrigger;
    private final String requestName;
    private final ReActorRef target;
    private final Serializable request;

    public Ask(Duration askTimeout, Class<ReplyT> expectedReplyType, CompletableFuture<Try<ReplyT>> completionTrigger,
               String requestName, ReActorRef target, Serializable request) {
        this.askTimeout = ObjectUtils.checkNonNullPositiveTimeInterval(askTimeout);
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
                        .reAct(ReActions::noReAction)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestName + "|" + target.getReActorId().getReActorUUID() + "|" +
                                            request.getClass().getSimpleName() + "|" +
                                            expectedReplyType.getSimpleName())
                            .build();
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        try {
            target.tell(raCtx.getSelf(), request).toCompletableFuture()
                  .thenAccept(delivery -> delivery.filter(DeliveryStatus::isDelivered, DeliveryException::new)
                                                  .ifError(error -> { raCtx.stop();
                                                                      this.completionTrigger.completeAsync(() -> Try.ofFailure(error)); }));
            this.completionTrigger.completeOnTimeout(Try.ofFailure(new TimeoutException()),
                                                     this.askTimeout.toMillis(), TimeUnit.MILLISECONDS)
                                  .thenAcceptAsync(reply -> raCtx.stop());
        } catch (RejectedExecutionException poolCannotHandleRequest) {
            this.completionTrigger.completeAsync(() -> Try.ofFailure(poolCannotHandleRequest));
            raCtx.stop();
        }
    }

    private void onExpectedReply(ReActorContext raCtx, ReplyT reply) {
        this.completionTrigger.completeAsync(() -> Try.ofSuccess(reply));
        raCtx.stop();
    }
}
