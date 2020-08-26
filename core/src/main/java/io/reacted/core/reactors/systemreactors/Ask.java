/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SubscriptionPolicy;
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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

@NonNullByDefault
public class Ask<ReplyT extends Serializable> implements ReActor {
    private final Timer systemTimer;
    private final Duration askTimeout;
    private final Class<ReplyT> expectedReplyType;
    private final CompletableFuture<Try<ReplyT>> completionTrigger;
    private final String requestName;
    private final ReActorRef target;
    private final Serializable request;
    @Nullable
    private TimerTask askExpirationTask;

    public Ask(Timer systemTimer, Duration askTimeout, Class<ReplyT> expectedReplyType,
               CompletableFuture<Try<ReplyT>> completionTrigger, String requestName, ReActorRef target,
               Serializable request) {
        this.systemTimer = Objects.requireNonNull(systemTimer);
        this.askTimeout = Objects.requireNonNull(askTimeout);
        this.expectedReplyType = Objects.requireNonNull(expectedReplyType);
        this.completionTrigger = Objects.requireNonNull(completionTrigger);
        this.requestName = Objects.requireNonNull(requestName);
        this.target = Objects.requireNonNull(target);
        this.request = Objects.requireNonNull(request);
    }

    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> {
                            this.askExpirationTask = getOnTimeoutExpireTask(raCtx, completionTrigger);
                            systemTimer.schedule(this.askExpirationTask, askTimeout.toMillis());
                            target.tell(raCtx.getSelf(), request)
                                  .thenAccept(delivery -> delivery.filter(DeliveryStatus::isDelivered)
                                                                  .ifError(error -> this.completionTrigger.complete(Try.ofFailure(error))));
                        })
                        .reAct(expectedReplyType, (raCtx, reply) -> {
                            raCtx.stop();
                            completionTrigger.complete(Try.ofSuccess(reply));
                        })
                        .reAct(ReActorStop.class, (raCtx, reActorStop) -> {
                            if (this.askExpirationTask != null) {
                                this.askExpirationTask.cancel();
                            }
                        })
                        .build();
    }

    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestName + "_" + target.getReActorId().getReActorUuid() + "_" +
                                            request.getClass().getSimpleName() + "_" +
                                            expectedReplyType.getSimpleName())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(BasicMbox::new)
                            .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }

    private static <ReplyT extends Serializable> TimerTask getOnTimeoutExpireTask(ReActorContext raCtx,
                                                                                  CompletableFuture<Try<ReplyT>> askFuture) {
        return new TimerTask() {
            @Override
            public void run() {
                raCtx.stop();
                askFuture.complete(Try.ofFailure(new TimeoutException()));
            }
        };
    }
}
