/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.ping;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.util.Objects;

@NonNullByDefault
class SimpleTestReActor implements ReActor {
    //Implementing this interface is not required
    private final String splitter;
    private int receivedPings = 0;
    private final int expectedMessages;

    public SimpleTestReActor(String splitter, int expectedMessages) {
        this.splitter = Objects.requireNonNull(splitter);
        this.expectedMessages = ObjectUtils.requiredInRange(expectedMessages, 0, Integer.MAX_VALUE,
                                                            IllegalArgumentException::new);
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (ctx, init) -> ctx.logInfo("ReActor Started!"))
                        .reAct(ReActorStop.class, (ctx, stop) -> ctx.logInfo("ReActor Stopped! Received {}/{} pings",
                                                                             receivedPings, expectedMessages))
                        .reAct(PreparationRequest.class, (ctx, prepReq) -> ctx.logInfo("ReActor is ready!"))
                        .reAct(ReActedMessage.StringMessage.class, this::onPing)
                        .build();
    }

    private void onPing(ReActorContext ctx, ReActedMessage.StringMessage ping) {
        Try.ofRunnable(() -> ctx.logInfo("Received ping {} on dispatcher {}", ping.toString().split(splitter)[1].trim(),
                                               Thread.currentThread().getName()))
           .ifError(error -> ctx.logError("Illegal ping format received", error));
        receivedPings++;
    }

    @Nonnull
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setDispatcherName(Dispatcher.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(ctx -> new UnboundedMbox())
                            .setReActorName(SimpleTestReActor.class.getSimpleName())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }
}
