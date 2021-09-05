/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.LongAdder;

public class MagicTestReActor implements ReActor {

    public static final LongAdder RECEIVED = new LongAdder();
    public static final LongAdder DEADMSG = new LongAdder();

    private final int msgMaxValue;
    private final boolean checkMsgOrdering;
    private final int maxReceived = Integer.MIN_VALUE;
    private final ReActorConfig reActorConfig;
    private final ReActions reActions;

    public MagicTestReActor(int maxMsgValue, boolean checkMsgOrdering, String reactorName) {
        this(maxMsgValue, checkMsgOrdering, ReActorConfig.newBuilder()
                                                         .setDispatcherName(
                                                             Dispatcher.DEFAULT_DISPATCHER_NAME)
                                                         .setMailBoxProvider(ctx -> new BasicMbox())
                                                         .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                                         .setReActorName(reactorName)
                                                         .build());
    }

    public MagicTestReActor(int maxMsgValue, boolean checkMsgOrdering, ReActorConfig reActorConfig) {
        this.msgMaxValue = maxMsgValue;
        this.checkMsgOrdering = checkMsgOrdering;
        this.reActorConfig = reActorConfig;
        this.reActions = ReActions.newBuilder()
                                  .reAct(DeadMessage.class, this::onDeadMessage)
                                  .reAct(Message.class, this::onMessage)
                                  .build();
    }

    public void onDeadMessage(ReActorContext ctx, DeadMessage deadMessage) {
        System.err.printf("Dispatcher used [%s] Payload: %s%n", ctx.getDispatcher()
                                                                   .getName(), deadMessage.getPayload());
        DEADMSG.increment();
    }

    public void onMessage(ReActorContext ctx, Message message) {
        System.err.printf("Dispatcher used [%s] Payload: %s%n", ctx.getDispatcher()
                                                                   .getName(), message.getPayload());
        RECEIVED.increment();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return reActorConfig;
    }

    @Nonnull
    @Override
    public ReActions getReActions() { return reActions; }
}
