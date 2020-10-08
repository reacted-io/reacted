/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.tell.pingpong;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SniffSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;

class PingReActor implements ReActor {
    private int pongReceived;
    private int pingSent;
    private final ReActorRef ponger;

    PingReActor(ReActorRef ponger) {
        this.pongReceived = 0;
        this.pingSent = 0;
        this.ponger = ponger;
    }

    @Override
    @Nonnull
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, this::onInit)
                        .reAct(Pong.class, this::onPong)
                        .reAct((raCtx, payload) -> {
                        })
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(PingReActor.class.getSimpleName())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setTypedSniffSubscriptions(SniffSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }

    private void onInit(ReActorContext raCtx, ReActorInit init) {
        sendPing(raCtx.getSelf(), this.pingSent++);
    }

    private void onPong(ReActorContext raCtx, Pong pong) {
        System.out.printf("Ping sent %d Pong received %d%n", this.pingSent, this.pongReceived++);
        sendPing(raCtx.getSelf(), this.pingSent++);
    }

    private void sendPing(ReActorRef sender, int pingSeq) {
        this.ponger.tell(sender, new Ping(pingSeq));
    }
}
