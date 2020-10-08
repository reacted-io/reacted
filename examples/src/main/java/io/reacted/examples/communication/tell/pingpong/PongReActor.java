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
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;

import javax.annotation.Nonnull;
import java.util.Timer;
import java.util.TimerTask;

public class PongReActor implements ReActor {
    private final Timer pongTimer = new Timer();

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(Ping.class, this::onPing)
                        .reAct(ReActorStop.class, this::onStop)
                        .reAct(((raCtx, payload) -> {
                        }))
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(PongReActor.class.getSimpleName())
                            .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                            .setMailBoxProvider(ctx -> new BasicMbox())
                            .setTypedSniffSubscriptions(SniffSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }

    public void onPing(ReActorContext raCtx, Ping ping) {
        System.out.printf("Pong received a ping for seq %d%n", ping.getPingValue());
        //Schedule a reply after 1 second
        this.pongTimer.schedule(new TimerTask() {
            @Override
            public void run() { raCtx.reply(new Pong(ping.getPingValue())); }
        }, 1000);
    }

    public void onStop(ReActorContext raCtx, ReActorStop stop) {
        this.pongTimer.cancel();
        this.pongTimer.purge();
    }
}
