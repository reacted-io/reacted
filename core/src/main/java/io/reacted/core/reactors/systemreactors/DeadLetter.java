/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class DeadLetter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetter.class);
    public static final ReActions DEADLETTERS = ReActions.newBuilder()
                                                         .reAct(DeadMessage.class,
                                                                (ctx, payload) -> ctx.getSelf().tell(ctx.getSender(),
                                                                                                     payload.getPayload()))
                                                         .reAct(ReActorService.RouteeReSpawnRequest.class,
                                                                (ctx, payload) -> {})
                                                         .reAct(ReActorInit.class, (ctx, payload) -> {})
                                                         .reAct(ReActorStop.class, (ctx, payload) -> {})
                                                         .reAct(new DeadLetter()::onMessage)
                                                         .build();
    /* All messages for reactors not found will be rerouted here */
    public static final AtomicLong RECEIVED = new AtomicLong();

    private  <PayloadT extends Serializable>
    void onMessage(ReActorContext reActorContext, PayloadT message) {
        LOGGER.debug("{} of {}: {}", getClass().getSimpleName(),
                    reActorContext.getReActorSystem()
                                  .getLocalReActorSystemId()
                                  .getReActorSystemName(),
                    message.toString());
        RECEIVED.incrementAndGet();
    }
}
