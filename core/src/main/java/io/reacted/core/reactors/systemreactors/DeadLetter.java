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
import io.reacted.core.services.Service;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetter.class);
    public static final ReActions DEADLETTERS = ReActions.newBuilder()
                                                         .reAct(DeadMessage.class,
                                                                (ctx, payload) -> ctx.getSelf()
                                                                                     .tell(ctx.getSender(),
                                                                                           payload.getPayload()))
                                                         .reAct(Service.RouteeReSpawnRequest.class,
                                                                ReActions::noReAction)
                                                         .reAct(ReActorInit.class, ReActions::noReAction)
                                                         .reAct(ReActorStop.class, ReActions::noReAction)
                                                         .reAct(DeadLetter::onMessage)
                                                         .build();
    /* All messages for reactors not found will be rerouted here */
    public static final AtomicLong RECEIVED = new AtomicLong();

    private DeadLetter() { }

    private static <PayloadT extends Serializable>
    void onMessage(ReActorContext raCtx, PayloadT message) {
        LOGGER.info("{} of {}: {} of type {} from {}", DeadLetter.class.getSimpleName(),
                    raCtx.getReActorSystem().getLocalReActorSystemId().getReActorSystemName(), message, message.getClass(),
                    raCtx.getSender());
        RECEIVED.incrementAndGet();
    }
}
