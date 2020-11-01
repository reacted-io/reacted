/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.giulio;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.utils.ReActedUtils;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import java.io.Serializable;
@NonNullByDefault
public class GreetService implements ReActor {
    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName("Worker")
                            .build();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class,
                               (raCtx, init) -> raCtx.logInfo("Reactor started {}",
                                                              raCtx.getSelf().getReActorId().getReActorName()))
                        .reAct(GreetRequest.class,
                               (raCtx, request) -> ReActedUtils.ifNotDelivered(raCtx.reply("Hello from " + GreetService.class.getSimpleName()),
                                                                               Throwable::printStackTrace))
                        .build();
    }

    public static class GreetRequest implements Serializable { }
}
