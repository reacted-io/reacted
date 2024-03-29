/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.quickstart;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;

@NonNullByDefault
final class GreeterService implements ReActor {

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
                               (ctx, init) ->
                                       ctx.logInfo("{} was born", ctx.getSelf().getReActorId().getReActorName()))
                        .reAct(GreetingsRequest.class,
                               (ctx, greetingsRequest) ->
                                       ctx.reply(ReActedMessage.of("Hello from " + GreeterService.class.getSimpleName())))
                        .build();
    }
    static final class GreetingsRequest implements ReActedMessage { }
}
