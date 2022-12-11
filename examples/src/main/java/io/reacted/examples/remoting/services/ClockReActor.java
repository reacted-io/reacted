/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.time.ZonedDateTime;

@NonNullByDefault
@Immutable
public class ClockReActor implements ReActor {
    private final String serviceName;

    ClockReActor(String serviceName) {
        this.serviceName = serviceName;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(TimeRequest.class,
                               (ctx, timeRequest) -> ctx.reply(ctx.getParent(), ZonedDateTime.now()))
                        .reAct(ReActions::noReAction)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(serviceName)
                            .setMailBoxProvider(ctx -> new UnboundedMbox())
                            .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }
}
