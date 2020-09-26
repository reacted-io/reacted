/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.time.ZonedDateTime;

@NonNullByDefault
@Immutable
public class ClockReActor implements ReActor {
    private final String workerDispatcherName;

    ClockReActor(String workerDispatcherName) {
        this.workerDispatcherName = workerDispatcherName;
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(TimeRequest.class,
                               (raCtx, timeRequest) -> raCtx.getSender().tell(raCtx.getParent(), ZonedDateTime.now()))
                        .reAct(ReActions::noReAction)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(ClockReActor.class.getSimpleName())
                            .setDispatcherName(workerDispatcherName)
                            .setMailBoxProvider(BasicMbox::new)
                            .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                            .build();
    }
}
