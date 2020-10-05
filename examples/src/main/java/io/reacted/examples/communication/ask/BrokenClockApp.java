/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.ask;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.BoundedBasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.examples.ExampleUtils;

import java.time.Duration;
import java.time.Instant;

import static io.reacted.core.config.reactors.SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS;

public class BrokenClockApp {
    public static void main(String[] args) {
        var reActorSystem = ExampleUtils.getDefaultInitedReActorSystem(BrokenClockApp.class.getSimpleName());

        var reactiveClockConfig = ReActorConfig.newBuilder()
                                               .setTypedSniffSubscriptions(NO_SUBSCRIPTIONS)
                                               //Accept at maximum 5 messages in the mailbox at the same time,
                                               //drop the ones in excess causing the delivery to fail
                                               .setMailBoxProvider(ctx -> new BoundedBasicMbox(5))
                                               .setReActorName("Reactive Clock")
                                               .setDispatcherName(ReActorSystem.DEFAULT_DISPATCHER_NAME)
                                               .build();
        //We did not set any reaction, this clock is not going to reply to anything
        var brokenReactiveClock = reActorSystem.spawn(ReActions.NO_REACTIONS, reactiveClockConfig)
                                               .orElseSneakyThrow();
        //Note: we do not need anoter reactor to intercept the answer
        brokenReactiveClock.ask(new TimeRequest(), Instant.class, Duration.ofSeconds(2), "What's the time?")
                           .thenApply(timeReply -> timeReply.map(instant -> "Wow, unexpected")
                                                            .orElse("Clock did not reply as expected"))
                           .thenAccept(System.out::println)
                           .thenAccept(nullValue -> reActorSystem.shutDown());
    }
}
