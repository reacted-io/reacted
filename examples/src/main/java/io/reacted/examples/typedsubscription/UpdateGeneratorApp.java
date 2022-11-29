/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.typedsubscription;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.examples.ExampleUtils;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class UpdateGeneratorApp {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        var reActorSystem = ExampleUtils.getDefaultInitedReActorSystem(UpdateGeneratorApp.class.getSimpleName());
        //Create a reactor that subscribes for Updates. Whenever a new Update is received by a reactor within
        //the reactorsystem, the TypeSubscriber reactor will receive a copy of it
        reActorSystem.spawn(new ReActor() {
            @Nonnull
            @Override
            public ReActions getReActions() {
                return ReActions.newBuilder()
                                .reAct(Update.class,
                                       (ctx, update) -> System.out.printf("Updates received %d%n",
                                                                          update.updateId()))
                                .reAct(ReActions::noReAction)
                                .build();
            }

            @Nonnull
            @Override
            public ReActorConfig getConfig() {
                return ReActorConfig.newBuilder()
                                    .setDispatcherName(Dispatcher.DEFAULT_DISPATCHER_NAME)
                                    .setReActorName("PassiveUpdatesListener")
                                    .setMailBoxProvider(ctx -> new UnboundedMbox())
                                    .setTypedSubscriptions(TypedSubscription.LOCAL.forType(Update.class))
                                    .build();
            }
        }).orElseSneakyThrow();
        //We lost the ReActorRef of the listener, we have no way of contacting it directly now
        IntStream.range(0, 1)
                 .mapToObj(Update::new)
                 .forEachOrdered(update -> reActorSystem.broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF,
                                                                                     update));
        TimeUnit.SECONDS.sleep(1);
        //alternatively, we can just send a message to another reactor. The listener will be triggered in the same way
        IntStream.range(1, 2)
                 .mapToObj(Update::new)
                 .forEachOrdered(update -> reActorSystem.getSystemSink().publish(ReActorRef.NO_REACTOR_REF, update));
        TimeUnit.SECONDS.sleep(1);
        reActorSystem.shutDown();
    }
}
