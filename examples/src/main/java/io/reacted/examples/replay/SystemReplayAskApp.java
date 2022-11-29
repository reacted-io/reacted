/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.replay;

import com.google.common.base.Strings;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.drivers.channels.replay.ReplayLocalDriver;
import io.reacted.examples.ExampleUtils;

import java.time.Instant;

public class SystemReplayAskApp {
    //NOTE: you may need to provide --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
    //as jvm option
    public static void main(String[] args) {
        String dumpDirectory = args.length == 0 || Strings.isNullOrEmpty(args[0]) ? "/tmp" : args[0];
        var dumpingLocalDriverCfg = CQDriverConfig.newBuilder()
                                                  .setChronicleFilesDir(dumpDirectory)
                                                  .setTopicName("ReplayTest")
                                                  .setChannelName("ReplayableChannel")
                                                  .build();
        var dumpingLocalDriver = new CQLocalDriver(dumpingLocalDriverCfg);
        var recordedReactorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayAskApp.class.getSimpleName(),
                                                                          dumpingLocalDriver,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                        .initReActorSystem();
        //Every message sent within the reactor system is going to be saved now
        var echoReActions = ReActions.newBuilder()
                                     .reAct((ctx, payload) -> ctx.getSender()
                                                                .publish(ReActorRef.NO_REACTOR_REF,
                                                                         String.format("%s - Received %s from %s @ %s%n",
                                                                                    Instant.now().toString(),
                                                                                    payload.toString(),
                                                                                    ctx.getSender().getReActorId()
                                                                                       .getReActorName(),
                                                                                    ctx.getReActorSystem()
                                                                                       .getSystemConfig()
                                                                                       .getReActorSystemName())))
                                     .build();
        var echoReActorConfig = ReActorConfig.newBuilder()
                                             .setReActorName("EchoReActor")
                                             .setDispatcherName(Dispatcher.DEFAULT_DISPATCHER_NAME)
                                             .setMailBoxProvider(ctx -> new BasicMbox())
                                             .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                             .build();

        var echoReference = recordedReactorSystem.spawn(echoReActions, echoReActorConfig)
                                                 .orElseSneakyThrow();

        echoReference.ask("I am an ask", String.class, "AskRequest")
                     .thenAccept(System.out::println)
                     .toCompletableFuture()
                     .join();
        recordedReactorSystem.shutDown();
        //Everything has been recorded, now let's try to replay it
        System.out.println("Replay start!");
        var replayDriverCfg = new ReplayLocalDriver(dumpingLocalDriverCfg);
        var replayedReActorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayAskApp.class.getSimpleName(),
                                                                          replayDriverCfg,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                .initReActorSystem();
        //Once the reactor will be created, the system will notify that and will begin its replay
        echoReference = replayedReActorSystem.spawn(echoReActions, echoReActorConfig).orElseSneakyThrow();
        echoReference.ask("I am an ask", String.class, "AskRequest")
                     .thenAccept(System.out::println)
                     .toCompletableFuture()
                     .join();
        replayedReActorSystem.shutDown();
    }
}
