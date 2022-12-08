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
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriverConfig;
import io.reacted.drivers.channels.replay.ReplayLocalDriver;
import io.reacted.examples.ExampleUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class SystemReplayApp {
    //NOTE: you may need to provide --illegal-access=permit --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
    //as jvm option
    public static void main(String[] args) throws InterruptedException {
        String dumpDirectory = args.length == 0 || Strings.isNullOrEmpty(args[0]) ? "/tmp/cq" : args[0];
        var dumpingLocalDriverCfg = CQLocalDriverConfig.newBuilder()
                                                       .setChronicleFilesDir(dumpDirectory)
                                                       .setChannelName("ReplayableChannel")
                                                       .build();
        var dumpingLocalDriver = new CQLocalDriver(dumpingLocalDriverCfg);

        var recordedReactorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayApp.class.getSimpleName(),
                                                                          dumpingLocalDriver,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                        .initReActorSystem();

        //Every message sent within the reactor system is going to be saved now
        var echoReActions = ReActions.newBuilder()
                                     .reAct((ctx, payload) -> System.out.printf("Received %s from %s @ %s%n",
                                                                                payload.toString(),
                                                                                ctx.getSender().getReActorId().getReActorName(),
                                                                                ctx.getReActorSystem().getSystemConfig()
                                                                                   .getReActorSystemName()))
                                     .build();
        var echoReActorConfig = ReActorConfig.newBuilder()
                                             .setReActorName("EchoReActor")
                                             .setDispatcherName("FlowDispatcher")
                                             .setMailBoxProvider(ctx -> new UnboundedMbox())
                                             .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                             .build();


        var echoReference = recordedReactorSystem.spawn(echoReActions, echoReActorConfig)
                                                 .orElseSneakyThrow();

        if (IntStream.range(0, 5)
                     .mapToObj(cycle -> "Message number " + cycle)
                     .map(echoReference::apublish)
                     .map(CompletionStage::toCompletableFuture)
                     .map(CompletableFuture::join)
                     .anyMatch(DeliveryStatus::isNotDelivered)) {
            System.err.println("Message not delivered, exiting");
            System.exit(1);
        }

        TimeUnit.SECONDS.sleep(1);

        recordedReactorSystem.shutDown();
        TimeUnit.SECONDS.sleep(1);

        //Everything has been recorded, now let's try to replay it
        System.out.println("Replay engine setup");
        var replayDriverCfg = new ReplayLocalDriver(dumpingLocalDriverCfg);
        var replayedReActorSystem =
                new ReActorSystem(ExampleUtils.getDefaultReActorSystemCfg(SystemReplayApp.class.getSimpleName(),
                                                                          replayDriverCfg,
                                                                          ExampleUtils.NO_SERVICE_REGISTRIES,
                                                                          ExampleUtils.NO_REMOTING_DRIVERS))
                        .initReActorSystem();
        System.out.println("Replay Start!");
        //Once the reactor will be created, the system will notify that and will begin its replay
        replayedReActorSystem.spawn(echoReActions, echoReActorConfig).orElseSneakyThrow();
        TimeUnit.SECONDS.sleep(1);
        replayedReActorSystem.shutDown();
    }
}
