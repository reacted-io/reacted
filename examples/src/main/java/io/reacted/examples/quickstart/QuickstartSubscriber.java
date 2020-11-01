/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.quickstart;

import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.TypedSubscription;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.kafka.KafkaDriver;
import io.reacted.drivers.channels.kafka.KafkaDriverConfig;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class QuickstartSubscriber {
    public static void main(String[] args) {
        var kafkaDriverConfig = KafkaDriverConfig.newBuilder()
                                                 .setChannelName("KafkaQuickstartChannel")
                                                 .setTopic("ReactedTopic")
                                                 .setGroupId("QuickstartGroupSubscriber")
                                                 .setMaxPollRecords(1)
                                                 .setBootstrapEndpoint("localhost:9092")
                                                 .build();
        var showOffSubscriberSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                           .addRemotingDriver(new KafkaDriver(kafkaDriverConfig))
                                                                           .setReactorSystemName("ShowOffSubscriberReActorSystemName")
                                                                           .build()).initReActorSystem();
        showOffSubscriberSystem.spawn(ReActions.newBuilder()
                                               .reAct(GreeterService.GreetingsRequest.class,
                                                      (raCtx, greetingsRequest) ->
                                                       raCtx.logInfo("{} asked for a greeting",
                                                                     raCtx.getSender().getReActorId()
                                                                                      .getReActorName()))
                                               .build(),
                                      ReActorConfig.newBuilder()
                                                   .setReActorName("Message Interceptor")
                                                   .setTypedSubscriptions(TypedSubscription.REMOTE.forType(GreeterService.GreetingsRequest.class))
                                                   .build())
                               .ifError(error -> showOffSubscriberSystem.shutDown());
    }
}
