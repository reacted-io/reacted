/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.CoreConstants;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.DeadMessage;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DeadLetterTest {

    @Test
    void messagesWithUnknownDestinationAreSentToDeadLetter() throws InterruptedException {
        // Prepare & init ReActorSystem
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                                                                     .setMsgFanOutPoolSize(2)
                                                                     .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                                     .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                          .setDispatcherName(CoreConstants.TEST_DISPATCHER)
                                                                                                          .setBatchSize(1_000)
                                                                                                          .setDispatcherThreadsNum(1)
                                                                                                          .build())
                                                                     .build();
        ReActorSystem reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();

        // Spawn new reactor
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName("TR")
                                                   .setDispatcherName(CoreConstants.TEST_DISPATCHER)
                                                   .setMailBoxProvider(ctx -> new BasicMbox())
                                                   .setTypedSubscriptions(TypedSubscription.LOCAL.forType(DeadMessage.class))
                                                   .build();
        reActorSystem.spawn(new MagicTestReActor(2, true, reActorConfig))
                     .orElseSneakyThrow();
        new ReActorRef(new ReActorId(ReActorId.NO_REACTOR_ID, CoreConstants.REACTOR_NAME),
                       reActorSystem.getLoopback()).publish(ReActorRef.NO_REACTOR_REF, "message");
        TimeUnit.SECONDS.sleep(1);
        Assertions.assertEquals(1, DeadLetter.RECEIVED.get());
    }
}
