/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.runtime;

import io.reacted.core.ReactorHelper;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.TypedSubscriptionPolicy;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.mock;

class DispatcherTest {
    static Dispatcher dispatcher;
    static DispatcherConfig dispatcherConfig;
    static ReActorContext reActorContext;
    static BasicMbox actorMbox;
    static ReActorRef parentReactor;
    static ReActorRef reactor;

    @BeforeAll
    static void prepareDispatcherWithConfig() {
        dispatcherConfig = DispatcherConfig.newBuilder()
                .setDispatcherName("dispatcher")
                .setBatchSize(2)
                .setDispatcherThreadsNum(2)
                .build();

        dispatcher = new Dispatcher(dispatcherConfig);

        actorMbox = new BasicMbox();
        parentReactor = ReactorHelper.generateReactor("parentReactor");
        reactor = ReactorHelper.generateReactor("reactor");
        reActorContext = ReActorContext.newBuilder()
                                       .setDispatcher(dispatcher)
                                       .setMbox(ctx -> actorMbox)
                                       .setParentActor(parentReactor)
                                       .setReActorSystem(mock(ReActorSystem.class))
                                       .setReactorRef(reactor)
                                       .setInterceptRules(TypedSubscriptionPolicy.LOCAL.forType(String.class))
                                       .setReActions(ReActions.NO_REACTIONS)
                                       .build();
    }

    @BeforeEach
    void startDispatcher() {
        dispatcher.initDispatcher(ReActorRef.NO_REACTOR_REF, false);
    }
}