/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactorsystem;

import io.reacted.core.CoreConstants;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactors.SubscriptionPolicy;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.system.LoopbackDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.patterns.Try;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;

class ReActorSystemTest {
    private static final String DISPATCHER_NAME = "TestDispatcher";
    private ReActorSystem reActorSystem;
    private final ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                             .setMailBoxProvider(BasicMbox::new)
                                                             .setDispatcherName(DISPATCHER_NAME)
                                                             .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                                                             .setReActorName("Reactor Name")
                                                             .build();

    private final ReActorConfig childReActorConfig = ReActorConfig.newBuilder()
                                                                  .setMailBoxProvider(BasicMbox::new)
                                                                  .setDispatcherName(DISPATCHER_NAME)
                                                                  .setTypedSniffSubscriptions(SubscriptionPolicy.SniffSubscription.NO_SUBSCRIPTIONS)
                                                                  .setReActorName("Child reactor name")
                                                                  .build();

    @BeforeEach
    void prepareReactorSystem() {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.RE_ACTED_ACTOR_SYSTEM)
                                                                     .setMsgFanOutPoolSize(2)
                                                                     .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                                                                     .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                          .setDispatcherName("TestDispatcher")
                                                                                                          .setBatchSize(1_000)
                                                                                                          .setDispatcherThreadsNum(1)
                                                                                                          .build())
                                                                     .build();
        reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();
    }

    @AfterEach
    void rampDownReactorSystem() {
        reActorSystem.shutDown();
        MagicTestReActor.RECEIVED.reset();
    }

    @Test
    void reactorSystemHasByDefaultLoopbackDriver() {
        boolean loopbackDriverPresent = false;
        for (ReActorSystemDriver reActorSystemDriver : reActorSystem.getReActorSystemDrivers()) {
            if (reActorSystemDriver.getClass().equals(LoopbackDriver.class)) {
                loopbackDriverPresent = true;
                break;
            }
        }
        Assertions.assertTrue(loopbackDriverPresent);
    }

    @Test
    void reactorSystemCanUnregisterLoopbackDriver() {
//        todo: test with different driver not loopback
//        LoopbackDriver loopbackDriver = new LoopbackDriver(reActorSystem.getSystemConfig().getReActorSystemName(),
//                reActorSystem, reActorSystem.getSystemConfig().getLocalDriver());
//        reActorSystem.registerReActorSystemDriver(loopbackDriver).orElseSneakyThrow();
//
//        reActorSystem.unregisterReActorSystemDriver(reActorSystem.getReActorSystemDrivers().iterator().next());
//        Assertions.assertFalse(reActorSystem.getReActorSystemDrivers().contains(loopbackDriver));
    }

    @Test
    void reactorSystemCanSpawnNewReactor() {
        Try<ReActorRef> reActorRef = reActorSystem.spawn(mock(ReActions.class), reActorConfig);

        Assertions.assertTrue(reActorRef.isSuccess());
        ReActorId reActorId = reActorRef.get().getReActorId();
        Assertions.assertTrue(reActorSystem.getReActor(reActorId).isPresent());
        Assertions.assertFalse(reActorSystem.getReActor(reActorId).isEmpty());
    }

    @Test
    void reactorSystemCanStopReactor() {
        Try<ReActorRef> reActorRef = reActorSystem.spawn(mock(ReActions.class), reActorConfig);

        ReActorId reActorId = reActorRef.get().getReActorId();
        reActorSystem.stopReActor(reActorId);
        Assertions.assertFalse(reActorSystem.getReActor(reActorId).isPresent());
        Assertions.assertTrue(reActorSystem.getReActor(reActorId).isEmpty());
    }

    @Test
    void reactorSystemCanSpawnNewChild() {
        Try<ReActorRef> fatherActor = reActorSystem.spawn(ReActions.NO_REACTIONS, reActorConfig);

        Try<ReActorRef> childReActor = reActorSystem.spawnChild(ReActions.NO_REACTIONS, fatherActor.get(),
                                                                childReActorConfig);
        childReActor.map(ReActorRef::getReActorId)
                    .map(reActorSystem::getReActor)
                    .ifSuccessOrElse(raCtx -> Assertions.assertTrue(raCtx.isPresent()),
                                     Assertions::fail);

        Optional<ReActorContext> reActor = fatherActor.map(ReActorRef::getReActorId)
                                                      .map(reActorSystem::getReActor)
                                                      .orElseSneakyThrow();
        childReActor.ifSuccessOrElse(child -> reActor.map(ReActorContext::getChildren)
                                                     .filter(children -> children.size() == 1)
                                                     .map(children -> children.get(0))
                                                     .ifPresentOrElse(firstChild -> Assertions.assertEquals(firstChild,
                                                                                                            childReActor.get()),
                                                                      Assertions::fail),
                                     Assertions::fail);
    }


    @Test
    void reactorSystemCanStopChild() {
        Try<ReActorRef> fatherActor = reActorSystem.spawn(mock(ReActions.class), reActorConfig);
        Try<ReActorRef> childReActor = reActorSystem.spawnChild(ReActions.NO_REACTIONS, fatherActor.get(),
                                                                childReActorConfig);

        Optional<ReActorContext> reActor = reActorSystem.getReActor(fatherActor.get().getReActorId());

        List<ReActorRef> children = reActor.get().getChildren();
        Assertions.assertEquals(1, children.size());

        reActorSystem.stopReActor(childReActor.get().getReActorId());
        Assertions.assertEquals(0, children.size());
    }

    @Test
    void reActorSystemCanBroadcastToListeners() {
        // todo change to use local var
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName("TR")
                                                   .setDispatcherName("TestDispatcher")
                                                   .setMailBoxProvider(BasicMbox::new)
                                                   .setTypedSniffSubscriptions(SubscriptionPolicy.LOCAL.forType(Message.class))
                                                   .build();

        reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig));

        reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig.toBuilder()
                                                                       .setReActorName("2nd reactor name")
                                                                       .build()));

        Message originalMsg = new Message(ReActorRef.NO_REACTOR_REF, ReActorRef.NO_REACTOR_REF, 0x31337,
                                          reActorSystem.getLocalReActorSystemId(), AckingPolicy.NONE,
                                          CoreConstants.DE_SERIALIZATION_SUCCESSFUL);

        reActorSystem.broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF, originalMsg);

        Awaitility.await()
                  .until(MagicTestReActor.RECEIVED::intValue, CoreMatchers.equalTo(2));
    }

//    @Test
//    void reactorSystemCanRegisterNewGate() {
//        ReActorSystemId reActorSystemId = new ReActorSystemId("reActorSystemId");
//        GateDescriptor gateDescriptor = reActorSystem.registerNewGate(reActorSystemId, new GateDescriptor(new NullReActorSystemRef(), mock(LocalDriver.class)));
//
//        Assertions.assertTrue(Optional.ofNullable(reActorSystem.findGate(reActorSystemId)).isPresent());
//        Assertions.assertEquals(Optional.of(gateDescriptor),
//                reActorSystem.findGate(reActorSystemId).filter(
//                        gateDescript -> gateDescript.equals(gateDescriptor)));
//    }
//
//    @Test
//    void reactorSystemCanRegisterMultipleGates() {
//        ReActorSystemId reActorSystemId = new ReActorSystemId("reActorSystemId");
//        GateDescriptor gateDescriptor1 = reActorSystem.registerNewGate(reActorSystemId, new GateDescriptor(new NullReActorSystemRef(), mock(LocalDriver.class)));
//        GateDescriptor gateDescriptor2 = reActorSystem.registerNewGate(reActorSystemId, new GateDescriptor(new NullReActorSystemRef(), mock(LocalDriver.class)));
//
//        Assertions.assertEquals(2, reActorSystem.getReActorSystemsGates().size());
//        Assertions.assertTrue(reActorSystem.getReActorSystemsGates().containsValue(gateDescriptor1));
//        Assertions.assertTrue(reActorSystem.getReActorSystemsGates().containsValue(gateDescriptor2));
//    }

}