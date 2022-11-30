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
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.system.LoopbackDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.Try;
import org.awaitility.Awaitility;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReActorSystemTest {
    private final Logger LOGGER = LoggerFactory.getLogger(ReActorSystemTest.class);
    private static final String DISPATCHER_NAME = "TestDispatcher";
    public static final String NO_RE_ACTOR_FOUND = "No ReActor found";
    private ReActorSystem reActorSystem;
    private final ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                             .setMailBoxProvider(ctx -> new UnboundedMbox())
                                                             .setDispatcherName(DISPATCHER_NAME)
                                                             .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                                             .setReActorName("Reactor Name")
                                                             .build();

    private final ReActorConfig childReActorConfig = ReActorConfig.newBuilder()
                                                                  .setMailBoxProvider(ctx -> new UnboundedMbox())
                                                                  .setDispatcherName(DISPATCHER_NAME)
                                                                  .setTypedSubscriptions(TypedSubscription.NO_SUBSCRIPTIONS)
                                                                  .setReActorName("Child reactor name")
                                                                  .build();

    @BeforeEach
    void prepareReactorSystem() {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                                                     .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
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
        Try<ReActorRef> reActorRef = reActorSystem.spawn(ReActions.NO_REACTIONS, reActorConfig);

        Assertions.assertTrue(reActorRef.isSuccess());
        ReActorId reActorId = reActorRef.orElseSneakyThrow().getReActorId();
        Assertions.assertNotNull(reActorSystem.getReActorCtx(reActorId));
        Assertions.assertNotNull(reActorSystem.getReActorCtx(reActorId));
    }

    @Test
    void reactorSystemCanSpawnNewChild() {
        Try<ReActorRef> fatherActor = reActorSystem.spawn(ReActions.NO_REACTIONS, reActorConfig);
        Try<ReActorRef> childReActor = reActorSystem.spawnChild(ReActions.NO_REACTIONS, fatherActor.get(),
                                                                childReActorConfig);
        childReActor.map(ReActorRef::getReActorId)
                    .map(reActorSystem::getReActorCtx)
                    .ifSuccessOrElse(Assertions::assertNotNull, Assertions::fail);

        Optional<ReActorContext> reActor = fatherActor.map(ReActorRef::getReActorId)
                                                      .map(reActorSystem::getReActorCtx)
                                                      .map(Optional::ofNullable)
                                                      .orElseSneakyThrow();
        childReActor.ifSuccessOrElse(child -> reActor.map(ReActorContext::getChildren)
                                                     .filter(children -> children.size() == 1)
                                                     .map(children -> children.iterator().next())
                                                     .ifPresentOrElse(firstChild -> Assertions.assertEquals(firstChild,
                                                                                                            childReActor.get()),
                                                                      Assertions::fail),
                                     Assertions::fail);
    }

    @Test
    void reactorSystemCanSpawnAStoppedReactorHavingTheSameName() {
        long iteration = 0;
        LOGGER.info("Init cycle");
        do {
            if (iteration > 0 && iteration % 100000 == 0) {
                LOGGER.info("Cycle {} {}", iteration, Instant.ofEpochSecond(0, System.nanoTime())
                                                             .toString());
            }
            ReActorRef actor = reActorSystem.spawn(ReActions.NO_REACTIONS, reActorConfig).orElseSneakyThrow();
            reActorSystem.stop(actor.getReActorId())
                         .map(CompletionStage::toCompletableFuture)
                         .ifPresentOrElse(CompletableFuture::join, () -> Assertions.fail(NO_RE_ACTOR_FOUND));
        } while (iteration++ < 5_000_000L);
        LOGGER.info("Cycle completed");
    }

    @Test
    void reactorSystemCanStopChild() {
        ReActorRef fatherActor = reActorSystem.spawn(ReActions.NO_REACTIONS, reActorConfig)
                                              .orElseSneakyThrow();
        ReActorRef childReActor = reActorSystem.spawnChild(ReActions.NO_REACTIONS, fatherActor,
                                                           childReActorConfig)
                                               .orElseSneakyThrow();

        Optional<ReActorContext> fatherCtx = Optional.ofNullable(reActorSystem.getReActorCtx(fatherActor.getReActorId()));

        Set<ReActorRef> children = fatherCtx.map(ReActorContext::getChildren)
                                             .orElse(Set.of());
        Assertions.assertEquals(1, children.size());
        reActorSystem.stop(childReActor.getReActorId())
                     .map(CompletionStage::toCompletableFuture)
                     .ifPresentOrElse(CompletableFuture::join,
                                      () -> Assertions.fail(NO_RE_ACTOR_FOUND));
        Assertions.assertEquals(0, children.size());
    }

    @Test
    void reActorSystemCanBroadcastToListeners() {
        // todo change to use local var
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                                                   .setReActorName("TR")
                                                   .setDispatcherName("TestDispatcher")
                                                   .setTypedSubscriptions(TypedSubscription.LOCAL.forType(Message.class))
                                                   .build();

        reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig));

        reActorSystem.spawn(new MagicTestReActor(1, true, ReActorConfig.fromConfig(reActorConfig)
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
