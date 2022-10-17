/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.replay;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.messages.reactors.EventExecutionAttempt;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

@NonNullByDefault
public class ReplayLocalDriver extends LocalDriver<CQDriverConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayLocalDriver.class);
    private final Set<ReActorId> spawnedReActors;
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ReActorSystem replayedActorSystem;

    public ReplayLocalDriver(CQDriverConfig driverConfig) {
        super(driverConfig);
        this.spawnedReActors = ConcurrentHashMap.newKeySet();
    }

    @Override
    public void initDriverLoop(ReActorSystem replayedActorSystem) {
        this.chronicle = ChronicleQueue.singleBuilder(getDriverConfig().getChronicleFilesDir()).build();
        this.replayedActorSystem = replayedActorSystem;
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> replayerMainLoop(Objects.requireNonNull(replayedActorSystem), Objects.requireNonNull(chronicle));
    }

    @Override
    public ChannelId getChannelId() {
        return ChannelId.ChannelType.REPLAY_CHRONICLE_QUEUE.forChannelName(getDriverConfig().getChannelName());
    }

    @Override
    public Properties getChannelProperties() { return new Properties(); }

    @Override
    public DeliveryStatus sendMessage(ReActorContext destination, Message message) {
        if (!(message.getPayload() instanceof DeliveryStatusUpdate)) {
            spawnedReActors.add(message.getDestination().getReActorId());
            spawnedReActors.add(message.getSender().getReActorId());
        }
        return DeliveryStatus.SENT;
    }

    @Override
    public CompletionStage<DeliveryStatus> sendAsyncMessage(ReActorContext destination, Message message) {
        return CompletableFuture.completedFuture(sendMessage(destination, message));
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    private void replayerMainLoop(ReActorSystem localReActorSystem, ChronicleQueue chronicle) {
        ExcerptTailer chronicleReader = chronicle.createTailer();
        Map<ReActorId, Map<Long, Message>> dstToMessageBySeqNum = new HashMap<>();
        Pauser pauser = Pauser.millis(1000, 2000);

        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {

            var nextMessageAttempt = Try.withResources(chronicleReader::readingDocument, ReplayLocalDriver::getNextMessage);
            var nextMessage = nextMessageAttempt.orElse(Optional.empty(),
                                                        error -> LOGGER.error("Error reading message from CQ", error))
                                                .orElse(null);
            if (nextMessage == null) {
                pauser.pause();
                continue;
            }
            pauser.reset();
            if (!isForLocalReActorSystem(localReActorSystem, nextMessage)) {
                continue;
            }
            Serializable payload = nextMessage.getPayload();

            if (payload instanceof EventExecutionAttempt executionAttempt) {
                while (!isTargetReactorAlreadySpawned(nextMessage)) {
                    pauser.pause();
                }

                var message = dstToMessageBySeqNum.getOrDefault(executionAttempt.getReActorId(),
                                                                Collections.emptyMap())
                                                  .remove(executionAttempt.getMsgSeqNum());
                ReActorContext destinationCtx = localReActorSystem.getReActorCtx(executionAttempt.getReActorId());

                if (destinationCtx == null || message == null) {
                    LOGGER.error("Unable to delivery message {} for ReActor {}",
                                 message, executionAttempt.getReActorId(), new IllegalStateException());
                } else {
                    if ( !syncForwardMessageToLocalActor(destinationCtx, message).isDelivered()) {
                         LOGGER.error("Unable to delivery message {} for ReActor {}",
                                     message, executionAttempt.getReActorId());
                    }
                }
            } else {
                dstToMessageBySeqNum.computeIfAbsent(nextMessage.getDestination().getReActorId(),
                                                     reActorId -> new HashMap<>())
                                    .put(nextMessage.getSequenceNumber(), nextMessage);
            }
        }
    }

    private static Optional<Message> getNextMessage(DocumentContext documentCtx) {
        //noinspection ConstantConditions
        return Optional.ofNullable(documentCtx.isPresent()
                                   ? documentCtx.wire().read().object(Message.class)
                                   : null);
    }

    private boolean isForLocalReActorSystem(ReActorSystem replayedAs, Message newMessage) {
        return newMessage.getDestination().getReActorSystemRef().equals(replayedAs.getLoopback());
    }

    private boolean isTargetReactorAlreadySpawned(Message newMessage) {
        //Every spawned reactor receives an init message, so there must be a send for it
        return spawnedReActors.contains(newMessage.getSender().getReActorId());
    }
}
