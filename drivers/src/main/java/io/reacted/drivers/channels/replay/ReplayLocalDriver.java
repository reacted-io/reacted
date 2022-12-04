/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.replay;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.messages.reactors.EventExecutionAttempt;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.drivers.channels.chroniclequeue.CQDriverConfig;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriver;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public <PayloadT extends Serializable> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                long seqNum, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        if (!(message instanceof DeliveryStatusUpdate)) {
            spawnedReActors.add(destination.getReActorId());
            spawnedReActors.add(source.getReActorId());
        }
        return DeliveryStatus.SENT;
    }

    @Override
    public <PayloadT extends Serializable>
    CompletionStage<DeliveryStatus> sendAsyncMessage(ReActorRef source, ReActorContext destinationCtx,
                                                     ReActorRef destination, long seqNum,
                                                     ReActorSystemId reActorSystemId,
                                                     AckingPolicy ackingPolicy, PayloadT message) {
        return CompletableFuture.completedFuture(sendMessage(source, destinationCtx, destination,
                                                             seqNum, reActorSystemId, ackingPolicy, message));
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    private void replayerMainLoop(ReActorSystem localReActorSystem, ChronicleQueue chronicle) {
        Map<Long, Message> emptyMap = new HashMap<>();
        ExcerptTailer chronicleReader = chronicle.createTailer();
        Map<ReActorId, Map<Long, Message>> dstToMessageBySeqNum = new HashMap<>();
        DriverCtx ctx = ReActorSystemDriver.getDriverCtx();
        Consumer<Message> offerForReplay = newMessage -> onNewMessage(localReActorSystem,
                                                                      emptyMap,
                                                                      dstToMessageBySeqNum, newMessage);
        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {
            chronicleReader.readDocument(in -> CQLocalDriver.readMessage(in, ctx, offerForReplay));
            /*
            try(DocumentContext documentContext = chronicleReader.readingDocument()) {

                if (!documentContext.isPresent()) {
                    pauser.pause();
                    continue;
                }
                var nextMessage = documentContext.wire().read().object(Message.class);

                if (nextMessage == null) {
                    pauser.pause();
                    continue;
                }
                pauser.reset();
                */
                /*
            } catch (Exception anyError) {
                LOGGER.error("Error reading message from CQ", anyError);
                break;
            } */
        }
    }

    private void onNewMessage(ReActorSystem localReActorSystem, Map<Long, Message> emptyMap, Map<ReActorId,
                              Map<Long, Message>> dstToMessageBySeqNum, Message nextMessage) {
        Pauser pauser = Pauser.balanced();

        if (!isForLocalReActorSystem(localReActorSystem, nextMessage)) {
            return;
        }
        Serializable payload = nextMessage.getPayload();

        if (!(payload instanceof EventExecutionAttempt executionAttempt)) {
            dstToMessageBySeqNum.computeIfAbsent(nextMessage.getDestination().getReActorId(),
                                                 reActorId -> new HashMap<>())
                                .put(nextMessage.getSequenceNumber(), nextMessage);
            return;
        }
        while (!isTargetReactorAlreadySpawned(nextMessage)) {
            pauser.pause();
        }
        pauser.reset();

        var message = dstToMessageBySeqNum.getOrDefault(executionAttempt.getReActorId(), emptyMap)
                                          .remove(executionAttempt.getMsgSeqNum());
        ReActorContext destinationCtx = localReActorSystem.getReActorCtx(executionAttempt.getReActorId());

        if (destinationCtx == null || message == null) {
            LOGGER.error("Unable to delivery message {} for ReActor {}",
                    message, executionAttempt.getReActorId(), new IllegalStateException());
        } else if (syncForwardMessageToLocalActor(destinationCtx, message).isNotDelivered()) {
                LOGGER.error("Unable to delivery message {} for ReActor {}",
                        message, executionAttempt.getReActorId());
        }
    }

    private boolean isForLocalReActorSystem(ReActorSystem replayedAs, Message newMessage) {
        return newMessage.getDestination().getReActorSystemRef().equals(replayedAs.getLoopback());
    }

    private boolean isTargetReactorAlreadySpawned(Message newMessage) {
        //Every spawned reactor receives an init message, so there must be a send for it
        return spawnedReActors.contains(newMessage.getSender().getReActorId());
    }
}
