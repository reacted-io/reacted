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
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.Recyclable;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.DeliveryStatusUpdate;
import io.reacted.core.messages.reactors.EventExecutionAttempt;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.drivers.channels.chroniclequeue.CQDeserializer;
import io.reacted.drivers.channels.chroniclequeue.CQLocalDriverConfig;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readReActorRef;
import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readReActorSystemId;

@NonNullByDefault
public class ReplayLocalDriver extends LocalDriver<CQLocalDriverConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayLocalDriver.class);
    private final Set<ReActorId> spawnedReActors;
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ReActorSystem replayedActorSystem;

    public ReplayLocalDriver(CQLocalDriverConfig driverConfig) {
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
    public <PayloadT extends ReActedMessage> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                long seqNum, ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        if (!(message instanceof DeliveryStatusUpdate)) {
            spawnedReActors.add(destination.getReActorId());
            spawnedReActors.add(source.getReActorId());
        }
        if (message instanceof Recyclable recyclable) {
            recyclable.revalidate();
        }
        return DeliveryStatus.SENT;
    }

    @Override
    public <PayloadT extends ReActedMessage>
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
        Pauser pauser = Pauser.balanced();
        try(DocumentContext documentContext = chronicleReader.readingDocument()) {
            var deserializer = new CQDeserializer();
            ReadMarshallable reader = wireIn -> {
                deserializer.setDeserializerInput(wireIn);
                readMessage(deserializer, localReActorSystem, emptyMap, dstToMessageBySeqNum);
            };
            while (!Thread.currentThread()
                          .isInterrupted() && !chronicle.isClosed()) {
                try {
                    if (chronicleReader.readDocument(reader) &&
                        documentContext.isPresent()) {
                        readMessage(deserializer, localReActorSystem, emptyMap, dstToMessageBySeqNum);
                        pauser.reset();
                    } else {
                        pauser.pause();
                    }
                } catch (Exception anyError) {
                    LOGGER.error("Error reading message from CQ", anyError);
                }
            }
        }
    }

    private <PayloadT extends ReActedMessage>
    void onNewMessage(ReActorSystem localReActorSystem, Map<Long, Message> emptyMap, Map<ReActorId,
                      Map<Long, Message>> dstToMessageBySeqNum, ReActorRef source, ReActorRef destination,
                      long sequenceNumber, ReActorSystemId fromReActorSystemId, AckingPolicy ackingPolicy,
                      PayloadT payload) {
        Pauser pauser = Pauser.balanced();

        if (!isForLocalReActorSystem(localReActorSystem, destination)) {
            return;
        }

        if (!(payload instanceof EventExecutionAttempt executionAttempt)) {
            dstToMessageBySeqNum.computeIfAbsent(destination.getReActorId(), reActorId -> new HashMap<>())
                                .put(sequenceNumber,
                                     Message.of(source, destination, sequenceNumber,
                                                fromReActorSystemId, ackingPolicy, payload));
            return;
        }
        while (!isReactorAlreadySpawned(source)) {
            pauser.pause();
        }
        pauser.reset();

        Message originalMessage = dstToMessageBySeqNum.getOrDefault(executionAttempt.getReActorId(), emptyMap)
                                                      .remove(executionAttempt.getMsgSeqNum());
        ReActorContext destinationCtx = localReActorSystem.getReActorCtx(executionAttempt.getReActorId());

        if (destinationCtx == null || originalMessage == null) {
            LOGGER.error("Unable to delivery message {} for ReActor {}",
                         originalMessage, executionAttempt.getReActorId(), new IllegalStateException());
        } else if (syncForwardMessageToLocalActor(originalMessage.getSender(), destinationCtx,
                                                  originalMessage.getDestination(),
                                                  originalMessage.getSequenceNumber(),
                                                  originalMessage.getCreatingReactorSystemId(),
                                                  originalMessage.getAckingPolicy(),
                                                  originalMessage.getPayload()).isNotDelivered()) {
                LOGGER.error("Unable to delivery message {} for ReActor {}",
                             originalMessage, executionAttempt.getReActorId());
        }
    }

    private boolean isForLocalReActorSystem(ReActorSystem replayedAs, ReActorRef destination) {
        return destination.getReActorSystemRef().equals(replayedAs.getLoopback());
    }

    private boolean isReactorAlreadySpawned(ReActorRef sender) {
        //Every spawned reactor receives an init message, so there must be a send for it
        return spawnedReActors.contains(sender.getReActorId());
    }

    private void readMessage(Deserializer in, ReActorSystem localReActorSystem,
                             Map<Long, Message> emptyMap, Map<ReActorId, Map<Long, Message>> dstToMessageBySeqNum) {
        onNewMessage(localReActorSystem, emptyMap, dstToMessageBySeqNum,
                     readReActorRef(in), readReActorRef(in), in.getLong(), readReActorSystemId(in),
                     in.getEnum(AckingPolicy.class), in.getObject());
    }
}
