/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.DriverCtx;
import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class CQLocalDriver extends LocalDriver<CQLocalDriverConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLocalDriver.class);
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ExcerptTailer cqTailer;

    public CQLocalDriver(CQLocalDriverConfig driverConfig) {
        super(driverConfig);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        this.chronicle = ChronicleQueue.singleBuilder(getDriverConfig().getChronicleFilesDir())
                                       .rollCycle(RollCycles.MINUTELY)
                                       .build();
        this.cqTailer = chronicle.createTailer().toEnd();
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> chronicleMainLoop(Objects.requireNonNull(cqTailer));
    }

    @Override
    public ChannelId getChannelId() {
        return ChannelId.ChannelType.LOCAL_CHRONICLE_QUEUE.forChannelName(getDriverConfig().getChannelName());
    }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getChannelProperties(); }

    @Override
    public <PayloadT extends ReActedMessage>
    CompletionStage<DeliveryStatus> sendAsyncMessage(ReActorRef source, ReActorContext destinationCtx,
                                                     ReActorRef destination, long seqNum,
                                                     ReActorSystemId reActorSystemId,
                                                     AckingPolicy ackingPolicy, PayloadT message) {
        if (!ackingPolicy.isAckRequired()) {
            return super.sendAsyncMessage(source, destinationCtx, destination, seqNum, reActorSystemId,
                                          ackingPolicy, message);
        }
        CompletionStage<DeliveryStatus> pendingAck = newPendingAckTrigger(seqNum);

        DeliveryStatus localDeliveryStatus = sendMessage(source, destinationCtx, destination, seqNum,
                                                         reActorSystemId, ackingPolicy, message);
        if (localDeliveryStatus == DeliveryStatus.SENT) {
            return pendingAck;
        }
        removePendingAckTrigger(seqNum);
        return DELIVERY_RESULT_CACHE[localDeliveryStatus.ordinal()];
    }

    @Override
    public <PayloadT extends ReActedMessage> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination, long seqNum,
                ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        try {
            chronicle.acquireAppender()
                     .writeDocument(w -> writeMessage(w, source, destination, seqNum, reActorSystemId,
                                                      ackingPolicy, message));
            return DeliveryStatus.SENT;
        } catch (Exception anyException) {
            getLocalReActorSystem().logError("Unable to send message {}", message, anyException);
            return DeliveryStatus.NOT_SENT;
        }
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    private void chronicleMainLoop(ExcerptTailer tailer) {
        var waitForNextMsg = Pauser.balanced();
        DriverCtx ctx = Objects.requireNonNull(ReActorSystemDriver.getDriverCtx());
        while(!Thread.currentThread().isInterrupted()) {
            try {
                if (tailer.readDocument(document -> readMessage(document, ctx))) {
                    waitForNextMsg.reset();
                } else {
                    waitForNextMsg.pause();
                }
            } catch (Exception anyException) {
                LOGGER.error("Unable to decode data", anyException);
            }
        }
    }

    private void readMessage(WireIn in, DriverCtx driverCtx) {
        in.read("M").marshallable(m -> offerMessage(readReActorRef(in, driverCtx),
                                                             readReActorRef(in, driverCtx),
                                                             readSequenceNumber(in),
                                                             readReActorSystemId(in),
                                                             readAckingPolicy(in),
                                                             readPayload(in)));
    }
    public static <PayloadT extends ReActedMessage>
    void writeMessage(WireOut out, ReActorRef source, ReActorRef destination, long seqNum,
                      ReActorSystemId localReActorSystemId, AckingPolicy ackingPolicy, PayloadT payload) {
        out.write("M")
           .marshallable(m -> writePayload(writeAckingPolicy(writeReActorSystemId(writeSequenceNumber(writeReActorRef(writeReActorRef(m, source),
                                                                                                                      destination),
                                                                                                      seqNum),
                                                                                  localReActorSystemId),
                                                             ackingPolicy),
                                           payload));
    }
    public static WireOut writeReActorRef(WireOut out, ReActorRef reActorRef) {
        return writeReActorSystemRef(writeReActorId(out, reActorRef.getReActorId()),
                                     reActorRef.getReActorSystemRef());
    }

    public static ReActorRef readReActorRef(WireIn in, DriverCtx driverCtx) {
        ReActorId reActorId = readReActorId(in);
        ReActorSystemRef reActorSystemRef = readReActorSystemRef(in, driverCtx);
        return new ReActorRef(reActorId, reActorSystemRef)
                .setHashCode(Objects.hash(reActorId, reActorSystemRef));
    }

    public static WireOut writeReActorSystemRef(WireOut out, ReActorSystemRef reActorSystemRef) {
        return writeChannelId(writeReActorSystemId(out, reActorSystemRef.getReActorSystemId()),
                              reActorSystemRef.getChannelId());
    }

    public static ReActorSystemRef readReActorSystemRef(WireIn in, DriverCtx ctx) {
        ReActorSystemRef reActorSystemRef = new ReActorSystemRef();
        ReActorSystemId reActorSystemId = readReActorSystemId(in);
        ChannelId channelId = readChannelId(in);
        ReActorSystemRef.setGateForReActorSystem(reActorSystemRef, reActorSystemId, channelId, ctx);
        return reActorSystemRef;
    }
    public static <PayloadT extends ReActedMessage> WireOut writePayload(WireOut wireOut, PayloadT payloadT) {
        return wireOut.write().object(payloadT);
    }

    @SuppressWarnings("unchecked")
    public static <PayloadT extends ReActedMessage> PayloadT readPayload(WireIn in) {
        return (PayloadT)in.read().object();
    }
    public static WireOut writeSequenceNumber(WireOut out, long sequenceNumber) {
        return out.write().int64(sequenceNumber);
    }

    public static long readSequenceNumber(WireIn in) {
        return in.read().int64();
    }
    public static WireOut writeReActorSystemId(WireOut out, ReActorSystemId reActorSystemId) {
        return reActorSystemId == ReActorSystemId.NO_REACTORSYSTEM_ID
               ? out.write().int8(ReActorSystemId.NO_REACTORSYSTEM_ID_MARKER)
               : out.write().int8(ReActorSystemId.COMMON_REACTORSYSTEM_ID_MARKER)
                    .write().writeString(reActorSystemId.getReActorSystemName());
    }

    public static ReActorSystemId readReActorSystemId(WireIn in) {
        if (in.read().int8() == ReActorSystemId.NO_REACTORSYSTEM_ID_MARKER) {
            return ReActorSystemId.NO_REACTORSYSTEM_ID;
        }
        return new ReActorSystemId(in.read().readString());
    }
    public static WireOut writeChannelId(WireOut out, ChannelId channelId) {
        return out.write().int8(channelId.getChannelType().ordinal())
                  .write().writeString(channelId.getChannelName());
    }

    public static ChannelId readChannelId(WireIn in) {
        return ChannelId.ChannelType.forOrdinal(in.read().int8())
                                    .forChannelName(in.read().readString());
    }

    public static WireOut writeAckingPolicy(WireOut out, AckingPolicy ackingPolicy) {
        return out.write().int8(ackingPolicy.ordinal());
    }
    public static AckingPolicy readAckingPolicy(WireIn in) {
        return AckingPolicy.forOrdinal(in.read().int8());
    }
    public static WireOut writeReActorId(WireOut out, ReActorId reActorId) {
        return reActorId == ReActorId.NO_REACTOR_ID
               ? out.write().int8(ReActorId.NO_REACTOR_ID_MARKER)
               : writeUUID(out.write().int8(ReActorId.COMMON_REACTOR_ID_MARKER)
                              .write().writeString(reActorId.getReActorName()), reActorId.getReActorUUID());
    }

    public static ReActorId readReActorId(WireIn in) {
        if (in.read().int8() == ReActorId.NO_REACTOR_ID_MARKER) {
            return ReActorId.NO_REACTOR_ID;
        }
        String reactorName = in.read().readString();
        UUID uuid = readUUID(in);
        return new ReActorId().setReActorName(reactorName)
                              .setReActorUUID(uuid)
                              .setHashCode(Objects.hash(uuid, reactorName));

    }

    public static WireOut writeUUID(WireOut out, UUID uuid) {
        return out.write().int64(uuid.getLeastSignificantBits())
                  .write().int64(uuid.getMostSignificantBits());
    }

    public static UUID readUUID(WireIn in) {
        long least = in.read().int64();
        long most = in.read().int64();
        return ReActorId.NO_REACTOR_ID_UUID.getLeastSignificantBits() == least &&
               ReActorId.NO_REACTOR_ID_UUID.getMostSignificantBits() == most
               ? ReActorId.NO_REACTOR_ID_UUID
               : new UUID(most, least);
    }
}
