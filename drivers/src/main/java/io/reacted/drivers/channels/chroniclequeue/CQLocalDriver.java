/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class CQLocalDriver extends LocalDriver<CQLocalDriverConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CQLocalDriver.class);
    private final ThreadLocal<CQSerializer> localSerializer = ThreadLocal.withInitial(CQSerializer::new);
    @Nullable
    private ChronicleQueue chronicle;
    private final ThreadLocal<ExcerptAppender> localAppender = ThreadLocal.withInitial(() -> chronicle.createAppender());
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
            CQSerializer serializer = localSerializer.get();
            try(var ctx =  localAppender.get().acquireWritingDocument(false)) {
                serializer.setSerializerOutput(Objects.requireNonNull(ctx.wire()));
                writeMessage(serializer, source, destination, seqNum, reActorSystemId, ackingPolicy, message);
            }
            return DeliveryStatus.SENT;
        } catch (Exception anyException) {
            LOGGER.error("ERROR SENDING MESSAGE:", anyException);
            getLocalReActorSystem().logError("Unable to send message {}", message, anyException);
            return DeliveryStatus.NOT_SENT;
        }
    }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        Optional.ofNullable(localAppender.get()).ifPresent(ExcerptAppender::close);
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    private void chronicleMainLoop(ExcerptTailer tailer) {
        var waitForNextMsg = Pauser.balanced();
        var deserializer = new CQDeserializer();
        while (!Thread.currentThread().isInterrupted()) {
            try(var ctx = tailer.readingDocument(false)) {
                if (!ctx.isPresent()) {
                    waitForNextMsg.pause();
                } else {
                    deserializer.setDeserializerInput(Objects.requireNonNull(ctx.wire()));
                    readMessage(deserializer);
                    waitForNextMsg.reset();
                }
            }
            catch (Exception anyException) {
                LOGGER.error("Unable to decode data", anyException);
            }
        }
    }

    private void readMessage(Deserializer in) {
        offerMessage(readReActorRef(in),
                     readReActorRef(in),
                     in.getLong(),
                     readReActorSystemId(in),
                     in.getEnum(AckingPolicy.class),
                     in.getObject());
    }
    public static <PayloadT extends ReActedMessage>
    void writeMessage(Serializer out, ReActorRef source, ReActorRef destination, long seqNum,
                      ReActorSystemId localReActorSystemId, AckingPolicy ackingPolicy, PayloadT payload) {
        source.encode(out);
        destination.encode(out);
        out.put(seqNum);
        localReActorSystemId.encode(out);
        out.putEnum(ackingPolicy);
        payload.encode(out);
    }

    public static ReActorRef readReActorRef(Deserializer in) {
        var ref = new ReActorRef();
        ref.decode(in);
        ref.setHashCode(Objects.hash(ref.getReActorId(), ref.getReActorSystemRef()));
        return ref;
    }
    public static ReActorSystemId readReActorSystemId(Deserializer in) {
        ReActorSystemId reActorSystemId = new ReActorSystemId();
        reActorSystemId.decode(in);
        return reActorSystemId;
    }
}
