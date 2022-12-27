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
import io.reacted.core.drivers.system.ReActorSystemDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.AckingPolicy;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.WireIn;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readAckingPolicy;
import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readPayload;
import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readReActorRef;
import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readReActorSystemId;
import static io.reacted.drivers.channels.chroniclequeue.CQLocalDriver.readSequenceNumber;

@NonNullByDefault
public class CQRemoteDriver extends RemotingDriver<CQRemoteDriverConfig> {
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ExcerptTailer cqTailer;

    public CQRemoteDriver(CQRemoteDriverConfig driverConfig) {
        super(driverConfig);
    }

    @Override
    public void initDriverLoop(ReActorSystem reActorSystem) {
        this.chronicle = ChronicleQueue.singleBuilder(getDriverConfig().getChronicleFilesDir()).build();
        this.cqTailer = chronicle.createTailer().toEnd();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CQRemoteDriver that = (CQRemoteDriver) o;
        return Objects.equals(getDriverConfig(), that.getDriverConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDriverConfig());
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() {
        return () -> cqRemoteDriverMainLoop(Objects.requireNonNull(cqTailer), Objects.requireNonNull(chronicle));
    }

    @Override
    public CompletableFuture<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    @Override
    public ChannelId getChannelId() {
        return ChannelId.ChannelType.REMOTING_CHRONICLE_QUEUE.forChannelName(getDriverConfig().getChannelName());
    }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getChannelProperties(); }

    @Override
    public <PayloadT extends ReActedMessage>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long seqNum, ReActorSystemId reActorSystemId,
                               AckingPolicy ackingPolicy, PayloadT message) {
        return sendMessage(getLocalReActorSystem(),
                           Objects.requireNonNull(chronicle).acquireAppender(),
                           source, destination, seqNum, ackingPolicy, message);
    }

    private void cqRemoteDriverMainLoop(ExcerptTailer cqTailer, ChronicleQueue chronicle) {
        Pauser readPauser = Pauser.balanced();
        DriverCtx ctx = Objects.requireNonNull(ReActorSystemDriver.getDriverCtx());
        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {
            try {
                if (cqTailer.readDocument(document -> readMessage(document, ctx))) {
                    readPauser.reset();
                } else {
                    readPauser.pause();
                }
            } catch (Exception anyException) {
                getLocalReActorSystem().logError("Unable to properly decode message", anyException);
            }
        }
    }
    private static <PayloadT extends ReActedMessage>
    DeliveryStatus sendMessage(ReActorSystem localReActorSystem, ExcerptAppender cqAppender,
                               ReActorRef source, ReActorRef destination, long seqNum,
                               AckingPolicy ackingPolicy, PayloadT message) {
        try {
            cqAppender.writeDocument(document -> CQLocalDriver.writeMessage(document, source, destination,
                                                                            seqNum, localReActorSystem.getLocalReActorSystemId(),
                                                                            ackingPolicy, message));
            return DeliveryStatus.SENT;
        } catch (Exception sendError) {
            localReActorSystem.logError("Error sending message {}", message.toString(),
                                        sendError);
            return DeliveryStatus.NOT_SENT;
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
}
