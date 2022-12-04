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
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.io.Serializable;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireKey;

@NonNullByDefault
public class CQRemoteDriver extends RemotingDriver<CQDriverConfig> {
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ExcerptTailer cqTailer;

    public CQRemoteDriver(CQDriverConfig driverConfig) {
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
    public <PayloadT extends Serializable>
    DeliveryStatus sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination,
                               long seqNum, ReActorSystemId reActorSystemId,
                               AckingPolicy ackingPolicy, PayloadT message) {
        return sendMessage(getLocalReActorSystem(),
                           Objects.requireNonNull(chronicle).acquireAppender(),
                           source, destination, seqNum, ackingPolicy, message);
    }

    private void cqRemoteDriverMainLoop(ExcerptTailer cqTailer, ChronicleQueue chronicle) {
        Pauser readPauser = Pauser.balanced();
        DriverCtx ctx = ReActorSystemDriver.getDriverCtx();
        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {
            cqTailer.readDocument(document -> CQLocalDriver.readMessage(document, ctx, this::offerMessage));
            /*
            Message newMessage = null;
            try(DocumentContext docCtx = cqTailer.readingDocument()) {
                if (docCtx.isPresent()) {
                    newMessage = docCtx.wire().read(getDriverConfig().getTopic())
                                       .object(Message.class);
                    readPauser.reset();
                }
            } catch (Exception anyException) {
                getLocalReActorSystem().logError("Unable to properly decode message", anyException);
            }

            if (newMessage == null) {
                readPauser.pause();
                continue;
            }

            offerMessage(newMessage);
            */
        }
    }
    private static <PayloadT extends Serializable>
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
}
