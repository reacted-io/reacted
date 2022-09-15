/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.drivers.channels.chroniclequeue;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireKey;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

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
    public boolean channelRequiresDeliveryAck() { return getDriverConfig().isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getProperties(); }

    @Override
    public DeliveryStatus sendMessage(ReActorContext destination, Message message) {
        return sendMessage(Objects.requireNonNull(chronicle)
                                  .acquireAppender(), getDriverConfig().getTopic(), message);
    }

    private void cqRemoteDriverMainLoop(ExcerptTailer cqTailer, ChronicleQueue chronicle) {
        Pauser readPauser = Pauser.millis(100, 500);

        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {

            Message newMessage = null;
            try(DocumentContext docCtx = cqTailer.readingDocument()) {
                if (docCtx.isPresent()) {
                    newMessage = docCtx.wire().read(getDriverConfig().getTopic())
                                       .object(Message.class);
                }
            } catch (Exception anyException) {
                getLocalReActorSystem().logError("Unable to properly decode message", anyException);
            }

            if (newMessage == null) {
                readPauser.pause();
                readPauser.reset();
                continue;
            }

            offerMessage(newMessage);
        }
    }
    private static DeliveryStatus sendMessage(ExcerptAppender cqAppender, WireKey topic,  Message message) {
        try {
            cqAppender.writeMessage(topic, message);
            return DeliveryStatus.DELIVERED;
        } catch (Exception anyException) {
            throw new DeliveryException(anyException);
        }
    }
}
