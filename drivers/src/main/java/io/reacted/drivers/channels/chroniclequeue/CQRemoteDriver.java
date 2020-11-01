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
import net.openhft.chronicle.wire.WireKey;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@NonNullByDefault
public class CQRemoteDriver extends RemotingDriver<CQDriverConfig> {
    private static final Logger LOGGER = Logger.getLogger(CQRemoteDriver.class);
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ExcerptTailer cqTailer;
    @Nullable
    private ReActorSystem localReActorSystem;

    public CQRemoteDriver(CQDriverConfig driverConfig) {
        super(driverConfig);
    }

    @Override
    public void initDriverLoop(ReActorSystem reActorSystem) {
        this.chronicle = ChronicleQueue.singleBuilder(getDriverConfig().getChronicleFilesDir()).build();
        this.cqTailer = chronicle.createTailer().toEnd();
        this.localReActorSystem = reActorSystem;
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
        return new ChannelId(ChannelId.ChannelType.REMOTING_CHRONICLE_QUEUE, getDriverConfig().getChannelName());
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return getDriverConfig().isDeliveryAckRequiredByChannel(); }

    @Override
    public Properties getChannelProperties() { return getDriverConfig().getProperties(); }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        return sendMessage(Objects.requireNonNull(chronicle)
                                  .acquireAppender(), getDriverConfig().getTopic(), message);
    }

    private void cqRemoteDriverMainLoop(ExcerptTailer cqTailer, ChronicleQueue chronicle) {
        Pauser readPauser = Pauser.millis(100, 500);

        while (!Thread.currentThread().isInterrupted() && !chronicle.isClosed()) {

            @SuppressWarnings("ConstantConditions")
            var newMessage = Try.withResources(cqTailer::readingDocument,
                                               documentCtx -> documentCtx.isPresent()
                                                              ? documentCtx.wire().read(getDriverConfig().getTopic())
                                                                                  .object(Message.class)
                                                              : null)
                                .orElse(null, error -> LOGGER.error("Unable to properly decode message", error));
            if (newMessage == null) {
                readPauser.pause();
                readPauser.reset();
                continue;
            }

            offerMessage(newMessage);
        }
    }
    private static Try<DeliveryStatus> sendMessage(ExcerptAppender cqAppender, WireKey topic,  Message message) {
        return Try.ofRunnable(() -> cqAppender.writeMessage(topic, message))
                  .map(success -> DeliveryStatus.DELIVERED);
    }
}
