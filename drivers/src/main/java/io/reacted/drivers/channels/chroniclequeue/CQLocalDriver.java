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
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class CQLocalDriver extends LocalDriver<CQDriverConfig> {
    private static final Logger LOGGER = Logger.getLogger(CQLocalDriver.class);
    @Nullable
    private ChronicleQueue chronicle;
    @Nullable
    private ExcerptTailer cqTailer;

    public CQLocalDriver(CQDriverConfig driverConfig) {
        super(driverConfig);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) {
        this.chronicle = ChronicleQueue.singleBuilder(getDriverConfig().getChronicleFilesDir())
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
    public Properties getChannelProperties() { return getDriverConfig().getProperties(); }

    @Override
    public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
        return CompletableFuture.completedFuture(Try.of(() -> sendMessage(destination, message)));
    }

    @Override
    public DeliveryStatus sendMessage(ReActorContext destination, Message message) {
        return sendMessage(message);
    }

    @Override
    public boolean channelRequiresDeliveryAck() { return getDriverConfig().isDeliveryAckRequiredByChannel(); }

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> Objects.requireNonNull(chronicle).close()));
    }

    private void chronicleMainLoop(ExcerptTailer tailer) {
        var waitForNextMsg = Pauser.millis(10, 500);

        while(!Thread.currentThread().isInterrupted()) {

            Message newMessage = null;
            try(DocumentContext docCtx = tailer.readingDocument()) {
                if(docCtx.isPresent()) {
                    newMessage = docCtx.wire().read(getDriverConfig().getTopic())
                                       .object(Message.class);
                }
            } catch (Exception anyException) {
                LOGGER.error("Unable to decode data", anyException);
            }

            if (newMessage == null) {
                waitForNextMsg.pause();
                waitForNextMsg.reset();
                continue;
            }
            offerMessage(newMessage);
        }
    }
    private DeliveryStatus sendMessage(Message message) {
        try {
            Objects.requireNonNull(Objects.requireNonNull(chronicle)
                                          .acquireAppender())
                   .writeMessage(getDriverConfig().getTopic(), message);
            return DeliveryStatus.DELIVERED;
        } catch (Exception anyException) {
            throw new DeliveryException(anyException);
        }
    }
}
