/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.system;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class DirectCommunicationLoggerDriver extends LocalDriver<DirectCommunicationLoggerConfig> {
    private final ChannelId channelId;
    private final PrintWriter logFile;

    /**
     * Creates a direct communication driver that logs in a file all the content of the exchanged messages within
     * the {@link ReActorSystem}
     *
     * @param config driver configuration
     * @throws UncheckedIOException If there is a problem opening the specified file
     */
    public DirectCommunicationLoggerDriver(DirectCommunicationLoggerConfig config) {
        super(config);
        this.channelId =  new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION,
                                        config.getChannelName());
        this.logFile = Try.of(() -> new FileWriter(config.getLogFilePath(), false))
                          .map(PrintWriter::new)
                          .orElseThrow(ioException -> new UncheckedIOException((IOException)ioException));
    }

    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) { return Try.VOID; }

    @Override
    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        return CompletableFuture.completedFuture(Try.VOID);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) { logFile.flush();}

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> { logFile.flush(); logFile.close(); }));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() { return () -> { }; }

    @Override
    public ChannelId getChannelId() { return this.channelId; }

    @Override
    public Properties getChannelProperties() { return new Properties(); }

    @Override
    public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
        logFile.println(message.toString());
        logFile.flush();
        return destination.isStop()
               ? Try.ofSuccess(DeliveryStatus.NOT_DELIVERED)
               : localDeliver(destination, message);
    }

    @Override
    public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
        CompletionStage<Try<DeliveryStatus>> delivery = destination.isStop()
                       ? CompletableFuture.completedFuture(Try.ofSuccess(DeliveryStatus.NOT_DELIVERED))
                       : asyncLocalDeliver(destination, message);
        delivery.thenAccept(deliveryAttempt -> {
            synchronized (logFile) {
                logFile.println(message.toString());
                logFile.flush();
            }
        });
        return delivery;
    }
}
