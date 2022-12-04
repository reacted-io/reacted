/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.DirectCommunicationLoggerConfig;
import io.reacted.core.drivers.system.LocalDriver;
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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
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
        this.channelId = ChannelId.ChannelType.DIRECT_COMMUNICATION.forChannelName(config.getChannelName());
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
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Properties getChannelProperties() { return new Properties(); }

    @Override
    public <PayloadT extends Serializable> DeliveryStatus
    sendMessage(ReActorRef source, ReActorContext destinationCtx, ReActorRef destination, long seqNum,
                ReActorSystemId reActorSystemId, AckingPolicy ackingPolicy, PayloadT message) {
        logFile.println(message);
        logFile.flush();
        return SystemLocalDrivers.DIRECT_COMMUNICATION.sendMessage(source, destinationCtx, destination,
                                                                   seqNum, reActorSystemId, ackingPolicy, message);
    }
}
