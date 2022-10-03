/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.ChannelId;
import io.reacted.core.config.drivers.DirectCommunicationSimplifiedLoggerConfig;
import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;

import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class DirectCommunicationSimplifiedLoggerDriver extends
                                                       LocalDriver<DirectCommunicationSimplifiedLoggerConfig> {
    private final ChannelId channelId;
    private final PrintWriter logFile;

    /**
     * A local delivery driver that logs on a file just the main information about a message.
     * This is the simplified/less noisy version of {@link DirectCommunicationLoggerDriver}
     * @param config driver configuration
     * @throws UncheckedIOException if there are problems opening the logfile
     */
    public DirectCommunicationSimplifiedLoggerDriver(DirectCommunicationSimplifiedLoggerConfig config) {
        super(config);
        this.channelId = ChannelId.ChannelType.DIRECT_COMMUNICATION
                                  .forChannelName(getDriverConfig().getChannelName());
        this.logFile = new PrintWriter(config.getPrintStream());
    }

    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) { return Try.VOID; }

    @Override
    public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
        logFile.flush();
        logFile.close();
        return CompletableFuture.completedFuture(Try.VOID);
    }

    @Override
    public void initDriverLoop(ReActorSystem localReActorSystem) { logFile.flush();}

    @Override
    public CompletionStage<Try<Void>> cleanDriverLoop() {
        return CompletableFuture.completedFuture(Try.ofRunnable(() -> {
            logFile.flush();
            logFile.close();
        }));
    }

    @Override
    public UnChecked.CheckedRunnable getDriverLoop() { return () -> { }; }

    @Override
    public ChannelId getChannelId() { return channelId; }

    @Override
    public Properties getChannelProperties() { return new Properties(); }

    @Override
    public DeliveryStatus sendMessage(ReActorContext destination, Message message) {
        synchronized (logFile) {
            logFile.printf("[%s] SENDER: %s\t\tDESTINATION: %s\t\t SEQNUM:%d\t\tPAYLOAD TYPE: %s%nPAYLOAD: %s%n%n",
                           Instant.now(),
                           message.getSender().getReActorId().getReActorName(),
                           message.getDestination().getReActorId().getReActorName(),
                           message.getSequenceNumber(),
                           message.getPayload().getClass().toString(),
                           message.getPayload());
            logFile.flush();
        }
        return destination.isStop()
               ? DeliveryStatus.NOT_DELIVERED
               : localDeliver(destination, message);
    }
}
