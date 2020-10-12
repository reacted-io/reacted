/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.ChannelId;
import io.reacted.core.drivers.system.DirectCommunicationCfg;
import io.reacted.core.drivers.system.DirectCommunicationDriver;
import io.reacted.core.drivers.system.DirectCommunicationLoggerCfg;
import io.reacted.core.drivers.system.DirectCommunicationLoggerDriver;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.patterns.UnChecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public final class SystemLocalDrivers {
    public static final Try<DeliveryStatus> MESSAGE_NOT_DELIVERED = Try.ofSuccess(DeliveryStatus.NOT_DELIVERED);
    public static final CompletionStage<Try<DeliveryStatus>>
            ASYNC_MESSAGE_NOT_DELIVERED = CompletableFuture.completedFuture(MESSAGE_NOT_DELIVERED);
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemLocalDrivers.class);
    private SystemLocalDrivers() { /* Not Required */ }

    public static final DirectCommunicationDriver DIRECT_COMMUNICATION =
            new DirectCommunicationDriver(DirectCommunicationCfg.newBuilder()
                                                                .setChannelName("DIRECT_COMMUNICATION")
                                                                .build());

    public static DirectCommunicationLoggerDriver getDirectCommunicationLogger(String loggingFilePath) {
        return new DirectCommunicationLoggerDriver(DirectCommunicationLoggerCfg.newBuilder()
                                                                               .setLogFilePath(loggingFilePath)
                                                                               .setChannelName("LOGGING_DIRECT_COMMUNICATION-")
                                                                               .build());
    }

    public static LocalDriver getDirectCommunicationSimplifiedLogger(String loggingFilePath) {
        return new LocalDriver() {
            public final ChannelId CHANNEL_ID = new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION,
                                                              "SIMPLIFIED_LOGGING_DIRECT_COMMUNICATION-" + loggingFilePath);
            private final PrintWriter logFile = new PrintWriter(Try.of(() -> new FileWriter(loggingFilePath, false))
                                                                   .orElseSneakyThrow());

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
                return CompletableFuture.completedFuture(Try.ofRunnable(() -> {
                    logFile.flush();
                    logFile.close();
                }));
            }

            @Override
            public UnChecked.CheckedRunnable getDriverLoop() { return () -> { }; }

            @Override
            public ChannelId getChannelId() { return CHANNEL_ID; }

            @Override
            public Properties getChannelProperties() { return new Properties(); }

            @Override
            public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
                synchronized (logFile) {
                    logFile.printf("SENDER: %s\t\tDESTINATION: %s\t\t SEQNUM:%d\t\tPAYLOAD TYPE: %s%nPAYLOAD: %s%n%n",
                                   message.getSender().getReActorId().getReActorName(),
                                   message.getDestination().getReActorId().getReActorName(),
                                   message.getSequenceNumber(),
                                   message.getPayload().getClass().toString(),
                                   message.getPayload().toString());
                    logFile.flush();
                }
                return destination.isStop() ? MESSAGE_NOT_DELIVERED : localDeliver(destination, message);
            }

            @Override
            public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
                synchronized (logFile) {

                    logFile.printf("SENDER: %s\t\tDESTINATION: %s\t\t SEQNUM:%d\t\tPAYLOAD TYPE: %s%nPAYLOAD: %s%n%n",
                                   message.getSender().getReActorId().getReActorName(),
                                   message.getDestination().getReActorId().getReActorName(),
                                   message.getSequenceNumber(),
                                   message.getPayload().getClass().toString(),
                                   message.getPayload().toString());
                    logFile.flush();
                }
                return destination.isStop() ? ASYNC_MESSAGE_NOT_DELIVERED : asyncLocalDeliver(destination, message);
            }
        };
    }
}
