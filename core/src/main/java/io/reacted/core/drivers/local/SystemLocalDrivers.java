/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.ChannelId;
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


    public static final LocalDriver DIRECT_COMMUNICATION = new LocalDriver() {
        public final ChannelId CHANNEL_ID = new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION,
                                              "DIRECT_COMMUNICATION");

        @Override
        public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
            return Try.VOID;
        }

        @Override
        public CompletionStage<Try<Void>> stopDriverCtx(ReActorSystem reActorSystem) {
            return CompletableFuture.completedFuture(Try.VOID);
        }

        @Override
        public void initDriverLoop(ReActorSystem localReActorSystem) { }

        @Override
        public CompletionStage<Try<Void>> cleanDriverLoop() { return CompletableFuture.completedFuture(Try.VOID); }

        @Override
        public UnChecked.CheckedRunnable getDriverLoop() { return () -> {}; }

        @Override
        public ChannelId getChannelId() { return CHANNEL_ID; }

        @Override
        public Properties getChannelProperties() {
            return new Properties();
        }

        @Override
        public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
            return destination.isStop() ? MESSAGE_NOT_DELIVERED : localDeliver(destination, message);
        }

        @Override
        public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
            return destination.isStop() ? ASYNC_MESSAGE_NOT_DELIVERED : asyncLocalDeliver(destination, message);
        }
    };

    public static LocalDriver getDirectCommunicationLogger(String loggingFilePath) {
        return new LocalDriver() {
            public final ChannelId CHANNEL_ID = new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION,
                                                              "LOGGING_DIRECT_COMMUNICATION-" + loggingFilePath);
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
                return CompletableFuture.completedFuture(Try.ofRunnable(() -> { logFile.flush(); logFile.close(); }));
            }

            @Override
            public UnChecked.CheckedRunnable getDriverLoop() { return () -> { }; }

            @Override
            public ChannelId getChannelId() { return CHANNEL_ID; }

            @Override
            public Properties getChannelProperties() { return new Properties(); }

            @Override
            public Try<DeliveryStatus> sendMessage(ReActorContext destination, Message message) {
                logFile.println(message.toString());
                logFile.flush();
                return destination.isStop() ? MESSAGE_NOT_DELIVERED : localDeliver(destination, message);
            }

            @Override
            public CompletionStage<Try<DeliveryStatus>> sendAsyncMessage(ReActorContext destination, Message message) {
                var delivery =  destination.isStop()
                                ? ASYNC_MESSAGE_NOT_DELIVERED
                                : asyncLocalDeliver(destination, message);
                delivery.thenAccept(deliveryAttempt -> {
                    synchronized (logFile) {
                        logFile.println(message.toString());
                        logFile.flush();
                    }
                });
                return delivery;
            }
        };
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

    private static Try<DeliveryStatus> localDeliver(ReActorContext destination, Message message) {
        Try<DeliveryStatus> deliverOperation = Try.of(() -> destination.getMbox()
                                                                       .deliver(message));
        rescheduleIfSuccess(deliverOperation, destination);
        return deliverOperation;
    }

    private static CompletionStage<Try<DeliveryStatus>> asyncLocalDeliver(ReActorContext destination, Message message) {
        var asyncDeliverResult = destination.getMbox()
                                            .asyncDeliver(message);
        asyncDeliverResult.thenAccept(result -> rescheduleIfSuccess(result, destination));
        return asyncDeliverResult;
    }

    private static void rescheduleIfSuccess(Try<DeliveryStatus> deliveryResult, ReActorContext destination) {
        deliveryResult.peekFailure(error -> LOGGER.error("Unable to deliver: ", error))
                      .filter(DeliveryStatus::isDelivered)
                      .ifSuccess(deliveryStatus -> destination.reschedule());
    }
}
