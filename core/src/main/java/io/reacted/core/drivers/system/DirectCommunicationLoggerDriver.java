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
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@NonNullByDefault
public class DirectCommunicationLoggerDriver extends LocalDriver<DirectCommunicationLoggerCfg> {
    private final ChannelId channelId;
    private PrintWriter logFile;

    public DirectCommunicationLoggerDriver(DirectCommunicationLoggerCfg cfg) {
        super(cfg);
        this.channelId =  new ChannelId(ChannelId.ChannelType.DIRECT_COMMUNICATION,
                                        cfg.getChannelName());
    }

    @Override
    public Try<Void> initDriverCtx(ReActorSystem localReActorSystem) {
        this.logFile =  Try.of(() -> new FileWriter(getDriverConfig().getLogFilePath(), false))
                           .map(PrintWriter::new)
                                             .orElseSneakyThrow();
        return Try.VOID;
    }

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
