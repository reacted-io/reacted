/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.handlers;

import com.sun.net.httpserver.HttpExchange;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.examples.webappbackend.Backend;
import io.reacted.examples.webappbackend.db.StoreReply;
import io.reacted.examples.webappbackend.db.StoreRequest;
import io.reacted.patterns.AsyncUtils;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class PostHandler implements ReActor {
    private static final AtomicLong KEYS = new AtomicLong();
    @Nullable
    private final HttpExchange httpExchange;
    private final ExecutorService ioAsyncExecutor;
    private final String requestId;
    private final StringBuilder payloadBuilder;
    public PostHandler(@Nullable HttpExchange httpExchange, String requestId, ExecutorService ioAsyncExecutor) {
        this.httpExchange = httpExchange;
        this.ioAsyncExecutor = Objects.requireNonNull(ioAsyncExecutor);
        this.requestId = requestId;
        this.payloadBuilder = new StringBuilder();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> onRequestHandlingInit(raCtx))
                        .reAct(DataChunkPush.class, this::onNewDataChunk)
                        .reAct(DataChunksCompleted.class, (raCtx, complete) -> onPostComplete(raCtx))
                        .reAct(StoreReply.class, this::onStoreReply)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestId)
                            .setMailBoxProvider(raCtx -> BackpressuringMbox.newBuilder()
                                                                           .setRealMailboxOwner(raCtx)
                                                                           .setAsyncBackpressurer(ioAsyncExecutor)
                                                                           .setBufferSize(1)
                                                                           .setRequestOnStartup(1)
                                                                           .setRealMbox(new BasicMbox())
                                                                           .setBackpressureTimeout(BackpressuringMbox.RELIABLE_DELIVERY_TIMEOUT)
                                                                           .setNonDelayable(Set.of(ReActorInit.class))
                                                                           .build())
                            .build();
    }

    private void onPostComplete(ReActorContext raCtx) {
        raCtx.getReActorSystem()
             .serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                .setServiceName(Backend.DB_SERVICE_NAME)
                                                                .build())
             .thenAccept(serviceDiscovery -> onDbServiceReply(raCtx, serviceDiscovery));
    }

    private void onDbServiceReply(ReActorContext raCtx, Try<ServiceDiscoveryReply> reply) {
        reply.filter(services -> !services.getServiceGates().isEmpty())
             .map(services -> services.getServiceGates().iterator().next())
             .mapOrElse(dbGate -> dbGate.tell(new StoreRequest(KEYS.getAndIncrement() + "",
                                                               payloadBuilder.toString())),
                        error -> raCtx.selfTell(new DbNotReachable()));
    }

    private void onStoreReply(ReActorContext raCtx, StoreReply storeReply) {
        if (httpExchange != null) {
            Try.ofRunnable(() -> httpExchange.getResponseBody().close())
               .ifError(error -> raCtx.logError("Error closing output stream", error));
        }
        raCtx.stop();
    }

    private void onNewDataChunk(ReActorContext raCtx, DataChunkPush newChunk) {
        payloadBuilder.append(newChunk.lineRead);
        ((BackpressuringMbox)raCtx.getMbox()).request(1);
    }

    private void onRequestHandlingInit(ReActorContext raCtx) {
        if (httpExchange == null) {
            return;
        }
        ioAsyncExecutor.submit(() -> Try.ofRunnable(() -> httpExchange.sendResponseHeaders(200, 0))
                                        .ifError(error -> { raCtx.logError("Unable to send response headers", error);
                                                            raCtx.stop(); }));
        readPostDataStream(raCtx);
    }

    /* Backpressured async read from the remote stream */
    private void readPostDataStream(ReActorContext raCtx) {
        var requestStream = Objects.requireNonNull(httpExchange).getRequestBody();
        var reader = new BufferedReader(new InputStreamReader(requestStream));
        CompletionStage<Try<DeliveryStatus>> whileLoop;
        whileLoop = AsyncUtils.asyncLoop(deliveryStatus -> sendTillAvailable(raCtx, reader),
                                         Try.ofSuccess(DeliveryStatus.DELIVERED), Objects::nonNull,
                                         error -> CompletableFuture.completedStage(Try.ofFailure(error)),
                                         ioAsyncExecutor);
        whileLoop.thenAccept(result -> Try.ofRunnable(reader::close)
                                          .ifSuccessOrElse(noVal -> raCtx.logInfo("Stream closed"),
                                                           error -> raCtx.logError("Error closing stream", error)));
    }

   @Nullable
   private static CompletionStage<Try<DeliveryStatus>> sendTillAvailable(ReActorContext raCtx,
                                                                         BufferedReader inputStream) {
        var nextMsg = getNextDataChunk(inputStream);
        var nextSend = raCtx.selfTell(nextMsg);
        return nextMsg.getClass() != DataChunksCompleted.class
                ? nextSend
                : nextSend.thenCompose(result -> CompletableFuture.completedFuture(null));
    }
    private static Serializable getNextDataChunk(BufferedReader requestStream) {
        return Try.of(requestStream::readLine)
                  .filter(Objects::nonNull)
                  .map(string -> (Serializable)new DataChunkPush(string))
                  .orElse(new DataChunksCompleted());
    }

    private static final class DataChunkPush implements Serializable {
        private final String lineRead;
        private DataChunkPush(String lineRead) {
            this.lineRead = lineRead;
        }
    }

    private static final class DataChunksCompleted implements Serializable {}
    private static final class DbNotReachable implements Serializable {}
}
