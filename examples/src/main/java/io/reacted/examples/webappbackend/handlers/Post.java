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
import io.reacted.core.mailboxes.UnboundedMbox;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.examples.webappbackend.Backend;
import io.reacted.examples.webappbackend.db.StorageMessages;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class Post implements ReActor {
    @Nullable
    private final HttpExchange httpExchange;
    private final ExecutorService ioAsyncExecutor;
    private final String requestId;
    private final StringBuilder payloadBuilder;
    public Post(@Nullable HttpExchange httpExchange, String requestId, ExecutorService ioAsyncExecutor) {
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
                        .reAct(StorageMessages.StoreReply.class, this::onStoreReply)
                        .reAct(StorageMessages.StoreError.class, this::onStoreError)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestId)
                            .setMailBoxProvider(raCtx -> BackpressuringMbox.newBuilder()
                                                                           .setRealMailboxOwner(raCtx)
                                                                           .setAvailableOnStartup(1)
                                                                           .setRealMbox(new UnboundedMbox())
                                                                           .build())
                            .build();
    }

    private void onPostComplete(ReActorContext raCtx) {
        raCtx.getMbox().request(1);
        raCtx.getReActorSystem()
             .serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                .setServiceName(Backend.DB_SERVICE_NAME)
                                                                .build())
             .thenAccept(serviceDiscovery -> onDbServiceReply(raCtx, serviceDiscovery));
    }

    private void onDbServiceReply(ReActorContext raCtx, ServiceDiscoveryReply services) {
        raCtx.getMbox().request(1);
        if (!services.getServiceGates().isEmpty()) {
            services.getServiceGates().iterator().next()
                    .publish(raCtx.getSelf(),
                             new StorageMessages.StoreRequest(String.valueOf(Instant.now().toEpochMilli()),
                                                           payloadBuilder.toString()));
        } else {
            raCtx.selfPublish(new StorageMessages.StoreError(new RuntimeException("No database found")));
        }
    }

    private void onStoreReply(ReActorContext raCtx, StorageMessages.StoreReply storeReply) {
        raCtx.getMbox().request(1);
        if (httpExchange != null) {
            Try.ofRunnable(() -> httpExchange.getResponseBody().close())
               .ifError(error -> raCtx.logError("Error closing output stream", error));
        }
        raCtx.stop();
    }

    private void onStoreError(ReActorContext raCtx, StorageMessages.StoreError error) {
        raCtx.logError("Error storing payload: ", error);
        if (httpExchange != null) {
            Try.ofRunnable(() -> { httpExchange.getResponseBody().write(error.toString().getBytes());
                                   httpExchange.getResponseBody().close(); })
               .ifError(replyError -> raCtx.logError("Error closing output stream", replyError));
        }
        raCtx.stop();
    }

    private void onNewDataChunk(ReActorContext raCtx, DataChunkPush newChunk) {
        raCtx.getMbox().request(1);
        payloadBuilder.append(newChunk.lineRead);
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
        CompletionStage<DeliveryStatus> whileLoop;
        while (sendTillAvailable(raCtx, reader).isDelivered());
        Try.ofRunnable(reader::close)
           .ifSuccessOrElse(noVal -> raCtx.logInfo("Stream closed"),
                            error -> raCtx.logError("Error closing stream", error));
    }

   @Nullable
   private static DeliveryStatus sendTillAvailable(ReActorContext raCtx,
                                                                         BufferedReader inputStream) {
        var nextMsg = getNextDataChunk(inputStream);
        var nextSend = raCtx.selfPublish(nextMsg);
        return nextMsg.getClass() != DataChunksCompleted.class
                ? nextSend
                : DeliveryStatus.BACKPRESSURE_REQUIRED;
    }
    private static Serializable getNextDataChunk(BufferedReader requestStream) {
        return Try.of(requestStream::readLine)
                  .filter(Objects::nonNull)
                  .map(string -> (Serializable)new DataChunkPush(string))
                  .orElse(new DataChunksCompleted());
    }

    private record DataChunkPush(String lineRead) implements Serializable {
    }

    private static final class DataChunksCompleted implements Serializable {}
}
