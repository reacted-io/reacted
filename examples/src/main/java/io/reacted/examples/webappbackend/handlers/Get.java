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
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.services.BasicServiceDiscoverySearchFilter;
import io.reacted.core.messages.services.ServiceDiscoveryReply;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.ObjectUtils;
import io.reacted.examples.webappbackend.Backend;
import io.reacted.examples.webappbackend.db.StorageMessages;
import io.reacted.patterns.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

public class Get implements ReActor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Get.class);
    @Nullable
    private final HttpExchange httpExchange;
    private final String requestId;
    private final ExecutorService asyncService;
    private ReActorRef dbGate;
    private String requestKey;
    public Get(@Nullable HttpExchange httpExchange, String requestId, ExecutorService asyncService) {
        this.httpExchange = httpExchange;
        this.requestId = Objects.requireNonNull(requestId);
        this.asyncService = Objects.requireNonNull(asyncService);
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(requestId)
                            .build();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> onInit(raCtx))
                        .reAct(ProcessGet.class, this::onProcessGet)
                        .reAct(ServiceDiscoveryReply.class, this::onDbServiceDiscoveryReply)
                        .build();
    }

    private void onInit(ReActorContext raCtx) {
        if (httpExchange != null) {
            CompletableFuture.runAsync(() -> Try.ofRunnable(() -> httpExchange.sendResponseHeaders(200, 0)), asyncService)
                             .toCompletableFuture()
                             .thenAccept(noVal -> raCtx.selfTell(new ProcessGet(httpExchange.getRequestURI()
                                                                                            .toString())));
        }
        // start a request in parallel
        raCtx.getReActorSystem()
             .serviceDiscovery(BasicServiceDiscoverySearchFilter.newBuilder()
                                                                .setServiceName(Backend.DB_SERVICE_NAME)
                                                                .build(), raCtx.getSelf());
    }

    private void onProcessGet(ReActorContext raCtx, ProcessGet getRequest) {
        try {
            this.requestKey = extractGetFirstParameter(getRequest.getRequest);
            if (dbGate != null) {
                retrieveEntry(raCtx, requestKey, dbGate);
            }
        } catch (Exception anyParseException) {
            sendReplyMessage("Invalid request format");
            raCtx.stop();
        }
    }

    private void onDbServiceDiscoveryReply(ReActorContext raCtx, ServiceDiscoveryReply serviceDiscoveryReply) {
        if (serviceDiscoveryReply.getServiceGates().isEmpty()) {
            /* No db to retrieve data from */
            sendReplyMessage("No database could be found");
            raCtx.stop();
            return;
        }
        this.dbGate = serviceDiscoveryReply.getServiceGates().iterator().next();
        if (requestKey != null) {
            retrieveEntry(raCtx, requestKey, dbGate);
        }
    }

    private void retrieveEntry(ReActorContext raCtx, String key, ReActorRef dbGate) {
        dbGate.ask(new StorageMessages.QueryRequest(key), StorageMessages.QueryReply.class, raCtx.getSelf().getReActorId().toString())
              .thenComposeAsync(queryReply -> sendReplyMessage(queryReply.map(StorageMessages.QueryReply::getPayload)
                                                                         .orElseGet(Throwable::getMessage)),
                                asyncService)
              .thenAccept(sendReturn -> { sendReturn.ifError(error -> raCtx.logError("Unable to send back reply", error));
                                          raCtx.stop(); });
    }
    private static String extractGetFirstParameter(String getRequest) {
        return getRequest.split("\\?")[1].split("=")[1];
    }

    private CompletionStage<Try<Void>> sendReplyMessage(String message) {
        return httpExchange == null
                ? CompletableFuture.completedStage(Try.ofSuccess(null))
                                   .thenApply(noVal -> Try.ofRunnable(() -> LOGGER.info("Payload Received: {}",
                                                                                        message)))
                : CompletableFuture.supplyAsync(() -> Try.withResources(httpExchange::getResponseBody,
                                                                        response -> sendData(response, message)),
                                                asyncService);
    }
    @SuppressWarnings("SameReturnValue")
    private static Void sendData(OutputStream outputStream, String data) throws IOException {
        outputStream.write(data.getBytes());
        return ObjectUtils.VOID;
    }
    private static class ProcessGet implements Serializable {
        private final String getRequest;
        private ProcessGet(String getRequest) {
            this.getRequest = getRequest;
        }
    }
}
