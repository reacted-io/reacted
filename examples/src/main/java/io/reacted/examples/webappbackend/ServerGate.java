/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.examples.webappbackend.handlers.GetHandler;
import io.reacted.examples.webappbackend.handlers.PostHandler;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class ServerGate implements ReActor, HttpHandler {
    private static final String WRITE_PREDICATE_PATH = "/write";
    private static final String READ_PREDICATE_PATH = "/read";
    private final AtomicLong requestCounter = new AtomicLong(0);
    private final HttpServer server;
    private final ExecutorService asyncService;
    private final ExecutorService serverExecutor;
    private final ConcurrentHashMap<String, HttpExchange> requestIdToHttpExchange;
    private ReActorContext thisCtx;
    public ServerGate(HttpServer server, ExecutorService asyncExecutorService, ExecutorService serverExecutor) {
        this.server = Objects.requireNonNull(server);
        this.asyncService = Objects.requireNonNull(asyncExecutorService);
        this.serverExecutor = Objects.requireNonNull(serverExecutor);
        this.requestIdToHttpExchange = new ConcurrentHashMap<>();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> onGateInit(raCtx))
                        .reAct(ReActorStop.class, (raCtx, stop) -> server.stop(0))
                        .reAct(SpawnPostHandler.class, this::spawnPostHandler)
                        .reAct(SpawnGetHandler.class, this::spawnGetHandler)
                        .build();
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName("SystemGate")
                            .build();
    }

    @Override
    public void handle(HttpExchange exchange) {
        var requestId = DigestUtils.md5Hex(exchange.getRequestURI().toASCIIString()) + "|" +
                        requestCounter.getAndIncrement();
        this.requestIdToHttpExchange.put(requestId, exchange);
        thisCtx.selfTell("GET".equals(exchange.getRequestMethod())
                         ? new SpawnGetHandler(requestId)
                         : new SpawnPostHandler(requestId));
    }
    private void onGateInit(ReActorContext raCtx) {
        this.thisCtx = raCtx;
        server.createContext(WRITE_PREDICATE_PATH, this);
        server.createContext(READ_PREDICATE_PATH, this);
        server.setExecutor(serverExecutor);
        server.start();
    }

    /* We use a message to trigger the spawn of the handler because in this way the Replay Driver will be able to
       re-issue the command. Replay driver can only deliver messages to ReActors, not creating them.
       On replay, this ServerGate reactor regardless if a http request arrives or not will receive a spawn request, triggering
       the handling using the saved logs
     */
    private void spawnPostHandler(ReActorContext raCtx, SpawnPostHandler spawnRequest) {
        thisCtx.spawnChild(new PostHandler(this.requestIdToHttpExchange.remove(spawnRequest.requestId),
                                           spawnRequest.requestId, asyncService))
               .ifError(error -> raCtx.logError("Unable to spawn child reactor", error));
    }

    private void spawnGetHandler(ReActorContext raCtx, SpawnGetHandler spawnRequest) {
        thisCtx.spawnChild(new GetHandler(this.requestIdToHttpExchange.remove(spawnRequest.requestId),
                                          spawnRequest.requestId, asyncService))
               .ifError(error -> raCtx.logError("Unable to spawn child reactor", error));
    }
    private static class SpawnPostHandler implements Serializable {
        public final String requestId;
        public SpawnPostHandler(String requestId) {
            this.requestId = requestId;
        }

        @Override
        public String toString() {
            return "SpawnPostHandler{" + "requestId='" + requestId + '\'' + '}';
        }
    }

    private static class SpawnGetHandler extends SpawnPostHandler {
        private SpawnGetHandler(String requestId) {
            super(requestId);
        }
    }
}
