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
import io.reacted.examples.webappbackend.handlers.Get;
import io.reacted.examples.webappbackend.handlers.Post;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

@NonNullByDefault
public class ServerGate implements ReActor, HttpHandler {
    private static final String WRITE_PREDICATE_PATH = "/write";
    private static final String READ_PREDICATE_PATH = "/read";
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
                        .reAct(ReActorInit.class, (ctx, init) -> onGateInit(ctx))
                        .reAct(ReActorStop.class, (ctx, stop) -> server.stop(0))
                        .reAct(SpawnPostHandler.class, this::spawnPostHandler)
                        .reAct(SpawnGetHandler.class, this::spawnGetHandler)
                        .reAct(SpawnUnknownHandler.class, ServerGate::spawnUnknownHandler)
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
        var requestId = exchange.getRequestURI().toASCIIString() + "|" + Instant.now().toString();
        this.requestIdToHttpExchange.put(requestId, exchange);
        thisCtx.selfPublish("GET".equals(exchange.getRequestMethod())
                         ? new SpawnGetHandler(requestId)
                         : "POST".equals(exchange.getRequestMethod())
                            ? new SpawnPostHandler(requestId)
                            : new SpawnUnknownHandler(requestId));
    }
    private void onGateInit(ReActorContext ctx) {
        this.thisCtx = ctx;
        server.createContext(WRITE_PREDICATE_PATH, this);
        server.createContext(READ_PREDICATE_PATH, this);
        server.setExecutor(serverExecutor);
        server.start();
    }

    /* We use a message to trigger the spawn of the handler because in this way the Replay Driver will be able to
       re-issue the command. Replay driver can only deliver messages to ReActors, not creating them.
       On replay, this ServerGate reactor regardless if a http request arrives or not will receive a spawn request,
       triggering the handling using the saved logs and initializing the flow
     */
    private void spawnPostHandler(ReActorContext ctx, SpawnPostHandler spawnRequest) {
        ctx.spawnChild(new Post(this.requestIdToHttpExchange.remove(spawnRequest.requestId),
                                  spawnRequest.requestId, asyncService))
               .ifError(error -> ctx.logError("Unable to spawn child reactor for request [{}]",
                                                spawnRequest.requestId, error));
    }

    private void spawnGetHandler(ReActorContext ctx, SpawnGetHandler spawnRequest) {
        ctx.spawnChild(new Get(this.requestIdToHttpExchange.remove(spawnRequest.getRequestId()),
                                 spawnRequest.getRequestId(), asyncService))
               .ifError(error -> ctx.logError("Unable to spawn child reactor for request [{}]",
                                                spawnRequest.getRequestId(), error));
    }
    private static void spawnUnknownHandler(ReActorContext ctx, SpawnUnknownHandler spawnUnknownRequest) {
        ctx.logError("Unknown request type received: ", spawnUnknownRequest.getRequestId());
    }

    private static class SpawnPostHandler implements Serializable {
        private final String requestId;
        private SpawnPostHandler(String requestId) { this.requestId = requestId; }

        String getRequestId() { return requestId; }
    }
    private static class SpawnGetHandler extends SpawnPostHandler {
        private SpawnGetHandler(String requestId) { super(requestId); }
    }

    private static class SpawnUnknownHandler extends SpawnPostHandler {
        private SpawnUnknownHandler(String requestId) { super(requestId); }
    }
}
