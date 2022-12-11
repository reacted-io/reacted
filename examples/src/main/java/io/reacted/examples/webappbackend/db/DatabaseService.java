/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;

import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;
import org.bson.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

@NonNullByDefault
public class DatabaseService implements ReActor {
    private static final String DB_NAME = "reactedapp";
    private static final String COLLECTION = "data";
    static final String PAYLOAD_FIELD = "payload";
    @Nullable
    private final MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    public DatabaseService(@Nullable MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(DatabaseService.class.getSimpleName())
                            .build();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (ctx, init) -> ifNotReplaying(this::onMongoInit, ctx, init))
                        .reAct(StorageMessages.StoreRequest.class,
                               (ctx, store) -> ifNotReplaying(this::onStoreRequest, ctx, store))
                        .reAct(StorageMessages.QueryRequest.class,
                               (ctx, query) -> ifNotReplaying(this::onQueryRequest, ctx, query))
                        .build();
    }

    private void onMongoInit(ReActorContext ctx, ReActorInit init) {
        this.mongoCollection = Objects.requireNonNull(mongoClient).getDatabase(DB_NAME)
                                      .getCollection(COLLECTION);
    }
    private void onStoreRequest(ReActorContext ctx, StorageMessages.StoreRequest request) {
        var publisher = mongoCollection.insertOne(new Document(Map.of("_id", request.getKey(),
                                                                      PAYLOAD_FIELD, request.getPayload())));
        publisher.subscribe(new MongoSubscribers.MongoStoreSubscriber(ctx.getSelf(), ctx.getSender()));
    }

    private void onQueryRequest(ReActorContext ctx, StorageMessages.QueryRequest request) {
        mongoCollection.find(Filters.eq("_id", request.key()))
                       .first().subscribe(new MongoSubscribers.MongoQuerySubscriber(ctx.getSelf(),
                                                                                    ctx.getSender()));
    }
    private <PayloadT extends Serializable>
    void ifNotReplaying(BiConsumer<ReActorContext, PayloadT> realCall, ReActorContext ctx, PayloadT anyPayload) {
        if (mongoClient != null) {
            realCall.accept(ctx, anyPayload);
        }
    }
}
