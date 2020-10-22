/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import org.bson.Document;
import com.mongodb.client.model.Filters;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class MongoGate implements ReActor {
    private static final String DB_NAME = "reactedapp";
    private static final String COLLECTION = "data";
    private static final String PAYLOAD_FIELD = "payload";
    @Nullable
    private final MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    public MongoGate(@Nullable MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Nonnull
    @Override
    public ReActorConfig getConfig() {
        return ReActorConfig.newBuilder()
                            .setReActorName(MongoGate.class.getSimpleName())
                            .build();
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> ifNotReplaying(this::onMongoInit, raCtx, init))
                        .reAct(StoreRequest.class, (raCtx, store) -> ifNotReplaying(this::onStoreRequest, raCtx, store))
                        .reAct(QueryRequest.class, (raCtx, query) -> ifNotReplaying(this::onQueryRequest, raCtx, query))
                        .build();
    }

    private void onMongoInit(ReActorContext raCtx, ReActorInit init) {
        this.mongoCollection = Objects.requireNonNull(mongoClient).getDatabase(DB_NAME)
                                      .getCollection(COLLECTION);
    }
    private void onStoreRequest(ReActorContext raCtx, StoreRequest request) {
        mongoCollection.insertOne(new Document(Map.of("_id", request.getKey(),
                                                      PAYLOAD_FIELD, request.getPayload())))
                       .subscribe(new MongoStoreSubscriber(raCtx.getSelf(), raCtx.getSender()));
    }

    private void onQueryRequest(ReActorContext raCtx, QueryRequest request) {
        mongoCollection.find(Filters.eq("_id", request.getKey()))
                       .first().subscribe(new MongoQuerySubscriber(raCtx.getSelf(),
                                                                   raCtx.getSender()));
    }

    public static class MongoQuerySubscriber implements Subscriber<Document> {
        private Subscription subscription;
        private final ReActorRef mongoGate;
        private final ReActorRef requester;
        public MongoQuerySubscriber(ReActorRef mongoGate, ReActorRef requester) {
            this.mongoGate = mongoGate;
            this.requester = requester;
        }
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Document item) {
            requester.tell(mongoGate, new QueryReply(item.get(PAYLOAD_FIELD).toString()));
            this.subscription.cancel();
        }

        @Override
        public void onError(Throwable throwable) { }

        @Override
        public void onComplete() { }
    }

    private static class MongoStoreSubscriber implements Subscriber<InsertOneResult> {
        private final ReActorRef requester;
        private final ReActorRef mongoGate;
        private Subscription subscription;
        private MongoStoreSubscriber(ReActorRef mongoGate, ReActorRef requester) {
            this.requester = requester;
            this.mongoGate = mongoGate;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(InsertOneResult item) {
            requester.tell(mongoGate, new StoreReply());
            subscription.cancel();
        }

        @Override
        public void onError(Throwable throwable) { }

        @Override
        public void onComplete() { }
    }

    private <PayloadT extends Serializable>
    void ifNotReplaying(BiConsumer<ReActorContext, PayloadT> realCall,
                                ReActorContext raCtx, PayloadT anyPayload) {
        if (mongoClient != null) {
            realCall.accept(raCtx, anyPayload);
        }
    }
}
