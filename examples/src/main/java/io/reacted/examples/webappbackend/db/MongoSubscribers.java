/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;

import com.mongodb.client.result.InsertOneResult;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.examples.webappbackend.db.DatabaseService;
import io.reacted.examples.webappbackend.db.StorageMessages;
import io.reacted.patterns.NonNullByDefault;
import org.bson.Document;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@NonNullByDefault
final class MongoSubscribers {
    static class MongoQuerySubscriber implements Subscriber<Document> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Subscription subscription;
        private final ReActorRef mongoGate;
        private final ReActorRef requester;

        MongoQuerySubscriber(ReActorRef mongoGate, ReActorRef requester) {
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
            requester.tell(mongoGate, new StorageMessages.QueryReply(item.get(DatabaseService.PAYLOAD_FIELD)
                                                                         .toString()))
                     .thenAccept(delivery -> subscription.request(1));
        }

        @Override
        public void onError(Throwable throwable) { }

        @Override
        public void onComplete() { }
    }
    static class MongoStoreSubscriber implements Subscriber<InsertOneResult> {
        private final ReActorRef requester;
        private final ReActorRef mongoGate;
        MongoStoreSubscriber(ReActorRef mongoGate, ReActorRef requester) {
            this.requester = requester;
            this.mongoGate = mongoGate;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(1);
        }

        @Override
        public void onNext(InsertOneResult item) { }

        @Override
        public void onError(Throwable throwable) {
            requester.tell(mongoGate, new StorageMessages.StoreError(throwable));
        }

        @Override
        public void onComplete() {
            requester.tell(mongoGate, new StorageMessages.StoreReply());
        }
    }
}
