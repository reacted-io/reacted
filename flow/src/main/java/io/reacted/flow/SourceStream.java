/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.patterns.NonNullByDefault;
import java.io.Serializable;
import java.util.Collection;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
@NonNullByDefault
public class SourceStream extends StreamProxy<Serializable> {
    @Nullable
    private final SourceSubscription sourceSubscription;
    private SourceStream(Stream<Serializable> inputStream) {
        super(inputStream);
        this.sourceSubscription = null;
    }

    private SourceStream(Stream<Serializable> inputStream,
                         SourceSubscription sourceSubscription) {
        super(inputStream);
        this.sourceSubscription = sourceSubscription;
    }

    public SourceStream init() {
        if (sourceSubscription != null) {
            sourceSubscription.init();
        }
        return this;
    }
    public static SourceStream of(Collection<Serializable> inputCollection) {
        return of(inputCollection.stream());
    }

    public static SourceStream of(Flow.Publisher<Serializable> publisher) {
        SourceSubscription subscription = new SourceSubscription();
        publisher.subscribe(subscription);
        Spliterator<Serializable> spliterator = new Spliterator<>() {
            @Override
            public boolean tryAdvance(Consumer<? super Serializable> action) {
                Serializable message = subscription.getNext();
                if (message != null) {
                    action.accept(message);
                }
                subscription.requestNext();
                return subscription.hasNext();
            }

            @Override
            public Spliterator<Serializable> trySplit() { return null; }

            @Override
            public long estimateSize() { return Long.MAX_VALUE; }

            @Override
            public int characteristics() {
                return Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.SORTED;
            }
        };
        return new SourceStream(StreamSupport.stream(spliterator, false), subscription);
    }
    public static SourceStream of(Stream<Serializable> inputStream) {
        return new SourceStream(inputStream);
    }

    @Override
    public void close() {
        if (sourceSubscription != null) {
            sourceSubscription.stop();
        }
    }

    private static class SourceSubscription implements Subscriber<Serializable> {
        private final BlockingQueue<Serializable> dataOutput = new LinkedBlockingQueue<>(1);
        private volatile boolean isTerminated = false;
        private Subscription subscription;
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(Serializable item) { dataOutput.add(item); }

        @Override
        public void onError(Throwable throwable) { this.isTerminated = true; }

        @Override
        public void onComplete() { this.isTerminated = true; }

        private boolean hasNext() { return !isTerminated; }
        private Serializable getNext() { return dataOutput.poll(); }
        private void init() { requestNext(); }
        private void stop() { subscription.cancel(); }
        private void requestNext() { subscription.request(1);}
    }
}
