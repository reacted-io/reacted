/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import java.io.Serializable;
import java.util.Collection;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
@NonNullByDefault
public class SourceStream<OutputT extends Serializable> extends StreamProxy<OutputT> {
    @Nullable
    private final SourceSubscription<OutputT> sourceSubscription;
    private SourceStream(Stream<OutputT> inputStream) {
        super(inputStream);
        this.sourceSubscription = null;
    }

    private SourceStream(Stream<OutputT> inputStream,
                         SourceSubscription<OutputT> sourceSubscription) {
        super(inputStream);
        this.sourceSubscription = sourceSubscription;
    }
    public static <OutputT extends Serializable> SourceStream<OutputT>
    of(Collection<OutputT> inputCollection) {
        return of(inputCollection.stream());
    }

    public static <OutputT extends Serializable>
    SourceStream<OutputT> of(Flow.Publisher<OutputT> publisher) {
        SourceSubscription<OutputT> subscription = new SourceSubscription<>();
        publisher.subscribe(subscription);
        Spliterator<OutputT> spliterator = new Spliterator<>() {
            @Override
            public boolean tryAdvance(Consumer<? super OutputT> action) {
                OutputT message = subscription.getNext();
                if (message != null) {
                    action.accept(message);
                    subscription.requestNext();
                }
                return subscription.hasNext();
            }

            @Override
            @Nullable
            public Spliterator<OutputT> trySplit() { return null; }

            @Override
            public long estimateSize() { return Long.MAX_VALUE; }

            @Override
            public int characteristics() { return Spliterator.IMMUTABLE | Spliterator.ORDERED; }
        };
        return new SourceStream<>(StreamSupport.stream(spliterator, false), subscription);
    }
    public static <OutputT extends Serializable>
    SourceStream<OutputT> of(Stream<OutputT> inputStream) {
        return new SourceStream<>(inputStream);
    }

    @Override
    public void close() {
        if (sourceSubscription != null) {
            sourceSubscription.stop();
        }
    }

    private static class SourceSubscription<OutputT extends Serializable>
        implements Subscriber<OutputT> {
        private final BlockingQueue<OutputT> dataOutput = new LinkedBlockingQueue<>(1);
        private volatile boolean isTerminated = false;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Subscription subscription;
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            requestNext();
        }

        @Override
        public void onNext(OutputT item) {
            dataOutput.add(item);
        }

        @Override
        public void onError(Throwable throwable) { this.isTerminated = true; }

        @Override
        public void onComplete() { this.isTerminated = true; }

        private boolean hasNext() { return !isTerminated; }
        @Nullable
        private OutputT getNext() {
            return Try.of(() -> dataOutput.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
                      .orElse(null, error -> stop());
        }
        private void stop() {
            this.isTerminated = true;
            subscription.cancel(); }
        private void requestNext() { subscription.request(1);}
    }
}
