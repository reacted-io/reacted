/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.streams;

import io.reacted.examples.ExampleUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.streams.ReactedSubmissionPublisher;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

@NonNullByDefault
class BestEffortConsumersApp {

    public static void main(String[] args) throws InterruptedException {
        var reactorSystem = ExampleUtils.getDefaultInitedReActorSystem(BestEffortConsumersApp.class.getSimpleName());
        var streamPublisher = new ReactedSubmissionPublisher<Integer>(reactorSystem,
                                                                      BestEffortConsumersApp.class.getSimpleName() +
                                                                      "-Publisher");
        var subscriber = new TestSubscriber<>(-1, Integer::compareTo);
        //Best effort subscriber. Updates from this may be lost
        streamPublisher.subscribe(subscriber);
        //We need to give the time to the subscription to propagate till the producer
        TimeUnit.SECONDS.sleep(1);
        var msgNum = 1_000_000;
        //Produce a stream of updates
        IntStream.range(0, msgNum)
                 //Propagate them to every consumer, regardless of the location
                 .forEachOrdered(streamPublisher::submit);
        streamPublisher.close();
        //NOTE: you can join or triggering the new update once the previous one has been delivered
        do {
            long updates = subscriber.getReceivedUpdates();
            TimeUnit.SECONDS.sleep(5);
            long updates2 = subscriber.getReceivedUpdates();
            if (updates == updates2) {
                break;
            }
        }while (true);
        System.out.printf("Best effort subscriber received %d/%d updates%n", subscriber.getReceivedUpdates(), msgNum);
        reactorSystem.shutDown();
    }

    private static class TestSubscriber<PayloadT> implements Flow.Subscriber<PayloadT> {
        private final Comparator<PayloadT> payloadTComparator;
        private final LongAdder updatesReceived;
        private volatile boolean isTerminated = false;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private volatile Flow.Subscription subscription;
        private volatile PayloadT lastItem;

        private TestSubscriber(PayloadT baseItem, Comparator<PayloadT> payloadComparator) {
            this.payloadTComparator = Objects.requireNonNull(payloadComparator);
            this.lastItem = Objects.requireNonNull(baseItem);
            this.updatesReceived = new LongAdder();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(PayloadT item) {
            if (!this.isTerminated) {
                if (this.payloadTComparator.compare(this.lastItem, item) >= 0) {
                    Try.ofRunnable(() -> { throw new IllegalStateException("Unordered sequence detected"); })
                       .peekFailure(Throwable::printStackTrace)
                       .orElseSneakyThrow();
                }
                this.lastItem = item;
                this.updatesReceived.increment();
                subscription.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (!this.isTerminated) {
                this.isTerminated = true;
                throwable.printStackTrace();
            }
        }

        @Override
        public void onComplete() {
            if (!this.isTerminated) {
                this.isTerminated = true;
                System.out.println("Feed is complete");
            }
        }

        private long getReceivedUpdates() { return updatesReceived.sum(); }
    }
}
