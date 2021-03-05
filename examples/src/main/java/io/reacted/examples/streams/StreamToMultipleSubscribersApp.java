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
import io.reacted.streams.ReactedSubmissionPublisher;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

@NonNullByDefault
class StreamToMultipleSubscribersApp {

    public static void main(String[] args) throws InterruptedException {
        var reactorSystem =
                ExampleUtils.getDefaultInitedReActorSystem(StreamToMultipleSubscribersApp.class.getSimpleName());
        var streamPublisher = new ReactedSubmissionPublisher<Integer>(reactorSystem,
                                                                      StreamToMultipleSubscribersApp.class.getSimpleName() + "-Publisher");
        var subscriber = new TestSubscriber<>(-1, Integer::compareTo);
        var subscriber2 = new TestSubscriber<>(-1, Integer::compareTo);
        var subscriber3 = new TestSubscriber<>(-1, Integer::compareTo);
        //Reliable (no messages lost) subscription
        streamPublisher.subscribe(subscriber, ReactedSubmissionPublisher.RELIABLE_SUBSCRIPTION);
        //Reliable (no messages lost) subscription
        streamPublisher.subscribe(subscriber2, ReactedSubmissionPublisher.RELIABLE_SUBSCRIPTION);
        //Best effort subscriber. Updates from this may be lost
        streamPublisher.subscribe(subscriber3);
        //We need to give the time to the subscription to propagate till the producer
        TimeUnit.SECONDS.sleep(1);
        var msgNum = 1_000_000;
        //Produce a stream of updates
        IntStream.range(0, msgNum)
                 //Propagate them to every consumer, regardless of the location
                 //Reliable subscribers will receive all the updates, best effort may loose something
                 //NOTE: in this example we are not slowing down the producer if a consumer cannot
                 //keep up with the update speed. Delivery guarantee is still valid, but pending
                 //updates will keep stacking in memory
                 .forEachOrdered(streamPublisher::submit);

        Awaitility.await()
                  .atMost(Duration.ofMinutes(5))
                  .until(() -> subscriber.getReceivedUpdates() == msgNum && subscriber2.getReceivedUpdates() == msgNum);
        streamPublisher.close();
        System.out.printf("Best effort subscriber received %d/%d updates%n", subscriber3.getReceivedUpdates(), msgNum);
        reactorSystem.shutDown();
    }

    private static class TestSubscriber<PayloadT> implements Flow.Subscriber<PayloadT> {
        private final Comparator<PayloadT> payloadTComparator;
        private final LongAdder updatesReceived;
        private boolean isTerminated = false;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private Flow.Subscription subscription;
        private PayloadT lastItem;

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
            updatesReceived.increment();
            if (!isTerminated) {
                if (payloadTComparator.compare(lastItem, item) >= 0) {
                    throw new IllegalStateException("Unordered sequence detected");
                }
                this.lastItem = item;
                subscription.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (!isTerminated) {
                this.isTerminated = true;
                throwable.printStackTrace();
            }
        }

        @Override
        public void onComplete() {
            if (!isTerminated) {
                this.isTerminated = true;
                System.out.println("Feed is complete");
            }
        }

        private long getReceivedUpdates() { return updatesReceived.sum(); }
    }
}
