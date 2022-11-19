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
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.awaitility.Awaitility;

@NonNullByDefault
class SlowdownProducerApp {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        var reactorSystem = ExampleUtils.getDefaultInitedReActorSystem(SlowdownProducerApp.class.getSimpleName());
        try(var streamPublisher = new ReactedSubmissionPublisher<Integer>(reactorSystem, 1_000,
                                                                          SlowdownProducerApp.class.getSimpleName() + "-Publisher")) {
            var subscribers = List.of(new TestSubscriber(), new TestSubscriber(), new TestSubscriber());
            int subId = 0;
            for (var subscriber : subscribers) {
                streamPublisher.subscribe(subscriber, 100, subId+++"")
                               .toCompletableFuture()
                               .join();
            }
            var msgNum = 1_000_000;
            Duration delay = Duration.ofNanos(1);
            //Produce a stream of updates
            for(int updateNum = 0; updateNum < msgNum; updateNum++) {
                if (streamPublisher.submit(updateNum).isBackpressureRequired()) {
                    TimeUnit.NANOSECONDS.sleep(delay.toNanos());
                    delay = delay.multipliedBy(2);
                } else {
                    if (delay.toNanos() > 1) {
                        delay = Duration.ofNanos(Math.max(1, delay.toNanos() / 2));
                    }
                }
            }
            //NOTE: you can join or triggering the new update once the previous one has been delivered
            Awaitility.await()
                      .atMost(Duration.ofSeconds(10))
                      .until(() -> subscribers.stream()
                                              .map(TestSubscriber::getReceivedUpdates)
                                              .allMatch(updates -> updates == msgNum));
            System.out.printf("Subscribers received %d/%d updates%n",
                              subscribers.get(0).getReceivedUpdates(), msgNum);
        }
        reactorSystem.shutDown();
    }

    private static class TestSubscriber implements Flow.Subscriber<Integer> {
        private final LongAdder updatesReceived = new LongAdder();
        private boolean isTerminated = false;
        private Flow.Subscription subscription;
        private int lastItem = -1;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Integer item) {
            updatesReceived.increment();
            if (!isTerminated) {
                if (lastItem >= item) {
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
                System.err.println("Feed is complete");
            }
        }

        private long getReceivedUpdates() { return updatesReceived.sum(); }
    }
}
