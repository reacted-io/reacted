/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.mailboxes;

import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

@NonNullByDefault
class BackpressuringSubscriber implements Flow.Subscriber<BackpressuringMbox.DeliveryRequest> {
    private final long requestOnStartup;
    private final Function<Message, DeliveryStatus> realDeliveryCallback;
    private final SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer;
    private final LongAdder preInitializationRequests;
    private final ReActorContext targetMailboxOwner;
    @Nullable
    private Flow.Subscription subscription;

    BackpressuringSubscriber(long requestOnStartup,
                             ReActorContext raCtx,
                             Function<Message, DeliveryStatus> realDeliveryCallback,
                             SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer) {
        this.requestOnStartup = requestOnStartup;
        this.realDeliveryCallback = realDeliveryCallback;
        this.backpressurer = backpressurer;
        this.targetMailboxOwner = raCtx;
        this.preInitializationRequests = new LongAdder();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        long requests = requestOnStartup;
        synchronized (preInitializationRequests) {
            requests += preInitializationRequests.sum();
        }
        if (requests > 0) {
            subscription.request(requests);
        }
    }

    @Override
    public void onNext(BackpressuringMbox.DeliveryRequest item) {
        if (realDeliveryCallback.apply(item.deliveryPayload).isDelivered()) {
            targetMailboxOwner.reschedule();
        }
    }

    @Override
    public void onError(Throwable throwable) { backpressurer.close(); }

    @Override
    public void onComplete() {
        //https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8254060
        Objects.requireNonNull(subscription).cancel();
    }

    public void request(long elementsToRequest) {
        if (subscription == null) {
            synchronized (preInitializationRequests) {
                if (subscription == null) {
                    preInitializationRequests.add(elementsToRequest);
                    return;
                }
            }
        }
        Objects.requireNonNull(subscription).request(elementsToRequest);
    }
}
