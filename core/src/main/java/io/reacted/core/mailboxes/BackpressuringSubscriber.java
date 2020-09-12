package io.reacted.core.mailboxes;

import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.patterns.Try;

import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

public class BackpressuringSubscriber implements Flow.Subscriber<BackpressuringMbox.DeliveryRequest> {
    private final long requestOnStartup;
    private final Function<Message, DeliveryStatus> realDeliveryCallback;
    private final SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer;
    private final LongAdder preInitializationRequests;
    private volatile boolean isCompleted;
    private volatile Flow.Subscription subscription;

    BackpressuringSubscriber(long requestOnStartup,
                             Function<Message, DeliveryStatus> realDeliveryCallback,
                             SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer) {
        this.requestOnStartup = requestOnStartup;
        this.realDeliveryCallback = realDeliveryCallback;
        this.backpressurer = backpressurer;
        this.preInitializationRequests = new LongAdder();
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        long requests = this.requestOnStartup;
        synchronized (this.preInitializationRequests) {
            requests += this.preInitializationRequests.sum();
        }
        if (requests > 0) {
            subscription.request(requests);
        }
    }

    @Override
    public void onNext(BackpressuringMbox.DeliveryRequest item) {
        var deliveryResult = this.isCompleted
                ? Try.ofSuccess(DeliveryStatus.NOT_DELIVERED)
                : Try.of(() -> realDeliveryCallback.apply(item.deliveryPayload));
        item.pendingTrigger.complete(deliveryResult);
        deliveryResult.ifError(this::onError);
    }

    @Override
    public void onError(Throwable throwable) {
        if (!this.isCompleted) {
            this.isCompleted = true;
            backpressurer.close();
        }
    }

    @Override
    public void onComplete() {
        if(!this.isCompleted) {
            this.isCompleted = true;
        }
    }

    public void request(long elementsToRequest) {
        if (this.subscription == null) {
            synchronized (this.preInitializationRequests) {
                if (this.subscription == null) {
                    this.preInitializationRequests.add(elementsToRequest);
                    return;
                }
            }
        }
        this.subscription.request(elementsToRequest);
    }
}
