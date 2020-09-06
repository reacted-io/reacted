package io.reacted.core.mailboxes;

import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.patterns.Try;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Function;

public class BackpressuringSubscriber implements Flow.Subscriber<BackpressuringMbox.DeliveryRequest> {
    private final long requestOnStartup;
    private final Function<Message, DeliveryStatus> realDeliveryCallback;
    private final Executor onErroAsyncExecutor;
    private final SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer;
    private volatile boolean isCompleted;
    private volatile Flow.Subscription subscription;

    BackpressuringSubscriber(long requestOnStartup,
                             Function<Message, DeliveryStatus> realDeliveryCallback,
                             Executor onErrorAsyncExecutor,
                             SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer) {
        this.requestOnStartup = requestOnStartup;
        this.realDeliveryCallback = realDeliveryCallback;
        this.onErroAsyncExecutor = onErrorAsyncExecutor;
        this.backpressurer = backpressurer;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        if (requestOnStartup > 0) {
            subscription.request(requestOnStartup);
        }
    }

    @Override
    public void onNext(BackpressuringMbox.DeliveryRequest item) {
        var deliveryResult = this.isCompleted
                ? Try.ofSuccess(DeliveryStatus.NOT_DELIVERED)
                : Try.of(() -> realDeliveryCallback.apply(item.deliveryPayload))
                     .peekFailure(error -> onErroAsyncExecutor.execute(() -> onError(error)));
        item.pendingTrigger.complete(deliveryResult);
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
        this.subscription.request(elementsToRequest);
    }
}
