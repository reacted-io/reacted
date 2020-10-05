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
    private volatile boolean isOpen;
    @Nullable
    private volatile Flow.Subscription subscription;

    BackpressuringSubscriber(long requestOnStartup,
                             ReActorContext raCtx,
                             Function<Message, DeliveryStatus> realDeliveryCallback,
                             SubmissionPublisher<BackpressuringMbox.DeliveryRequest> backpressurer) {
        this.requestOnStartup = requestOnStartup;
        this.realDeliveryCallback = realDeliveryCallback;
        this.backpressurer = backpressurer;
        this.targetMailboxOwner = raCtx;
        this.preInitializationRequests = new LongAdder();
        this.isOpen = true;
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
        if (this.isOpen) {
            try {
                if (this.realDeliveryCallback.apply(item.deliveryPayload).isDelivered()) {
                    this.targetMailboxOwner.reschedule();
                }
            } catch (Exception anyError) {
                onError(anyError);
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (this.isOpen) {
            this.isOpen = false;
            backpressurer.close();
        }
    }

    @Override
    public void onComplete() {
        if(this.isOpen) {
            this.isOpen = false;
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
        Objects.requireNonNull(this.subscription).request(elementsToRequest);
    }
}
