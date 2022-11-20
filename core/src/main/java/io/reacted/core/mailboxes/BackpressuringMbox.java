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
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.ObjectUtils;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

@NonNullByDefault
public class BackpressuringMbox implements MailBox {
    public static final long DEFAULT_MESSAGES_REQUESTED_ON_STARTUP = 1L;
    private final MailBox realMbox;
    private final AtomicReference<Set<Class<? extends Serializable>>> notDelayable;
    private final Set<? extends Serializable> outOfStreamControl;
    private final long requestOnStartup;
    private final ReActorContext realMailboxOwner;
    private final long backpressuringThreshold;
    private final BlockingDeque<Message> bufferQueue = new LinkedBlockingDeque<>();
    private long available;

    /*
     * BackpressuringMbox wrapper for any other mailbox type.
     */
    private BackpressuringMbox(Builder builder) {
        this.outOfStreamControl = Objects.requireNonNull(builder.outOfStreamControl,
                                                         "Out of Stream control set cannot be null");
        this.available = ObjectUtils.requiredInRange(builder.requestOnStartup, 0L, Long.MAX_VALUE, IllegalArgumentException::new);
        this.realMailboxOwner = Objects.requireNonNull(builder.realMailboxOwner,
                                                       "Mailbox owner reactor cannot be null");
        this.realMbox = Objects.requireNonNull(builder.realMbox,
                                               "A backing mailbox must be provided");
        this.notDelayable = new AtomicReference<>(Objects.requireNonNull(builder.notDelayable,
                                                   "Non delayable messages set cannot be a null")
                                   .stream()
                                   .filter(Objects::nonNull)
                                   .collect(Collectors.toUnmodifiableSet()));
        this.requestOnStartup = builder.requestOnStartup;
        this.backpressuringThreshold = ObjectUtils.requiredInRange(builder.backpressuringThreshold,
                                                                   1L, Long.MAX_VALUE,
                                                                   IllegalArgumentException::new);
    }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public boolean isEmpty() { return  realMbox.isEmpty(); }

    @Override
    public boolean isFull() { return realMbox.isFull(); }

    @Override
    public long getMsgNum() { return realMbox.getMsgNum(); }

    @Override
    public long getMaxSize() { return realMbox.getMaxSize(); }
    public long getRequestOnStartup() { return requestOnStartup; }

    @Nonnull
    @Override
    public Message getNextMessage() {
        return realMbox.getNextMessage();
    }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        DeliveryStatus deliveryAttempt = DeliveryStatus.DELIVERED;
        if (isDelayable(message.getPayload().getClass())) {
            synchronized (this) {
                if (available > 0 && bufferQueue.isEmpty()) {
                    if (!outOfStreamControl.contains(message.getPayload().getClass())) {
                        available--;
                    }
                    return realMbox.deliver(message);
                }
                bufferQueue.addLast(message);
            }
            if (bufferQueue.size() >= backpressuringThreshold) {
                deliveryAttempt = DeliveryStatus.BACKPRESSURE_REQUIRED;
            }
        } else {
            deliveryAttempt = realMbox.deliver(message);
        }
        return deliveryAttempt;
    }

    @Override
    public void request(long messagesNum) {
        synchronized (this) {
            this.available += messagesNum;
            while(available != 0 && !bufferQueue.isEmpty()) {
                Message payload = bufferQueue.removeFirst();
                realMbox.deliver(payload);
                if (!outOfStreamControl.contains(payload.getPayload().getClass())) {
                    available--;
                }
            }
        }
        realMailboxOwner.reschedule();
    }

    public static Optional<BackpressuringMbox> toBackpressuringMailbox(MailBox mailBox) {
        return BackpressuringMbox.class.isAssignableFrom(mailBox.getClass())
               ? Optional.of((BackpressuringMbox)mailBox)
               : Optional.empty();
    }
    public boolean isDelayable(Class<? extends Serializable> payloadType) {
        return !notDelayable.get().contains(payloadType);
    }

    public BackpressuringMbox addNonDelayableTypes(Set<Class<? extends Serializable>> notDelayedToAdd) {
        Set<Class<? extends Serializable>> notDelayedCache;
        Set<Class<? extends Serializable>> notDelayedMerge;
        do {
            notDelayedCache = notDelayable.get();
            notDelayedMerge = Stream.concat(notDelayedCache.stream(),
                                            Objects.requireNonNull(notDelayedToAdd,
                                                                   "Non delayable types cannot be null")
                                                   .stream()
                                                   .filter(Objects::nonNull))
                                    .collect(Collectors.toUnmodifiableSet());
        }while(!notDelayable.compareAndSet(notDelayedCache, notDelayedMerge));
        return this;
    }

    public static class Builder {
        private MailBox realMbox = new BasicMbox();
        private long backpressuringThreshold = DEFAULT_MESSAGES_REQUESTED_ON_STARTUP;
        private long requestOnStartup = DEFAULT_MESSAGES_REQUESTED_ON_STARTUP;

        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActorContext realMailboxOwner;
        private Set<Class<? extends Serializable>> notDelayable = Set.of(ReActorInit.class,
                                                                         ReActorStop.class);
        private Set<Class<? extends Serializable>> outOfStreamControl = Set.of();

        private Builder() { }

        /**
         * Message types that will be delivered according to the mailbox sequence, but will not be affected
         * by the {@link java.util.concurrent.Flow} regulation mechanism: a message delivered whose type
         * belongs to this set, will not alter the requested (allowed to be delivered) counter
         *
         * @param notRegulatedByStreamControl An arbitrary set of {@link Class} message types that will not impact
         *                                    the {@link BackpressuringMbox#request(long)} mechanism when delivered
         * @return this {@link Builder}
         */
        @SafeVarargs
        public final Builder setOutOfStreamControl(Class<? extends Serializable> ...notRegulatedByStreamControl) {
            this.outOfStreamControl = Set.of(notRegulatedByStreamControl);
            return this;
        }

        /**
         * Set the backpressuring threshold for this mailbox. If more messages than this
         * threshold should be waiting in the mailbox, the outcome of a delivery will be
         * {@link DeliveryStatus#BACKPRESSURE_REQUIRED} to notify the producer that it has
         * to slow down
         * @param threshold
         * @return this {@link Builder}
         */
        public final Builder setBackpressuringThreshold(long threshold) {
            this.backpressuringThreshold = threshold;
            return this;
        }
        /**
         *
         * @param realMbox Backing-up mailbox
         *                 Default: {@link BasicMbox}
         * @return this {@link Builder}
         */
        public final Builder setRealMbox(MailBox realMbox) {
            this.realMbox = realMbox;
            return this;
        }


        /**
         *
         * @param requestOnStartup a non-negative integer representing how main messages should be
         *                         automatically made deliverable on startup.
         *                         Same semantic of Java Flow Subscription.request.
         *                         Default {@link #DEFAULT_MESSAGES_REQUESTED_ON_STARTUP}
         * @return this {@link Builder}
         */
        public final Builder setRequestOnStartup(int requestOnStartup) {
            this.requestOnStartup = requestOnStartup;
            return this;
        }

        /**
         * A non-delayable message is a message that has to be delivered immediately, without being
         * regulated by the {@link BackpressuringMbox#request(long)} mechanism and preempting any
         * other pending message
         *
         * @param notDelayable An arbitrary set of {@link Class} message types that cannot be delayed
         * @see BackpressuringMbox#request(long)
         * @return this {@link Builder}
         */
        @SafeVarargs
        public final Builder setNonDelayable(Class<? extends Serializable> ...notDelayable) {
            this.notDelayable = Set.of(notDelayable);
            return this;
        }

        /**
         *
         * @param realMailboxOwner {@link ReActorContext} of the reactor owning {@link BackpressuringMbox#realMbox}
         * @return this {@link Builder}
         */
        public final Builder setRealMailboxOwner(ReActorContext realMailboxOwner) {
            this.realMailboxOwner = realMailboxOwner;
            return this;
        }

        /**
         *
         * @return a {@link BackpressuringMbox}
         * @throws NullPointerException if any of the non null arguments is found to be null
         * @throws IllegalArgumentException if {@code sequencer} can create more than one thread
         *                                  if {@code bufferSize} is not positive
         *                                  if {@code requestOnStartup} is negative
         */
        public BackpressuringMbox build() { return new BackpressuringMbox(this); }
    }
}
