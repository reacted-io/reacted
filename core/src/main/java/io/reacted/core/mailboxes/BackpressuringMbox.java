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
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

@NonNullByDefault
public class BackpressuringMbox implements MailBox {
    public static final long DEFAULT_MESSAGES_REQUESTED_ON_STARTUP = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BackpressuringMbox.class);
    private final MailBox realMbox;
    private final Set<Class<? extends Serializable>> notBackpressurable;
    private final long requestOnStartup;
    private final ReActorContext realMailboxOwner;
    private final AtomicLong available;
    private final long backpressuringThreshold;

    /*
     * BackpressuringMbox wrapper for any other mailbox type.
     */
    private BackpressuringMbox(Builder builder) {
        this.available = new AtomicLong(ObjectUtils.requiredInRange(builder.requestOnStartup, 0L, Long.MAX_VALUE, IllegalArgumentException::new));
        this.realMailboxOwner = Objects.requireNonNull(builder.realMailboxOwner,
                                                       "Mailbox owner reactor cannot be null");
        this.realMbox = Objects.requireNonNull(builder.realMbox,
                                               "A backing mailbox must be provided");
        this.notBackpressurable = Objects.requireNonNull(builder.notBackpressurable,
                                                         "Non backpressurable messages set cannot be a null")
                                         .stream()
                                         .filter(Objects::nonNull)
                                         .collect(Collectors.toUnmodifiableSet());
        this.requestOnStartup = builder.requestOnStartup;
        this.backpressuringThreshold = ObjectUtils.requiredInRange(builder.backpressuringThreshold,
                                                                   1L, Long.MAX_VALUE,
                                                                   IllegalArgumentException::new);
    }

    public static Builder newBuilder() { return new Builder(); }

    @Override
    public boolean isEmpty() { return  realMbox.isEmpty() || available.get() == 0; }

    @Override
    public boolean isFull() { return realMbox.isFull(); }

    @Override
    public long getMsgNum() { return realMbox.getMsgNum(); }

    @Override
    public long getMaxSize() { return realMbox.getMaxSize(); }
    public long getRequestOnStartup() { return requestOnStartup; }

    @Nonnull
    @Override
    public Message getNextMessage() { return realMbox.getNextMessage(); }

    @Nonnull
    @Override
    public DeliveryStatus deliver(Message message) {
        DeliveryStatus deliveryAttempt = realMbox.deliver(message);
        if (deliveryAttempt.isDelivered()) {
            if (realMbox.getMsgNum() >= backpressuringThreshold) {
                deliveryAttempt = DeliveryStatus.BACKPRESSURE_REQUIRED;
            }
        }
        return deliveryAttempt;
    }

    @Override
    public void request(long messagesNum) {
        if (available.addAndGet(messagesNum) > 0) {
            realMailboxOwner.reschedule();
        }
    }

    public static Optional<BackpressuringMbox> toBackpressuringMailbox(MailBox mailBox) {
        return BackpressuringMbox.class.isAssignableFrom(mailBox.getClass())
               ? Optional.of((BackpressuringMbox)mailBox)
               : Optional.empty();
    }
    public boolean isBackpressurable(Class<? extends Serializable> payloadType) {
        return !notBackpressurable.contains(payloadType);
    }

    public static class Builder {
        private MailBox realMbox = new BasicMbox();
        private long backpressuringThreshold = DEFAULT_MESSAGES_REQUESTED_ON_STARTUP;
        private long requestOnStartup = DEFAULT_MESSAGES_REQUESTED_ON_STARTUP;
        @SuppressWarnings("NotNullFieldNotInitialized")
        private ReActorContext realMailboxOwner;
        private Set<Class<? extends Serializable>> notBackpressurable = Set.of(ReActorInit.class,
                                                                               ReActorStop.class);

        private Builder() { }

        public final Builder setBackpressuringThreshold(long threshold) {
            this.backpressuringThreshold = threshold;
            return this;
        }
        /**
         *
         * @param realMbox Backing-up mailbox
         *                 Default: {@link BasicMbox}
         * @return this builder
         */
        public final Builder setRealMbox(MailBox realMbox) {
            this.realMbox = realMbox;
            return this;
        }


        /**
         *
         * @param requestOnStartup how main messages should be automatically made deliverable on startup.
         *                         Same semantic of Java Flow Subscription.request.
         *                         Default {@link #DEFAULT_MESSAGES_REQUESTED_ON_STARTUP}
         * @return this builder
         */
        public final Builder setRequestOnStartup(int requestOnStartup) {
            this.requestOnStartup = requestOnStartup;
            return this;
        }

        /**
         *
         * @param notBackpressurable Messages that cannot be lost. If a delivery cannot be done immediately the system
         *                           will wait till when necessary to deliver the message
         * @return this builder
         */
        public final Builder setNonBackpressurable(Set<Class<? extends Serializable>> notBackpressurable) {
            this.notBackpressurable = notBackpressurable;
            return this;
        }

        /**
         *
         * @param realMailboxOwner {@link ReActorContext} of the reactor owning {@link BackpressuringMbox#realMbox}
         * @return this builder
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
