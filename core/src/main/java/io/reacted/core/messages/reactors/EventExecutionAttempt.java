/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.core.messages.Recyclable;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.serialization.Deserializer;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.core.serialization.Serializer;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Serial;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class EventExecutionAttempt implements ReActedMessage, Recyclable {

    @Serial
    private static final long serialVersionUID = 1;
    private ReActorId reActorId;
    private long executionSeqNum;
    private long msgSeqNum;

    private boolean isValid = true;

    public EventExecutionAttempt() {
        /* Required for Externalizable */
        this.reActorId = ReActorId.NO_REACTOR_ID;
        this.executionSeqNum = 0;
        this.msgSeqNum = 0;
    }
    public ReActorId getReActorId() { return reActorId; }

    public long getExecutionSeqNum() { return executionSeqNum; }

    public long getMsgSeqNum() { return msgSeqNum; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventExecutionAttempt that)) return false;
        return getExecutionSeqNum() == that.getExecutionSeqNum() &&
               getMsgSeqNum() == that.getMsgSeqNum() &&
               Objects.equals(getReActorId(), that.getReActorId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getReActorId(), getExecutionSeqNum(), getMsgSeqNum());
    }

    @Override
    public String toString() {
        return "EventExecutionAttempt{" +
               "reActorId=" + reActorId +
               ", executionSeqNum=" + executionSeqNum +
               ", msgSeqNum=" + msgSeqNum +
               '}';
    }

    @Override
    public void encode(Serializer serializer) {
        getReActorId().encode(serializer);
        serializer.put(getExecutionSeqNum());
        serializer.put(getMsgSeqNum());
        revalidate();
    }

    @Override
    public void decode(Deserializer deserializer) {
        var receivedReActorId = new ReActorId();
        receivedReActorId.decode(deserializer);
        setReActorId(receivedReActorId);
        setExecutionSeqNum(deserializer.getLong());
        setMessageSeqNum(deserializer.getLong());
        revalidate();
    }

    @SuppressWarnings("UnusedReturnValue")
    public EventExecutionAttempt setReActorId(ReActorId reActorId) {
        this.reActorId = reActorId;
        invalidate();
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public EventExecutionAttempt setExecutionSeqNum(long executionSeqNum) {
        this.executionSeqNum = executionSeqNum;
        invalidate();
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public EventExecutionAttempt setMessageSeqNum(long messageSeqNum) {
        this.msgSeqNum = messageSeqNum;
        invalidate();
        return this;
    }

    public EventExecutionAttempt errorIfInvalid() {
        if (!isValid) {
            throw new IllegalStateException("Attempt to use an invalid recycled object detected");
        }
        return this;
    }
    @Override
    public void revalidate() {
        this.isValid = true;
    }

    @Override
    public void invalidate() {
        this.isValid = false;
    }
}
