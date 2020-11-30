/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import io.reacted.core.messages.SerializationUtils;
import io.reacted.core.reactors.ReActorId;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class EventExecutionAttempt implements Externalizable {

    private static final long serialVersionUID = 1;
    private static final long REACTOR_ID_OFFSET = SerializationUtils.getFieldOffset(EventExecutionAttempt.class,
                                                                          "reActorId")
                                                                    .orElseSneakyThrow();
    private static final long EXECUTION_SEQ_NUM_OFFSET = SerializationUtils.getFieldOffset(EventExecutionAttempt.class,
                                                                                   "executionSeqNum")
                                                                           .orElseSneakyThrow();
    private static final long MESSAGE_SEQ_NUM_OFFSET = SerializationUtils.getFieldOffset(EventExecutionAttempt.class,
                                                                                   "msgSeqNum")
                                                                         .orElseSneakyThrow();
    private final ReActorId reActorId;
    private final long executionSeqNum;
    private final long msgSeqNum;

    public EventExecutionAttempt() {
        /* Required for Externalizable */
        this.reActorId = ReActorId.NO_REACTOR_ID;
        this.executionSeqNum = 0;
        this.msgSeqNum = 0;
    }

    public EventExecutionAttempt(ReActorId reActorId, long executionSeqNum, long msgSeqNum) {
        this.reActorId = reActorId;
        this.executionSeqNum = executionSeqNum;
        this.msgSeqNum = msgSeqNum;
    }

    public ReActorId getReActorId() { return reActorId; }

    public long getExecutionSeqNum() { return executionSeqNum; }

    public long getMsgSeqNum() { return msgSeqNum; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventExecutionAttempt)) return false;
        EventExecutionAttempt that = (EventExecutionAttempt) o;
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
    public void writeExternal(ObjectOutput out) throws IOException {
        getReActorId().writeExternal(out);
        out.writeLong(getExecutionSeqNum());
        out.writeLong(getMsgSeqNum());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ReActorId receivedReActorId = new ReActorId();
        receivedReActorId.readExternal(in);
        setReActorId(receivedReActorId);
        setExecutionSeqNum(in.readLong());
        setMessageSeqNum(in.readLong());
    }

    @SuppressWarnings("UnusedReturnValue")
    private EventExecutionAttempt setReActorId(ReActorId reActorId) {
        return SerializationUtils.setObjectField(this, REACTOR_ID_OFFSET, reActorId);
    }

    @SuppressWarnings("UnusedReturnValue")
    private EventExecutionAttempt setExecutionSeqNum(long executionSeqNum) {
        return SerializationUtils.setLongField(this, EXECUTION_SEQ_NUM_OFFSET, executionSeqNum);
    }

    @SuppressWarnings("UnusedReturnValue")
    private EventExecutionAttempt setMessageSeqNum(long messageSeqNum) {
        return SerializationUtils.setLongField(this, MESSAGE_SEQ_NUM_OFFSET, messageSeqNum);
    }
}
