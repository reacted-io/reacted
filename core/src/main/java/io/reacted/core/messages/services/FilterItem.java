/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import com.google.common.base.Objects;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Properties;

@NonNullByDefault
public record FilterItem(ReActorRef serviceGate, Properties serviceProperties) implements Serializable {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilterItem that = (FilterItem) o;
        return Objects.equal(serviceProperties(), that.serviceProperties()) &&
                Objects.equal(serviceGate(), that.serviceGate());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(serviceProperties(), serviceGate());
    }

    @Override
    public String toString() {
        return "FilterItem{" + "serviceProperties=" + serviceProperties + ", serviceGate=" + serviceGate + '}';
    }
}
