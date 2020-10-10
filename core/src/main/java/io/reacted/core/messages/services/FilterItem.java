/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.services;

import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.NonNullByDefault;
import java.util.Properties;

@NonNullByDefault
public class FilterItem {
    private final Properties serviceProperties;
    private final ReActorRef serviceGate;

    public FilterItem(ReActorRef serviceGate, Properties serviceProperties) {
        this.serviceGate = serviceGate;
        this.serviceProperties = serviceProperties;
    }

    public Properties getServiceProperties() { return serviceProperties; }

    public ReActorRef getServiceGate() { return serviceGate; }
}
