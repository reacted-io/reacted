/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.serviceregistry;

import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

@Immutable
@NonNullByDefault
public class RegistryServicePublicationFailed implements ReActedMessage {
    private final Throwable publicationError;
    private final String serviceName;

    public RegistryServicePublicationFailed(String serviceName, Throwable publicationError) {
        this.publicationError = Objects.requireNonNull(publicationError);
        this.serviceName = Objects.requireNonNull(serviceName);
    }

    public Throwable getPublicationError() { return publicationError; }

    public String getServiceName() { return serviceName; }
}
