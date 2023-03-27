/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.remoting.services;

import io.reacted.core.serialization.ReActedMessage;

import java.time.ZonedDateTime;

class TimeMessages {
    public record TimeRequest() implements ReActedMessage {}
    public record TimeReply(ZonedDateTime time) implements ReActedMessage { }
}
