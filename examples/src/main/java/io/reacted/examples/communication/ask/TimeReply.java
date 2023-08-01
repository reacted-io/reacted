/*
 * Copyright (c) 2023 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.communication.ask;

import io.reacted.core.serialization.ReActedMessage;

import java.time.Instant;

public record TimeReply(Instant time) implements ReActedMessage { }
