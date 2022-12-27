/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams.messages;

import io.reacted.core.serialization.ReActedMessage;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PublisherComplete implements ReActedMessage {

  @Override
  public String toString() {
    return "PublisherComplete{}";
  }
}
