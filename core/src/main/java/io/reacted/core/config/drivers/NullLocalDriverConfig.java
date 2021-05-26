/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.core.config.ChannelId;

public class NullLocalDriverConfig extends ChannelDriverConfig<NullLocalDriverConfig.Builder,
                                                                NullLocalDriverConfig> {
  private NullLocalDriverConfig(Builder builder) { super(builder); }
  public static NullLocalDriverConfig.Builder newBuilder() { return new NullLocalDriverConfig.Builder(); }

  public static class Builder extends ChannelDriverConfig.Builder<NullLocalDriverConfig.Builder,
                                                                  NullLocalDriverConfig> {
    private Builder() { setAckCacheSize(0); }
    @Override
    public NullLocalDriverConfig build() {
      return new NullLocalDriverConfig(this);
    }
  }
}
