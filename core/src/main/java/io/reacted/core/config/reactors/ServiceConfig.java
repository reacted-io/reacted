/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.reactors;

import io.reacted.core.config.reactors.ServiceConfig.Builder;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class ServiceConfig extends ReActorServiceConfig<Builder, ServiceConfig> {
  private ServiceConfig(Builder builder) {
    super(builder);
  }
  public static Builder newBuilder() { return new Builder(); }

  public static class Builder extends ReActorServiceConfig.Builder<Builder, ServiceConfig> {

    private Builder() { /* No body required */}

    @Override
    public ServiceConfig build() {
      return new ServiceConfig(this);
    }
  }
}
