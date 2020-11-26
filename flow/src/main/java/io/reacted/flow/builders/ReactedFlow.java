/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow.builders;

public class ReactedFlow {
    private ReactedFlow(Builder builder) {

    }

    public static Builder newBuilder() { return new Builder(); }

    public static class Builder implements StepLevel<ReactedFlow> {
        private Builder() { }

        public StepLevel

        public ReactedFlow build() { return new ReactedFlow(this); }
    }
}
