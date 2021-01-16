/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config;
public abstract class InheritableBuilder<BuilderT extends InheritableBuilder.Builder<BuilderT, BuiltT>,
                                         BuiltT extends InheritableBuilder<BuilderT, BuiltT>> {

    protected InheritableBuilder(Builder<BuilderT, BuiltT> builder) { }

    public abstract static class Builder<BuilderT, BuiltT> {

        public abstract BuiltT build();

        @SuppressWarnings("unchecked")
        protected BuilderT getThis() { return (BuilderT)this; }
    }
}
