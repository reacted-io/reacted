/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.messages.reactors;

import javax.annotation.concurrent.Immutable;
import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@Immutable
public final class ReActorStop implements Externalizable {
    private static final long serialVersionUID = 1;

    @Override
    public void writeExternal(ObjectOutput out) { /* No fields */ }

    @Override
    public String toString() {
        return "ReActorStop{}";
    }

    @Override
    public void readExternal(ObjectInput in) { /* No fields */ }
}
