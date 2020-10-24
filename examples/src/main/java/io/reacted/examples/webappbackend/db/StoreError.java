/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.examples.webappbackend.db;

import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public class StoreError extends Throwable {
    public StoreError(Throwable anyError) {
        super(anyError);
    }
}
