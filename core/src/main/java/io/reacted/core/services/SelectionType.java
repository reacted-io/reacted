/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.services;

public enum SelectionType {
    /**
     * Request a reference to a router providing the service
     */
    ROUTED,
    /**
     * Request a reference to a reactor providing the service
     */
    DIRECT
}
