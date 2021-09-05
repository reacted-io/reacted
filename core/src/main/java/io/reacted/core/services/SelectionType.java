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
     * Request a reference to a router providing the service. It's the choice if you want to exploit
     * automatic load balancing among different routees
     */
    ROUTED,
    /**
     * Request a reference to a reactor providing the service. No load balancing will take place,
     * but all the overhead related to message re-routing will be avoided
     */
    DIRECT
}
