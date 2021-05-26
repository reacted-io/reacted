/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core;

import io.reacted.core.drivers.system.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.reactors.ReActorId;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystemId;
import io.reacted.core.reactorsystem.ReActorSystemRef;

import java.util.Properties;

public class ReactorHelper {
    public static final ReActorSystemId TEST_REACTOR_SYSTEM_ID = new ReActorSystemId("TEST_REACTOR_SYSTEM");

    public static ReActorRef generateReactor(String reActorName) {
        return generateReactor(reActorName, TEST_REACTOR_SYSTEM_ID, SystemLocalDrivers.DIRECT_COMMUNICATION);
    }

    public static ReActorRef generateReactor(String reActorName, ReActorSystemId reActorSystemId,
                                             LocalDriver<?> localDriver) {
        ReActorSystemRef testReActorSystemRef = new ReActorSystemRef(localDriver, new Properties(), reActorSystemId);
        return new ReActorRef(new ReActorId(ReActorId.NO_REACTOR_ID, reActorName), testReActorSystemRef);
    }
}
