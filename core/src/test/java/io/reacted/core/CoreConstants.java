/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core;

import io.reacted.core.serialization.ReActedMessage;

public final class CoreConstants {

    public static final String DESTINATION = "DESTINATION";
    public static final String DISPATCHER = "Dispatcher";
    public static final ReActedMessage DE_SERIALIZATION_SUCCESSFUL = ReActedMessage.of("De/Serialization Successful!");

    public static final String HIGH_PRIORITY = "high priority";

    public static final String LOW_PRIORITY = "low priority";

    public static final String PRIORITY_2 = "priority 2";

    public static final String REACTOR_NAME = "ReactorName";
    public static final String REACTED_ACTOR_SYSTEM = "ReActedActorSystem";

    public static final String SOURCE = "SOURCE";

    public static final String TEST_DISPATCHER = "TestDispatcher";

    private CoreConstants() {
    }
}
