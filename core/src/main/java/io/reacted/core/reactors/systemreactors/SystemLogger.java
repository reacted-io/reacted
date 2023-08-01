/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.messages.reactors.LogMessage;
import io.reacted.core.messages.reactors.ReActedDebug;
import io.reacted.core.messages.reactors.ReActedError;
import io.reacted.core.messages.reactors.ReActedInfo;
import io.reacted.core.reactors.ReActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class SystemLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemLogger.class);
    public static final ReActions SYSTEM_LOGGER = ReActions.newBuilder()
            .reAct(ReActedError.class,
                   (ctx, message) -> logMessage(LOGGER::error, message))
            .reAct(ReActedDebug.class,
                   (ctx, message) -> logMessage(LOGGER::debug, message))
            .reAct(ReActedInfo.class,
                   (ctx, message) -> logMessage(LOGGER::info, message))
            .reAct(ReActions::noReAction)
            .build();

    private SystemLogger() { }
    private static void logMessage(Consumer<String> logger, LogMessage message) {
        logger.accept(message.getMessage());
    }
}
