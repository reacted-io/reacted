/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import io.reacted.core.messages.reactors.ReActedDebug;
import io.reacted.core.messages.reactors.ReActedError;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class SystemLogger {
    private static final String NO_ERROR_DESCRIPTION = "";
    @Nullable
    private static final Throwable NO_EXCEPTION = null;

    private final static Logger LOGGER = LoggerFactory.getLogger(SystemLogger.class);
    public static final ReActions SYSTEM_LOGGER = ReActions.newBuilder()
            .reAct(ReActorInit.class, (ctx, init) -> {})
            .reAct(ReActorStop.class, (ctx, init) -> {})
            .reAct(ReActedError.class, SystemLogger::onErrorMessage)
            .reAct(ReActedDebug.class, SystemLogger::onDebugMessage)
            .build();

    private static void onErrorMessage(ReActorContext ctx, ReActedError payload) {
        LOGGER.error("{}", payload.getMessage().orElse(NO_ERROR_DESCRIPTION),
                           payload.getError().orElse(NO_EXCEPTION));
    }

    private static void onDebugMessage(ReActorContext raCtx, ReActedDebug payload) {
        LOGGER.info(payload.getDebugMessage());
    }
}
