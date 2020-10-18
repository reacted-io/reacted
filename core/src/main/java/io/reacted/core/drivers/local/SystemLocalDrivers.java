/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.drivers.system.DirectCommunicationConfig;
import io.reacted.core.drivers.system.DirectCommunicationDriver;
import io.reacted.core.drivers.system.DirectCommunicationLoggerConfig;
import io.reacted.core.drivers.system.DirectCommunicationLoggerDriver;
import io.reacted.core.drivers.system.DirectCommunicationSimplifiedLoggerConfig;
import io.reacted.core.drivers.system.DirectCommunicationSimplifiedLoggerDriver;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;

@NonNullByDefault
public final class SystemLocalDrivers {
    private SystemLocalDrivers() { /* Not Required */ }

    public static final DirectCommunicationDriver DIRECT_COMMUNICATION =
            new DirectCommunicationDriver(DirectCommunicationConfig.newBuilder()
                                                                   .setChannelName("DIRECT_COMMUNICATION")
                                                                   .build());

    /**
     * Returns a {@link DirectCommunicationDriver} for <b>local</b> communication that after sending each message
     * logs the content of the message in a file
     *
     * @param loggingFilePath path of the file that should be created/truncated to store the dump of the messages
     *                        exchanged within the local {@link ReActorSystem}
     * @throws java.io.UncheckedIOException if an error occurs opening the file
     * @return the {@link DirectCommunicationLoggerDriver}
     */
    public static DirectCommunicationLoggerDriver getDirectCommunicationLogger(String loggingFilePath) {
        return new DirectCommunicationLoggerDriver(DirectCommunicationLoggerConfig.newBuilder()
                                                                                  .setLogFilePath(loggingFilePath)
                                                                                  .setChannelName("LOGGING_DIRECT_COMMUNICATION-")
                                                                                  .build());
    }

    /**
     * Returns a {@link DirectCommunicationDriver} for <b>local</b> communication that after sending each message
     * logs the main information of the message in a file. It is a less noisy version of {@link DirectCommunicationLoggerDriver}
     *
     * @param loggingFilePath path of the file that should be created/truncated to store the dump of the messages
     *                        exchanged within the local {@link ReActorSystem}
     * @throws java.io.UncheckedIOException if an error occurs opening the file
     * @return the {@link DirectCommunicationSimplifiedLoggerDriver}
     */
    public static DirectCommunicationSimplifiedLoggerDriver
    getDirectCommunicationSimplifiedLoggerDriver(String loggingFilePath) {
        return new DirectCommunicationSimplifiedLoggerDriver(DirectCommunicationSimplifiedLoggerConfig.newBuilder()
                                                                                                      .setLogFilePath(loggingFilePath)
                                                                                                      .setChannelName("SIMPLIFIED_LOGGING_DIRECT_COMMUNICATION-")
                                                                                                      .build());
    }
}
