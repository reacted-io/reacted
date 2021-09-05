/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.drivers.local;

import io.reacted.core.config.drivers.DirectCommunicationConfig;
import io.reacted.core.config.drivers.DirectCommunicationLoggerConfig;
import io.reacted.core.config.drivers.DirectCommunicationSimplifiedLoggerConfig;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import java.io.FileNotFoundException;
import java.io.PrintStream;

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
    getDirectCommunicationSimplifiedLoggerDriver(String loggingFilePath)
        throws FileNotFoundException {
        return getDirectCommunicationSimplifiedLoggerDriver(new PrintStream(loggingFilePath));
    }
    /**
     * Returns a {@link DirectCommunicationDriver} for <b>local</b> communication that after sending each message
     * logs the main information of the message in a file. It is a less noisy version of {@link DirectCommunicationLoggerDriver}
     *
     * @param loggingPrintStream {@link PrintStream} that should be used for printing the messages that are
     *                           exchanged within the local {@link ReActorSystem}
     *
     *                           Note: the supplied {@link PrintStream } will be automatically flushed
     *                                 and closed on driver termination
     * @throws java.io.UncheckedIOException if an error occurs opening the file
     * @return the {@link DirectCommunicationSimplifiedLoggerDriver}
     */
    public static DirectCommunicationSimplifiedLoggerDriver
    getDirectCommunicationSimplifiedLoggerDriver(PrintStream loggingPrintStream) {
        return new DirectCommunicationSimplifiedLoggerDriver(DirectCommunicationSimplifiedLoggerConfig.newBuilder()
                                                                                                      .setLogStream(loggingPrintStream)
                                                                                                      .setChannelName("SIMPLIFIED_LOGGING_DIRECT_COMMUNICATION-")
                                                                                                      .build());
    }
}
