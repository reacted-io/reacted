/*
 * Copyright (c) 2021 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.config.drivers;

import io.reacted.patterns.NonNullByDefault;

import java.io.PrintStream;
import java.util.Objects;

@NonNullByDefault
public class DirectCommunicationSimplifiedLoggerConfig extends ChannelDriverConfig<DirectCommunicationSimplifiedLoggerConfig.Builder, DirectCommunicationSimplifiedLoggerConfig> {
    private final PrintStream printStream;
    private DirectCommunicationSimplifiedLoggerConfig(Builder builder) {
        super(builder);
        this.printStream = Objects.requireNonNull(builder.printStream,
                                                  "PrintStream cannot be null");
    }

    public PrintStream getPrintStream() { return printStream; }

    public static DirectCommunicationSimplifiedLoggerConfig.Builder newBuilder() { return new Builder(); }

    public static class Builder extends ChannelDriverConfig.Builder<DirectCommunicationSimplifiedLoggerConfig.Builder, DirectCommunicationSimplifiedLoggerConfig> {
        @SuppressWarnings("NotNullFieldNotInitialized")
        private PrintStream printStream;
        private Builder() { }

        /**
         * Define the output stream for dumping the messages.
         *
         * @param printStream A {@link PrintStream } that will be used to print the messages exchanged
         *                    within the local {@link io.reacted.core.reactorsystem.ReActorSystem}
         *
         *                    Note: The supplied stream will be automatically flushed and closed
         *                          on driver termination
         * @return this builder
         */
        public final Builder setLogStream(PrintStream printStream) {
            this.printStream = printStream;
            return this;
        }

        @Override
        public DirectCommunicationSimplifiedLoggerConfig build() {
            return new DirectCommunicationSimplifiedLoggerConfig(this);
        }
    }
}
