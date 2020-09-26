package io.reacted.core.messages.reactors;

import io.reacted.patterns.NonNullByDefault;

import java.io.Serializable;
import java.util.Objects;

@NonNullByDefault
public abstract class LogMessage implements Serializable {
    private final String format;
    private final Serializable[] arguments;
    public LogMessage(String format, Serializable ...arguments) {
        this.format = Objects.requireNonNull(format);
        this.arguments = Objects.requireNonNull(arguments);
    }

    public String getFormat() {
        return format;
    }

    public Serializable[] getArguments() {
        return arguments;
    }
}
