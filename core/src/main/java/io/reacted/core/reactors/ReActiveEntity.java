package io.reacted.core.reactors;

import javax.annotation.Nonnull;

public interface ReActiveEntity {
    @Nonnull
    ReActions getReActions();
}