/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.utils;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class ObjectUtils {
    private ObjectUtils() { /* No instances allowed */ }

    /**
     * Checks if the provided interval is non null and bigger than {@link Duration#ZERO}
     * @param interval a {@link Duration}
     * @return the argument provided
     * @throws NullPointerException if the provided argument is null
     * @throws IllegalArgumentException if the provided interval is not positive
     */
    public static Duration checkNonNullPositiveTimeInterval(@Nonnull Duration interval) {
        return requiredCondition(Objects.requireNonNull(interval),
                                 nonNullInterval -> nonNullInterval.compareTo(Duration.ZERO) > 0,
                                 () -> new IllegalArgumentException("Provided interval is not positive"));
    }

    public static <ElementT extends Comparable<ElementT>, ExceptionT extends RuntimeException>
    ElementT requiredInRange(ElementT element, ElementT inclusiveRangeStart, ElementT inclusiveRangeEnd,
                             Supplier<ExceptionT> onError) {
        if (!(java.util.Objects.requireNonNull(inclusiveRangeEnd).compareTo(java.util.Objects.requireNonNull(inclusiveRangeStart)) < 0) &&
            java.util.Objects.requireNonNull(element).compareTo(inclusiveRangeStart) >= 0 &&
                element.compareTo(inclusiveRangeEnd) <= 0) {
            return element;
        }
        throw onError.get();
    }

    public static <ReturnT, OnErrorT extends RuntimeException>  ReturnT
    requiredCondition(ReturnT element, Predicate<ReturnT> controlPredicate,
                      Supplier<OnErrorT> onControlPredicateFailure) {
        if (controlPredicate.negate().test(element)) {
            throw onControlPredicateFailure.get();
        }
        return element;
    }
}