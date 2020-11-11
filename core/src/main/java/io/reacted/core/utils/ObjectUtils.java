/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.utils;

import io.reacted.patterns.NonNullByDefault;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@NonNullByDefault
public final class ObjectUtils {
    private ObjectUtils() { /* No instances allowed */ }

    @Nullable
    public static <InputT, OutputT> OutputT ifNotNull(@Nullable InputT input, Function<InputT, OutputT> ifNotNull) {
        if (input == null) {
            return null;
        }
        return Objects.requireNonNull(ifNotNull).apply(input);
    }

    public static <InputT> void runIfNotNull(@Nullable InputT input, Consumer<InputT> ifNotNull) {
        if (input != null) {
            Objects.requireNonNull(ifNotNull).accept(input);
        }
    }

    /**
     * Checks if the provided interval is non null, bigger than {@link Duration#ZERO} and smaller than
     * {@code limitAmount} {@code limitUnit}
     * @param interval a {@link Duration}
     * @param limitAmount inclusive upperbound of the allowed duration
     * @param limitUnit {@link TimeUnit} of the {@code limitAmount}
     * @return the argument provided
     * @throws NullPointerException if the provided argument is null
     * @throws IllegalArgumentException if the provided interval is not positive
     */
    public static Duration checkNonNullPositiveTimeIntervalWithLimit(@Nullable Duration interval, long limitAmount,
                                                                     TimeUnit limitUnit) {
        return requiredCondition(checkNonNullPositiveTimeInterval(interval),
                                 positiveInterval -> positiveInterval.compareTo(Duration.of(limitAmount,
                                                                                            limitUnit.toChronoUnit())) <= 0,
                                 () -> new IllegalArgumentException("Provided interval is not within upperbound limit"));

    }

    /**
     * Checks if the provided interval is non null and bigger than {@link Duration#ZERO}
     * @param interval a {@link Duration}
     * @return the argument provided
     * @throws NullPointerException if the provided argument is null
     * @throws IllegalArgumentException if the provided interval is not positive
     */
    public static Duration checkNonNullPositiveTimeInterval(@Nullable Duration interval) {
        return requiredCondition(Objects.requireNonNull(interval),
                                 nonNullInterval -> nonNullInterval.compareTo(Duration.ZERO) > 0,
                                 () -> new IllegalArgumentException("Provided interval is not positive"));
    }

    public static <ElementT extends Comparable<ElementT>, ExceptionT extends RuntimeException>
    ElementT requiredInRange(ElementT element, ElementT inclusiveRangeStart, ElementT inclusiveRangeEnd,
                             Supplier<ExceptionT> onError) {
        if (!(Objects.requireNonNull(inclusiveRangeEnd).compareTo(Objects.requireNonNull(inclusiveRangeStart)) < 0) &&
              Objects.requireNonNull(element).compareTo(inclusiveRangeStart) >= 0 &&
              element.compareTo(inclusiveRangeEnd) <= 0) {
            return element;
        }
        throw onError.get();
    }

    public static <ReturnT, OnErrorT extends RuntimeException>  ReturnT
    requiredCondition(ReturnT element, Predicate<ReturnT> controlPredicate,
                      Supplier<OnErrorT> onControlPredicateFailure) {
        if (Objects.requireNonNull(controlPredicate)
                   .negate().test(Objects.requireNonNull(element))) {
            throw Objects.requireNonNull(onControlPredicateFailure).get();
        }
        return element;
    }

    public static byte[] toBytes(Properties properties) throws IOException {
        var byteArrayOutputStream = new ByteArrayOutputStream();
        properties.store(byteArrayOutputStream,"");
        return byteArrayOutputStream.toByteArray();
    }

    public static Properties fromBytes(byte[] data) throws IOException {
        var byteArrayInputStream = new ByteArrayInputStream(data);
        var properties = new Properties();
        properties.load(byteArrayInputStream);
        return properties;
    }
}