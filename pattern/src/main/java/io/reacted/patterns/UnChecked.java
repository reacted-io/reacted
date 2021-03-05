/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class UnChecked {

    private UnChecked() { }

    public static <T> Supplier<T> supplier(CheckedSupplier<T> checkedSupplier) {
        return unchecker(checkedSupplier);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static <T> Consumer<T> consumer(CheckedConsumer<T> checkedConsumer) {
        return arg -> unchecker(() -> {
            checkedConsumer.accept(arg);
            return Try.VOID;
        });
    }

    public static <T, U, D> TriConsumer<T, U, D> triConsumer(CheckedTriConsumer<T, U, D> checkedTriConsumer) {
        return (arg1, arg2, arg3) -> unchecker(() -> { checkedTriConsumer.accept(arg1, arg2, arg3);
                                                       return Try.VOID; }).get();
    }
    @SuppressWarnings("ReturnValueIgnored")
    public static <T, U> BiConsumer<T, U> biConsumer(CheckedBiConsumer<T, U> checkedBiConsumer) {
        return (arg1, arg2) -> unchecker(() -> { checkedBiConsumer.accept(arg1, arg2); return Try.VOID; }).get();
    }

    public static <T, R> Function<T, R> function(CheckedFunction<T, R> checkedFunction) {
        return arg -> unchecker(() -> checkedFunction.apply(arg)).get();
    }

    public static <T, U, R> BiFunction<T, U, R> biFunction(CheckedBiFunction<T, U, R> checkedBifunction) {
        return (arg1, arg2) -> unchecker(() -> checkedBifunction.apply(arg1, arg2)).get();
    }

    public static <T> UnaryOperator<T> unaryOperator(CheckedUnaryOperator<T> checkedUnaryOperator) {
        return arg1 -> unchecker(() -> checkedUnaryOperator.apply(arg1)).get();
    }

    public static <T> BinaryOperator<T> binaryOperator(CheckedBinaryOperator<T> checkedBinaryOperator) {
        return (arg1, arg2) -> unchecker(() -> checkedBinaryOperator.apply(arg1, arg2)).get();
    }

    public static <T> Predicate<T> predicate(CheckedPredicate<T> checkedPredicate) {
        return arg -> unchecker(() -> checkedPredicate.test(arg)).get();
    }

    @FunctionalInterface
    public interface TriConsumer<T, U, D> {
        void accept(T arg1, U arg2, D arg3);
    }
    @SuppressWarnings("EmptyMethod")
    @FunctionalInterface
    public interface CheckedSupplier<T> {
        T get() throws Throwable;
    }

    @SuppressWarnings("EmptyMethod")
    @FunctionalInterface
    public interface CheckedConsumer<T> {
        void accept(T arg1) throws Throwable;
    }

    @SuppressWarnings("RedundantThrows")
    @FunctionalInterface
    public interface CheckedBiConsumer<T, U> {
        void accept(T arg1, U arg2) throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedTriConsumer<T, U, D> {
        void accept(T arg1, U arg2, D arg3) throws Throwable;
    }

    @SuppressWarnings("EmptyMethod")
    @FunctionalInterface
    public interface CheckedFunction<T, U> {
        U apply(T arg1) throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedBiFunction<T, U, R> {
        R apply(T arg1, U arg2) throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedUnaryOperator<T> extends CheckedFunction<T, T> {
        static <T> CheckedUnaryOperator<T> identity() {
            return t -> t;
        }
    }

    @FunctionalInterface
    public interface CheckedBinaryOperator<T> extends CheckedBiFunction<T, T, T> { }

    @FunctionalInterface
    public interface CheckedPredicate<T> {
        boolean test(T arg) throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedCallable<T> {
        T call() throws Throwable;
    }

    private static <T> Supplier<T> unchecker(CheckedSupplier<T> resultSupplier) {
        return () -> Try.of(resultSupplier::get)
                        .orElseRecover(UnChecked::sneakyThrow);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable, U> U sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }
}
