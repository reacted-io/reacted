/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import io.reacted.patterns.UnChecked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UncheckedTests {

    private static <T> void throwerConsumer(T argument) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T> boolean throwerPredicate(T argument) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T> T throwerSupplier() throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T, U> void throwerBiConsumer(T a, U b) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T, R> R throwerFunction(T argument) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T, U, R> R throwerBiFunction(T a, U b) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T> T throwerUnaryOperator(T a) throws Exception {
        throw new Exception("Just Fail");
    }

    private static <T> T throwerBinaryOperator(T a, T b) throws Exception {
        throw new Exception("Just Fail");
    }

    @Test
    public void test_consumer() {
        UnChecked.consumer(UncheckedTests::throwerConsumer).accept(null);
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_predicate() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.predicate(UncheckedTests::throwerPredicate).test(null));
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_supplier() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.supplier(UncheckedTests::throwerSupplier).get());
    }

    @Test
    public void test_biconsumer() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.biConsumer(UncheckedTests::throwerBiConsumer).accept(null, null));
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_function() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.function(UncheckedTests::throwerFunction).apply(null));
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_bifunction() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.biFunction(UncheckedTests::throwerBiFunction).apply(null, null));
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_unaryoperator() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.unaryOperator(UncheckedTests::throwerUnaryOperator).apply(null));
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored")
    public void test_binaryoperator() {
        Assertions.assertThrows(Exception.class,
                                () -> UnChecked.binaryOperator(UncheckedTests::throwerBinaryOperator).apply(null,
                                                                                                            null));
    }
}

