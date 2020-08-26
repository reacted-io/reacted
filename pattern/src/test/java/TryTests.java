/*
 * Copyright (c) 2020 , <Razvan Nicoara> [ razvan@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import io.reacted.patterns.Try;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ConstantConditions")
public class TryTests {

    private static final String SUCCESS = "Huge Success!";
    private static final String FAILURE = "Bad Failure!";

    @Test
    public void testFailWithRuntime() {
        Try<Character> result = Try.of(() -> "  ".charAt(123));
        assertFalse(result.isSuccess());
    }

    @Test
    public void testFailWithChecked() {
        Try<String> result = Try.of(TryTests::failingCheckedThrower);
        assertFalse(result.isSuccess());
        assertTrue(result.getCause() instanceof PrivateException);
    }

    @Test
    public void testSuccessWithChecked() {
        Try<String> result = Try.of(TryTests::successCheckedThrower);
        assertTrue(result.isSuccess());
    }

    @Test
    public void testFilterFailedTry() {
        Try<String> failedTry = Try.ofFailure(new PrivateException());
        Try<String> filterMonad = failedTry.filter(string -> true);
        assertFalse(filterMonad.isSuccess());
        assertTrue(filterMonad.getCause() instanceof PrivateException);
    }

    @Test
    public void testFilter() {
        Try<String> successFilterMonad = Try.of(TryTests::successCheckedThrower)
                                            .filter(retVal -> Objects.equals(retVal, SUCCESS));
        assertTrue(successFilterMonad.isSuccess());
        assertEquals(SUCCESS, successFilterMonad.get());

        Try<String> failFilterMonad = Try.of(TryTests::successCheckedThrower)
                                         .filter(retVal -> Objects.equals(retVal, SUCCESS + SUCCESS));
        assertFalse(failFilterMonad.isSuccess());
        assertTrue(failFilterMonad.getCause() instanceof NoSuchElementException);
    }

    @Test
    public void testIfSuccess() {
        Try.ofFailure(new PrivateException())
           .ifSuccess(obj -> Assertions.fail());

        final AtomicInteger checks = new AtomicInteger(0);

        Try.ofSuccess(SUCCESS)
           .ifSuccess(obj -> checks.incrementAndGet());

        assertEquals(1, checks.get());
    }

    @Test
    public void testIfError() {
        Try<Void> voidTry = Try.ofFailure(new PrivateException())
                               .ifError(exc -> { });
        assertTrue(voidTry.isSuccess());
        Try<Void> voidFailTry = Try.ofFailure(new PrivateException())
                                   .ifError(exc -> {
                                       throw new PrivateException();
                                   });
        assertFalse(voidFailTry.isSuccess());
        assertTrue(voidFailTry.getCause() instanceof PrivateException);

        Try<Void> voidSuccessTry = Try.ofSuccess(SUCCESS)
                                      .ifError(exc -> {
                                          throw new PrivateException();
                                      });
        assertTrue(voidSuccessTry.isSuccess());
    }

    @Test
    public void testMapOrElse() {
        Try<String> elseResult = Try.ofFailure(new PrivateException())
                                    .mapOrElse(dummySuccess -> {
                                                   Assertions.fail();
                                                   return null;
                                               },
                                               exception -> SUCCESS);
        assertTrue(elseResult.isSuccess());
        assertEquals(elseResult.get(), SUCCESS);

        Try<String> elseFailure = Try.ofFailure(new PrivateException())
                                     .mapOrElse(dummySuccess -> {
                                                    Assertions.fail();
                                                    return null;
                                                },
                                                exception -> failingCheckedThrower());
        assertFalse(elseFailure.isSuccess());

        Try<String> mapFailure = Try.ofSuccess(SUCCESS)
                                    .mapOrElse(success -> failingCheckedThrower(),
                                               exception -> {
                                                   Assertions.fail();
                                                   return null;
                                               });

        assertFalse(mapFailure.isSuccess());

        Try<String> mapSuccess = Try.ofSuccess(SUCCESS)
                                    .mapOrElse(String::toUpperCase,
                                               exception -> {
                                                   Assertions.fail();
                                                   return null;
                                               });
        assertTrue(mapSuccess.isSuccess());
        assertEquals(SUCCESS.toUpperCase(), mapSuccess.get());

        String elseRecover = (String) Try.ofFailure(new PrivateException()).orElseRecover(exception -> SUCCESS);
        assertEquals(SUCCESS, elseRecover);

        String elseNotRecover = Try.of(() -> SUCCESS).orElseRecover(exc -> FAILURE);
        assertEquals(SUCCESS, elseNotRecover);

    }

    @Test
    public void testRecover() {
        Try<String> failedRecover = Try.of(() -> SUCCESS.substring(0, SUCCESS.length() * 2))
                                       .recover(PrivateException.class, Try.ofSuccess(SUCCESS));
        assertFalse(failedRecover.isSuccess());
        assertTrue(failedRecover.getCause() instanceof StringIndexOutOfBoundsException);

        Try<String> recovered = Try.of(() -> SUCCESS.substring(0, SUCCESS.length() * 2))
                                   .recover(StringIndexOutOfBoundsException.class, Try.ofSuccess(SUCCESS));
        assertTrue(recovered.isSuccess());
        assertEquals(SUCCESS, recovered.get());

        Try<String> recoverBySupplier = Try.of(() -> SUCCESS.substring(0, SUCCESS.length() * 2))
                                           .recover(StringIndexOutOfBoundsException.class, () -> SUCCESS);
        assertTrue(recoverBySupplier.isSuccess());
        assertEquals(SUCCESS, recoverBySupplier.get());

        Try<Object> failedRecoverMapper = Try.ofFailure(new PrivateException())
                                             .recover(PrivateException.class,
                                                      exception -> exception.getStackTrace()[Integer.MAX_VALUE]);
        assertFalse(failedRecoverMapper.isSuccess());
        assertTrue(failedRecoverMapper.getCause() instanceof IndexOutOfBoundsException);

        Try<Object> successfulRecoverMapper = Try.ofFailure(new PrivateException())
                                                 .recover(PrivateException.class, exc -> SUCCESS + exc.toString());

        assertTrue(successfulRecoverMapper.isSuccess());
        assertTrue(successfulRecoverMapper.get().toString().contains("PrivateException"));

        Try<Object> successfulRecoverMapperWithSuperException = Try.ofFailure(new PrivateException())
                                                                   .recover(Exception.class,
                                                                            exc -> SUCCESS + exc.toString());

        assertTrue(successfulRecoverMapperWithSuperException.isSuccess());
        assertTrue(successfulRecoverMapperWithSuperException.get().toString().contains("PrivateException"));
    }

    @Test
    public void testOrElseTry() {

        assertTrue(Try.ofFailure(new PrivateException()).orElseTry(Try.ofSuccess(SUCCESS)).isSuccess());
        assertTrue(Try.ofFailure(new PrivateException()).orElseTry(() -> Try.ofSuccess(SUCCESS)).isSuccess());
        assertTrue(Try.ofFailure(new PrivateException()).orElseTry(tryVal -> {
            if (tryVal instanceof PrivateException) {
                return Try.ofSuccess(SUCCESS);
            }
            throw new IllegalStateException(new PrivateException());
        }).isSuccess());
        assertTrue(Try.ofFailure(new PrivateException()).orElseTry(() -> Try.ofSuccess(SUCCESS)).isSuccess());
        assertTrue(Try.ofFailure(new PrivateException()).orElseTry(tryVal -> {
            if (tryVal instanceof PrivateException) {
                return Try.ofSuccess(SUCCESS);
            }
            throw new PrivateException();
        }).isSuccess());

        assertFalse(Try.ofFailure(new PrivateException()).orElseTry(() -> {
            throw new PrivateException();
        }).isSuccess());
        assertTrue(Try.ofSuccess(new PrivateException()).orElseTry(() -> {
            throw new PrivateException();
        }).isSuccess());

        assertEquals(SUCCESS, Try.ofFailure(new PrivateException()).orElse(SUCCESS));
        assertEquals(SUCCESS, Try.ofFailure(new PrivateException()).orElseGet(() -> SUCCESS));

        AtomicBoolean hasBeenExecuted = new AtomicBoolean(false);

        String tryAndConsumeException = (String) Try.ofFailure(new PrivateException())
                                                    .orElse(SUCCESS, exception -> hasBeenExecuted.set(true));

        assertEquals(SUCCESS, tryAndConsumeException);
        assertTrue(hasBeenExecuted.get());

        hasBeenExecuted.set(false);
        boolean tryConsumeException = Try.ofSuccess(true)
                                         .orElse(false, exception -> hasBeenExecuted.set(true));
        assertTrue(tryConsumeException);
        assertFalse(hasBeenExecuted.get());

    }

    @Test
    public void testOrElseThrowFailure() {
        Assertions.assertThrows(IllegalStateException.class,
                                () -> Try.ofFailure(new PrivateException()).orElseThrow(IllegalStateException::new));
    }

    @Test
    public void testOrElseThrowSuccess() {
        Try.ofSuccess(SUCCESS).orElseThrow(IllegalStateException::new);
    }

    @Test
    public void testMap() {
        Try<String> successMap = Try.ofSuccess(SUCCESS).map(String::toLowerCase);
        assertEquals(SUCCESS.toLowerCase(), successMap.orElse(FAILURE));

        Try<String> failedMap = Try.of(TryTests::failingCheckedThrower).map(String::toLowerCase);
        assertEquals(FAILURE, failedMap.orElse(FAILURE));
    }

    @Test
    public void testFlatMap() {
        assertTrue(Try.ofSuccess(SUCCESS)
                      .flatMap(str -> Try.of(() -> str.charAt(0)))
                      .filter(chr -> chr == SUCCESS.charAt(0)).isSuccess());

        assertFalse(Try.ofFailure(new PrivateException())
                       .flatMap(obj -> Try.ofSuccess(SUCCESS))
                       .isSuccess());

        assertFalse(Try.ofSuccess(SUCCESS)
                       .flatMap(str -> {
                           throw new PrivateException();
                       })
                       .isSuccess());

        assertEquals("Real Success", Try.ofSuccess(SUCCESS)
                                        .flatMap(success -> Try.ofSuccess("Real Success"))
                                        .get());
    }

    @Test
    public void testWithResources() {
        Closeable<String> checkIfClosed = Closeable.valueOf(SUCCESS);
        Try<String> autoCloseTry = Try.withResources(() -> checkIfClosed,
                                                     resource -> resource.getValue().toUpperCase());

        assertTrue(autoCloseTry.isSuccess());
        assertEquals(SUCCESS.toUpperCase(), autoCloseTry.get());
        assertTrue(checkIfClosed.isClosed());

        Closeable<String> checkIfClosed1 = Closeable.valueOf(SUCCESS);
        Closeable<String> checkIfClosed2 = Closeable.valueOf(SUCCESS);

        Try<String> autoClose2Try;
        autoClose2Try = Try.withResources(() -> checkIfClosed1, () -> checkIfClosed2,
                                          (resource1, resource2) -> resource1.getValue() + resource2.getValue());

        assertTrue(autoClose2Try.isSuccess());
        assertEquals(SUCCESS + SUCCESS, autoClose2Try.get());
        assertTrue(checkIfClosed1.isClosed());
        assertTrue(checkIfClosed2.isClosed());

        Try<String> failResource = Try.withResources(() -> {
                                                         throw new PrivateException();
                                                     },
                                                     resource -> {
                                                         Assertions.fail();
                                                         return null;
                                                     });

        assertFalse(failResource.isSuccess());
    }

    @Test
    public void testSpecificErrorPeeking() {
        AtomicBoolean hasBeenExecuted = new AtomicBoolean(false);
        Try<String> failedTry = Try.ofFailure(new PrivateException());
        failedTry.peekFailure(NullPointerException.class, exc -> hasBeenExecuted.set(true));
        assertFalse(hasBeenExecuted.get());
        failedTry.peek(NullPointerException.class,
                       success -> hasBeenExecuted.set(true),
                       exc -> hasBeenExecuted.set(true));
        assertFalse(hasBeenExecuted.get());
        failedTry.peekFailure(PrivateException.class, exc -> hasBeenExecuted.set(true));
        assertTrue(hasBeenExecuted.get());
    }

    public void usageExample() {
        final Path path = null; //Any path you want
        long stringLengthSum = Try.withResources(() -> Files.lines(path),
                                                 TryTests::manageStream1 /* 1, 2, 3, 4, 5, 6, 7 */)
                                  .orElse(-1L, Throwable::printStackTrace);
    }

    private static long manageStream1(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    .map(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
                                    .recover(IndexOutOfBoundsException.class, Try.ofSuccess("RECOVER")))
                    // mapOrElse expects checked mappers, so it always returns a io.reacted.patterns.Try. In this
                    // specific case
                    // since we are sure that our trymappers are not going to throw anything we could have used
                    // mapOrElse(...).get() instead of mapOrElse(...).orElse(...)
                    .mapToInt(tryMonad -> tryMonad.mapOrElse(String::length, exception -> 0)
                                                  .orElse(0))
                    .sum();
    }

    private static long manageStream2(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    .map(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
                                    .recover(IndexOutOfBoundsException.class, Try.ofSuccess("RECOVER")))
                    // map is applied only on success, so the map below avoids all the failures not related to
                    // index out of bounds exceptions (because we recovered it on the previous step)
                    .map(tryMonad -> tryMonad.map(String::length))
                    // if the monad holds failure, convert it to a 0. We could have achieved the same result
                    // filtering on isSuccess and get()-ting
                    .mapToInt(tryMonad -> tryMonad.orElse(0))
                    .sum();
    }

    private static long manageStream3(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    .flatMap(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
                                        .recover(IndexOutOfBoundsException.class, Try.ofSuccess("RECOVER"))
                                        .stream())
                    .mapToInt(String::length)
                    .sum();
    }

    private static long manageStream4(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    .map(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
                                    .recover(IndexOutOfBoundsException.class, Try.ofSuccess("RECOVER"))
                                    .toOptional())
                    .flatMap(Optional::stream)
                    .mapToInt(String::length)
                    .sum();
    }

    private static long manageStream5(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    .flatMap(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
                                        .peekFailure(Throwable::printStackTrace)
                                        .recover(IndexOutOfBoundsException.class, Try.ofSuccess("RECOVER"))
                                        .stream())
                    .mapToInt(String::length)
                    .sum();
    }

    private static long manageStream6(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    /* Simply ignore all the exceptions in this case */
                    .flatMap(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line)).stream())
                    .mapToInt(String::length)
                    .sum();
    }

    private static long manageStream7(Stream<String> input) {
        return input.filter(line -> line.contains("Any condition you want"))
                    // Simply ignore all the exceptions in this case
                    .map(line -> Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line)))
                    // the below monad can be failed because of an exception in anyCheckedOrUncheckedMethodYouWant
                    // or of the evaluation to false of the predicate. Stream() will cut out the failed monads
                    .flatMap(tryMonad -> tryMonad.filter(readString -> !readString.isEmpty()).stream())
                    .mapToInt(String::length)
                    .sum();
    }

    private static String anyCheckedOrUncheckedMethodYouWant(String inputFileLine)
            throws IndexOutOfBoundsException {
        if (ThreadLocalRandom.current().nextInt() % 123 == 0) {
            throw new IllegalStateException(new PrivateException());
        }
        return inputFileLine.substring(0, ThreadLocalRandom.current().nextInt());
    }

    private static String failingCheckedThrower() throws PrivateException {
        throw new PrivateException();
    }

    @SuppressWarnings("SameReturnValue")
    private static String successCheckedThrower() {
        return SUCCESS;
    }

    static class PrivateException extends Exception {
    }

    static class Closeable<T> implements AutoCloseable {
        private boolean closed = false;
        private final T value;

        private Closeable(T value) {
            this.value = value;
        }

        public static <T> Closeable<T> valueOf(T value) {
            return new Closeable<>(value);
        }

        public T getValue() {
            return value;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}

