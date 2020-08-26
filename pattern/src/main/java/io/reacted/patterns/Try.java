/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class Try<T> {

    public static final Try<Void> VOID = Try.of(Try::noOp);
    private static final Try.Failure<NoSuchElementException> FILTER_FAILED = Try.ofFailure(new NoSuchElementException());

    private static final UnsupportedOperationException FAILURE_UNSUPPORTED_OPERATION_EXCEPTION =
            new UnsupportedOperationException("Failure has not result value");
    private static final UnsupportedOperationException SUCCESS_UNSUPPORTED_OPERATION_EXCEPTION =
            new UnsupportedOperationException("Success has no failure cause");

    private Try() { /* No object construction allowed */ }

    /**
     * Placeholder for a no operation
     *
     * @return Void
     */
    @SuppressWarnings("SameReturnValue")
    public static Void noOp(Object... args) {
        return null;
    }

    /**
     * Identity io.reacted.patterns.Try function. May be used as placeholder as mapper
     */
    public static <U> Try<U> identity(Try<U> value) {
        return value;
    }

    /**
     * Identity function. May be used as placeholder as mapper
     */
    public static <U> U identity(U value) {
        return value;
    }

    /**
     * @return true if the operation has been successful. False otherwise
     */
    public abstract boolean isSuccess();

    /**
     * @return true if the operation has failed, true otherwise
     */
    public boolean isFailure() {
        return !isSuccess();
    }

    /**
     * @return Return the cause of a failure
     */
    public abstract Throwable getCause();

    /**
     * This get will return the success value of a successful io.reacted.patterns.Try. Be aware that if the success
     * value is a NULL,
     * a io.reacted.patterns.Try.Success object will be returned whose value returned by get() is NULL
     * Nulls can safely be used as return values with io.reacted.patterns.Try, just remember that you will get what
     * you supply
     *
     * @return get the success value for this io.reacted.patterns.Try.
     */
    public abstract T get();

    /**
     * @return An optional containing a non null value on io.reacted.patterns.Try success, empty otherwise
     */
    public Optional<T> toOptional() {
        return isSuccess() ? Optional.ofNullable(get()) : Optional.empty();
    }

    /**
     * Converts the io.reacted.patterns.Try into a java Stream. If the io.reacted.patterns.Try is successful and
     * contains a non-null value,
     * a stream containing that values is returned, otherwise an empty stream.
     * <p>
     * anyCollection.stream()
     * .flatMap(element -> io.reacted.patterns.Try.of(() -> riskyCall(element)).stream())
     * .collect(...)
     *
     * @return A stream containing the io.reacted.patterns.Try a non-null value on success, empty otherwise
     */
    public Stream<T> stream() {
        return toOptional().stream();
    }

    /**
     * Applies a consumer on a resource. The resource is automatically closed regardless of the result of the
     * computation
     *
     * <pre>{@code
     * int stringLengthSum = io.reacted.patterns.Try.withResources(() -> Files.lines(path),
     *                                         inputStream -> inputStream.mapToInt(String::length)
     *                                                                   .sum())
     *                          .orElse(-1, Throwable::printStackTrace);
     * }</pre>
     *
     * @param resourceToResult Mapper function that takes as input the resource and returns a value
     * @param resourceSupplier Supplier for the AutoCloseable resource
     * @param <T>              Mapper return type
     * @param <A>              Type of the AutoCloseable resource
     * @return try of the mapper result
     */
    public static <T, A extends AutoCloseable>
    Try<T> withResources(TryResourceSupplier<? extends A> resourceSupplier,
                         TryMapper<? super A, ? extends T> resourceToResult) {
        try {
            try (A resource = Objects.requireNonNull(resourceSupplier).get()) {
                return Try.ofSuccess(Objects.requireNonNull(resourceToResult).apply(resource));
            }
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Applies a consumer on two resources. The resources are automatically closed regardless of the result of the
     * computation
     *
     * <pre>{@code
     * int stringLengthSum = io.reacted.patterns.Try.withResources(() -> Files.lines(path),
     *                                         () -> Files.lines(anotherPath),
     *                                         (firstInputStream, secondInputStream)
     *                                                          -> Stream.concat(firstInputStream, secondInputStream)
     *                                                                   .mapToInt(String::length).sum())
     *                          .orElse(-1, Throwable::printStackTrace);
     * }</pre>
     *
     * @param resourceToResult  Mapper function that takes as input two resources and returns a value
     * @param resourceSupplier1 Supplier for the first resource argument
     * @param resourceSupplier2 Supplier for the second resource argument
     * @param <T>               Return type of the mapper
     * @param <A>               Type of the first AutoCloseable resource
     * @param <A1>              Type of the second AutoCloseable resource
     * @return a try of the mapper result
     */
    public static <T, A extends AutoCloseable, A1 extends AutoCloseable>
    Try<T> withResources(TryResourceSupplier<? extends A> resourceSupplier1,
                         TryResourceSupplier<? extends A1> resourceSupplier2,
                         UnChecked.CheckedBiFunction<? super A, ? super A1, ? extends T> resourceToResult) {

        try {
            try (A resource1 = Objects.requireNonNull(resourceSupplier1).get();
                 A1 resource2 = Objects.requireNonNull(resourceSupplier2).get()) {
                return Try.ofSuccess(Objects.requireNonNull(resourceToResult).apply(resource1, resource2));
            }
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    public static <T, A extends AutoCloseable, A1 extends AutoCloseable>
    Try<T> withChainedResources(TryResourceSupplier<? extends A> resourceSupplier1,
                                TryMapper<A, ? extends A1> resourceMapper,
                                UnChecked.CheckedBiFunction<? super A, ? super A1, ? extends T> resourceToResult) {
        try {
            try (A resource1 = Objects.requireNonNull(resourceSupplier1.get());
                 A1 resource2 = Objects.requireNonNull(resourceMapper).apply(resource1)) {
                return Try.ofSuccess(Objects.requireNonNull(resourceToResult).apply(resource1, resource2));
            }
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Creates a io.reacted.patterns.Try monad with the result of a computation
     *
     * <pre>{@code io.reacted.patterns.Try.of(() -> anyCheckedOrUncheckedMethodYouWant(anyInput)) }</pre>
     *
     * @param supplier Supplier of the value contained in the io.reacted.patterns.Try
     * @param <T>      type contained in the io.reacted.patterns.Try
     * @return a io.reacted.patterns.Try containing the value generated by the supplier or failure
     */
    public static <T> Try<T> of(TryValueSupplier<? extends T> supplier) {
        try {
            return Try.ofSuccess(Objects.requireNonNull(supplier).get());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * @param function Callable used as supplier for the value of the io.reacted.patterns.Try
     * @param <T>      Type returned by the callable
     * @return a io.reacted.patterns.Try containing the value generated by the callable or failure
     */
    public static <T> Try<T> ofCallable(UnChecked.CheckedCallable<? extends T> function) {
        try {
            return Try.ofSuccess(Objects.requireNonNull(function).call());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * @param function a simple Runnable function
     * @return an anonymous io.reacted.patterns.Try containing, failed if it's the case, the exception thrown by the
     * Runnable
     */
    public static Try<Void> ofRunnable(UnChecked.CheckedRunnable function) {
        try {
            Objects.requireNonNull(function).run();
            return Try.VOID;
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Creates a successful io.reacted.patterns.Try carrying the specified valued
     *
     * @param value Value that is contained into the io.reacted.patterns.Try
     * @param <T>   type of the value
     * @return a successful io.reacted.patterns.Try
     */
    public static <T> Success<T> ofSuccess(T value) {
        return Success.valueOf(value);
    }

    /**
     * Creates a failed io.reacted.patterns.Try carrying the specified failure exception
     *
     * @param throwable failure value for the io.reacted.patterns.Try
     * @param <T>       type of the failed io.reacted.patterns.Try
     * @return a failed io.reacted.patterns.Try containing the specified throwable as cause
     */
    public static <T> Failure<T> ofFailure(Throwable throwable) {
        return Failure.valueOf(throwable);
    }

    /**
     * Gets the io.reacted.patterns.Try value on success, otherwise throws an exception generated by the provided
     * Throwable -> Throwable mapper
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> performComputation())).orElseThrow(ErrorOccurredException::new);
     * }</pre>
     *
     * @param throwableSupplier Supplier of the exception that is going to be thrown if the io.reacted.patterns.Try
     *                          is a failure
     * @param <X>               type of the thrown exception
     * @return the value contained in the io.reacted.patterns.Try if it is successful
     */
    @SuppressWarnings("UnusedReturnValue")
    public <X extends Throwable> T orElseThrow(Function<Throwable, X> throwableSupplier) throws X {
        if (!isSuccess()) {
            throw Objects.requireNonNull(throwableSupplier).apply(getCause());
        }
        return get();
    }

    /**
     * Gets the io.reacted.patterns.Try value on success, otherwise sneaky throws the exception that caused the failure
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> performComputation())).orElseSneakyThrow();
     * }</pre>
     *
     * @return the value contained in the io.reacted.patterns.Try if it is successful
     */
    public <X extends Throwable> T orElseSneakyThrow() throws X {
        if(isFailure()) {
            UnChecked.sneakyThrow(getCause());
        }
        return get();
    }


    /**
     * Gets the value contained in io.reacted.patterns.Try on success, otherwise returns the specified value.
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousComputation()).orElse(anotherObject);
     * }</pre>
     *
     * @param alternative An alternative value to return in case the io.reacted.patterns.Try is a failure
     * @param <U>         (sub)type of the alternative value
     * @return the original value contained into the io.reacted.patterns.Try or the alternative one
     */
    public <U extends T> T orElse(U alternative) {
        return isSuccess() ? get() : alternative;
    }

    /**
     * Gets the value contained in io.reacted.patterns.Try on success, otherwise returns the output of the provided
     * supplier.
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousComputation()).orElse(() -> generateAnotherObject());
     * }</code>
     *
     * @param alternative supplier for generating an alternative value in case the io.reacted.patterns.Try is a failure
     * @return the original value contained into the io.reacted.patterns.Try or the one generated by the supplier
     */
    public T orElseGet(Supplier<? extends T> alternative) {
        return isSuccess() ? get() : Objects.requireNonNull(alternative).get();
    }

    public T orElseGet(Supplier<? extends T> alternative,
                       Consumer<? super Throwable> exceptionConsumer) {
        if (isFailure()) {
            Objects.requireNonNull(exceptionConsumer).accept(getCause());
        }
        return isSuccess() ? get() : Objects.requireNonNull(alternative).get();
    }

    public T orElseGet(Function<? super Throwable, ? extends T> exceptionMapper) {
        return isSuccess() ? get() : exceptionMapper.apply(getCause());
    }

    /**
     * Gets the value contained in io.reacted.patterns.Try on success, otherwise returns the specified value and the exception that
     * generated the failure is processed with the provided consumer
     *
     * <pre>{@code
     *  var result = io.reacted.patterns.Try.of(() -> dangerousComputation()).orElse(anotherObject, LOGGER::print);
     *  }</code>
     *
     * @param alternative an alternative io.reacted.patterns.Try that should be returned in case the main one is a failure
     * @param exceptionConsumer on failure, apply this consumer to the generated Throwable before returning
     *                          the alternative value
     * @return the original io.reacted.patterns.Try or the alternative one
     */
    public T orElse(T alternative, Consumer<? super Throwable> exceptionConsumer) {
        if (isFailure()) {
            Objects.requireNonNull(exceptionConsumer).accept(getCause());
        }
        return isSuccess() ? get() : alternative;
    }

    /**
     * Gets the value contained in io.reacted.patterns.Try on success, otherwise maps the exception into a valid return value
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousComputation()).orElseRecover(exception -> exception.toString());
     * }</code>
     *
     * @param exceptionMapper a function that should not throw any exception that maps the throwable to a valid T value
     * @return the original io.reacted.patterns.Try or the mapped one
     */
    public T orElseRecover(Function<? super Throwable, ? extends T> exceptionMapper) {
        return isSuccess() ? get() : Objects.requireNonNull(exceptionMapper).apply(getCause());
    }

    /**
     * Gets the value contained in io.reacted.patterns.Try on success, otherwise returns the specified io.reacted.patterns.Try
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousComputation()).orElse(anotherTryMonad);
     * }</code>
     *
     * @param alternative an alternative io.reacted.patterns.Try that should be returned in case the main one is a failure
     * @return the original io.reacted.patterns.Try or the alternative one
     */
    @SuppressWarnings("unchecked")
    public Try<T> orElseTry(Try<? extends T> alternative) {
        return isSuccess() ? this : (Try<T>) Objects.requireNonNull(alternative);
    }

    /**
     * Gets the current io.reacted.patterns.Try or if it is a failure, the result of the provided TrySupplier
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousCall()).orElseTry(() -> anotherDangerousCall())
     * }</code>
     *
     * @param supplier supplier for an alternative value in case the main io.reacted.patterns.Try is a failure
     * @return the original io.reacted.patterns.Try or the alternative one
     */
    public Try<T> orElseTry(TryValueSupplier<? extends T> supplier) {
        if (isSuccess()) {
            return this;
        }
        try {
            return Try.ofSuccess(Objects.requireNonNull(supplier).get());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Gets the current io.reacted.patterns.Try or if it is a failure, the result of the provided TrySupplier and the specified consumer
     * process the exception that has generated the failure
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousCall()).orElseTry(() -> otherDangerousCall(), LOGGER::printFirstCallExc)
     * }</pre>
     *
     * @param supplier          supplier for an alternative value in case the main io.reacted.patterns.Try is a failure
     * @param exceptionConsumer on failure, apply this consumer to the generated Throwable before calling the
     *                          alternative value supplier
     * @return the original io.reacted.patterns.Try or the alternative one
     */
    public Try<T> orElseTry(TryValueSupplier<? extends T> supplier,
                            UnChecked.CheckedConsumer<? super Throwable> exceptionConsumer) {
        if (isSuccess()) {
            return this;
        }
        try {
            exceptionConsumer.accept(getCause());
            return Try.ofSuccess(Objects.requireNonNull(supplier).get());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Gets the current io.reacted.patterns.Try or if it is a failure maps the generated exception into an
     * alternative io.reacted.patterns.Try using
     * the specified mapper
     *
     * <pre>{@code
     * var result = io.reacted.patterns.Try.of(() -> dangerousCall()).orElseTry(throwable -> throwableToAnotherTry(throwable))
     * }</code>
     *
     * @param excToAlternative if the main io.reacted.patterns.Try is a failure, the generated exception is taken as
     *                         argument by this mapper for providing an alternate io.reacted.patterns.Try
     * @return the original value or the generated one if failure
     */
    @SuppressWarnings("unchecked")
    public Try<T> orElseTry(TryMapper<Throwable, Try<? extends T>> excToAlternative) {
        if (isSuccess()) {
            return this;
        }
        try {
            return (Try<T>)excToAlternative.apply(getCause());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Like Stream.flatMap(), io.reacted.patterns.Try.flatMap flattens two nested io.reacted.patterns.Try. If the io.reacted.patterns.Try is successful the flatMapping
     * function will be applied, otherwise the current io.reacted.patterns.Try will be returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> result = io.reacted.patterns.Try.of(() -> dangerousCall())
     *                         .flatMap(dangerousResult -> io.reacted.patterns.Try.of(() -> anotherDangerousCall(dangerousResult)))
     *                         .filter(secondCallResult -> !secondCallResult().isEmpty());
     * }</pre>
     *
     * @param flatMapper mapper that transforms the value of a successful io.reacted.patterns.Try into
     *                   another io.reacted.patterns.Try
     * @param <U>        type of the argument of the returned io.reacted.patterns.Try
     * @return a failure or the flatmapped io.reacted.patterns.Try
     */
    @SuppressWarnings("unchecked")
    public <U> Try<U> flatMap(TryMapper<? super T, Try<? extends U>> flatMapper) {
        Try<Try<? extends U>> unflatMap = map(flatMapper);
        return unflatMap.isSuccess() ? (Try<U>) unflatMap.get() : (Failure<U>) unflatMap;
    }

    /**
     * If the io.reacted.patterns.Try is successful, another io.reacted.patterns.Try will be returned with the
     * result of the specified mapper,
     * otherwise the current io.reacted.patterns.Try is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> failedMap = io.reacted.patterns.Try.of(TryTests::failingCheckedThrower).map(String::toLowerCase);
     * String result = failedMap.orElse(FAILURE);
     * }</pre>
     *
     * @param mapper mapper function for transforming a io.reacted.patterns.Try to another
     * @param <U>    type contained into the returned io.reacted.patterns.Try
     * @return the new io.reacted.patterns.Try on success or failure
     */
    public <U> Try<U> map(TryMapper<? super T, ? extends U> mapper) {
        if (isSuccess()) {
            try {
                return Try.ofSuccess(Objects.requireNonNull(mapper).apply(get()));
            } catch (Throwable error) {
                return Try.ofFailure(error);
            }
        } else {
            return (Failure<U>)this;
        }
    }

    /**
     * If the io.reacted.patterns.Try is a failure, the current io.reacted.patterns.Try is returned.
     * If the try Is a success, its value is tested with the specified predicate.
     * If such test is successful, the current io.reacted.patterns.Try is returned, otherwise a Failure is
     * returned
     *
     * <pre>{@code
     * String resultString = io.reacted.patterns.Try.of(() -> dangerousCallThatReturnsAString())
     *                          .filter(String::isUpperCase)
     *                          .orElse("DEFAULT_UPPERCASE_VALUE");
     * }</pre>
     *
     * @param predicate filter predicate for filtering a successful io.reacted.patterns.Try. Is a success try
     *                  does not pass the filter, a failed io.reacted.patterns.Try will be returned
     * @return a io.reacted.patterns.Try for which the predicate is true or failure
     */
    @SuppressWarnings("unchecked")
    public Try<T> filter(UnChecked.CheckedPredicate<? super T> predicate) {
        try {
            if (isFailure() ||
                isSuccess() && predicate.test(get())) {
                return this;
            }
            return (Try<T>)FILTER_FAILED;
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * If the io.reacted.patterns.Try is a failure, the current io.reacted.patterns.Try is returned.
     * If the try Is a success, its value is tested with the specified predicate.
     * If such test is successful, the current io.reacted.patterns.Try is returned, otherwise a Failure is
     * returned
     *
     * <pre>{@code
     * String resultString = io.reacted.patterns.Try.of(() -> dangerousCallThatReturnsAString())
     *                          .filter(String::isUpperCase)
     *                          .orElse("DEFAULT_UPPERCASE_VALUE");
     * }</pre>
     *
     * @param predicate filter predicate for filtering a successful io.reacted.patterns.Try. Is a succeeded try
     *                  does not pass the filter, a failed io.reacted.patterns.Try will be returned
     * @param  exceptionOnTestFailure if the predicate evaluates to false, the failure cause will be provided by this
     * @return a io.reacted.patterns.Try for which the predicate is true or failure
     */
    public Try<T> filter(UnChecked.CheckedPredicate<? super T> predicate,
                         Supplier<? extends Throwable> exceptionOnTestFailure) {
        try {
            if (isFailure() ||
                isSuccess() && predicate.test(get())) {
                    return this;
            }
            return Try.ofFailure(exceptionOnTestFailure.get());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * If the io.reacted.patterns.Try is a success, the provided consumer is applied to the result. A io.reacted
     * .patterns.Try is returned for checking
     * the success or the failure of the consumer
     *
     * <pre>{@code
     * io.reacted.patterns.Try<Void> consumerResult = io.reacted.patterns.Try.ofSuccess(SUCCESS).ifSuccess(LOGGER::printSuccess);
     * }</pre>
     *
     * @param consumer Consumer for the value of a successful io.reacted.patterns.Try
     * @return an anonymous io.reacted.patterns.Try for checking if the consumer execution was successful
     */
    public Try<Void> ifSuccess(TryConsumer<? super T> consumer) {
        return ifSuccessOrElse(consumer, Try::noOp);
    }

    /**
     * If io.reacted.patterns.Try is a failure, the provided consumer will be applied to the generated Throwable.
     * A io.reacted.patterns.Try is returned for checking the success or the failure of the consumer
     *
     * <pre>{@code
     * io.reacted.patterns.Try<Void> consumerResult = io.reacted.patterns.Try.ofFailure(AnyThrowable).ifError(LOGGER::printError);
     * }</code>
     *
     * @param consumer Consumer for the exception contained in a failed io.reacted.patterns.Try
     * @return an anonymous io.reacted.patterns.Try for checking if the consumer execution was successful
     */
    public Try<Void> ifError(TryConsumer<? super Throwable> consumer) {
        return ifSuccessOrElse(Try::noOp, consumer);
    }

    /**
     * Recover the status of a io.reacted.patterns.Try, if failed. If the io.reacted.patterns.Try is failed because of an exception of the specified class,
     * a new io.reacted.patterns.Try will be returned using the provided supplier. If the exception class does not match, the original
     * failed io.reacted.patterns.Try is returned.
     * Different recover calls can be chains for fine exception handling
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> catchSpecificException = io.reacted.patterns.Try.of(() -> riskyCallReturningString())
     *                                         .recover(StringIndexOutOfBoundException.class,
     *                                                 () -> io.reacted.patterns.Try.ofSuccess("Whooops"))
     * }</pre>
     *
     * @param exception           Exception class we want to intercept
     * @param successValGenerator Supplier for an alternate io.reacted.patterns.Try in case the provided exception
     *                            class is intercepted
     * @param <X>                 Exception type we want to intercept
     * @return a successful io.reacted.patterns.Try, the generated one in case the provided exception class is
     * intercepted or failure
     */
    public <X extends Throwable> Try<T> recover(Class<X> exception,
                                                TryValueSupplier<? extends T> successValGenerator) {
        if (isSuccess() ||
            !Objects.requireNonNull(exception).isAssignableFrom(getCause().getClass())) {
            return this;
        }
        try {
            return Try.ofSuccess(Objects.requireNonNull(successValGenerator).get());
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Recover the status of a io.reacted.patterns.Try, if failed. If the io.reacted.patterns.Try is failed because
     * of an exception of the specified class,
     * the provided io.reacted.patterns.Try will be returned. If the exception class does not match, the original
     * failed io.reacted.patterns.Try is returned
     * Different recover calls can be chains for fine exception handling
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> catchSpecificException = io.reacted.patterns.Try.of(() -> dangerousCallReturningString())
     *                                         .recover(StringIndexOutOfBoundException.class, io.reacted.patterns.Try.ofSuccess("Whooops"))
     * }</pre>
     *
     * @param exception    Exception class we want to intercept
     * @param successValue alternate io.reacted.patterns.Try to be returned in case the provided exception class is
     *                     intercepted
     * @param <X>          Exception type we want to intercept
     * @return a successful io.reacted.patterns.Try, the generated one in case the provided exception class is
     * intercepted or failure
     */
    @SuppressWarnings("unchecked")
    public <X extends Throwable> Try<T> recover(Class<X> exception, Try<? extends T> successValue) {
        if (isSuccess() ||
            !Objects.requireNonNull(exception).isAssignableFrom(getCause().getClass())) {
            return this;
        }
        return (Try<T>)successValue;
    }

    /**
     * Recover the status of a io.reacted.patterns.Try, if failed, a success Try is generated using the provided supplier
     * Will be returned the original io.reacted.patterns.Try if it is successful, otherwise the try returned by the
     * mapper
     * Different recover calls can be chains for fine exception handling
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> catchSpecificException = io.reacted.patterns.Try.of(() -> dangerousCallReturningString())
     *                                         .recover(StringIndexOutOfBoundException.class,
     *                                                 () -> io.reacted.patterns.Try.ofSuccess("Whooops"))
     * }</pre>
     *
     * @param exception       Exception class we want to intercept
     * @param successProvider Supplier for the successful try
     * @param <X>             Exception type we want to intercept
     * @return a io.reacted.patterns.Try representing the result of the alternative mapping process or the original
     * io.reacted.patterns.Try
     */
    @SuppressWarnings("unchecked")
    public <X extends Throwable> Try<T> recover(Class<X> exception,
                                                UnChecked.CheckedSupplier<Try<? extends T>> successProvider) {
        if (isSuccess() ||
            !Objects.requireNonNull(exception).isAssignableFrom(getCause().getClass())) {
            return this;
        }
        try {
            return (Try<T>)Objects.requireNonNull(successProvider).get();
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Recover the status of a io.reacted.patterns.Try, if failed, mapping the generated exception to another org
     * .reacted.patterns.Try
     * Will be returned the original io.reacted.patterns.Try if it is successful, otherwise the try returned by the
     * mapper
     * Different recover calls can be chains for fine exception handling
     *
     * <pre>{@code
     * io.reacted.patterns.Try<String> catchSpecificException = io.reacted.patterns.Try.of(() -> dangerousCallReturningString())
     *                                         .recover(StringIndexOutOfBoundException.class,
     *                                                 exception -> io.reacted.patterns.Try.ofSuccess("Whooops: " + exception))
     * }</pre>
     *
     * @param exception         Exception class we want to intercept
     * @param alternativeMapper Mapper that transforms an exception value to a valid result
     * @param <X>               Exception type we want to intercept
     * @return a io.reacted.patterns.Try representing the result of the alternative mapping process or the original
     * io.reacted.patterns.Try
     */
    public <X extends Throwable> Try<T> recover(Class<X> exception,
                                                TryMapper<? super Throwable, ? extends T> alternativeMapper) {
        try {
            if (isSuccess() ||
                !Objects.requireNonNull(exception).isAssignableFrom(getCause().getClass())) {
                return this;
            }
            return Try.ofSuccess(Objects.requireNonNull(alternativeMapper).apply(getCause()));
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * Check the status of the current io.reacted.patterns.Try and if it is a success apply the specified consumer to
     * its value.
     * The io.reacted.patterns.Try is left untouched and is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
     *    .peekSuccess(LOGGER::printDebugDiagnostics)
     * }</pre>
     *
     * @param ifSuccess consumer for the io.reacted.patterns.Try value in case the io.reacted.patterns.Try is a success
     * @return the untouched io.reacted.patterns.Try
     */
    public Try<T> peekSuccess(Consumer<? super T> ifSuccess) {
        return peek(ifSuccess, Try::noOp);
    }

    /**
     * Check if the current io.reacted.patterns.Try is a Failure and in that case apply the specified consumer to its
     * failure cause. The io.reacted.patterns.Try is left untouched and is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
     *    .peekFailure(LOGGER::printDebugDiagnostics)
     * }</pre>
     *
     * @param ifError if io.reacted.patterns.Try is a failure, consumer will be applied to the contained exception
     * @return the untouched io.reacted.patterns.Try
     */
    public Try<T> peekFailure(Consumer<? super Throwable> ifError) {
        return peek(Try::noOp, ifError);
    }

    /**
     * Check if the status of the current io.reacted.patterns.Try is a failure of the specified class and in that
     * case apply
     * the specified consumer to its failure cause. The io.reacted.patterns.Try is left untouched and is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> methodThrowingAnException(line))
     *    .peekFailure(ExceptionClassYouWantToIntercept.class, LOGGER::printDebugDiagnostics)
     * }</pre>
     *
     * @param ifError if io.reacted.patterns.Try is a failure, consumer will be applied to the contained exception
     * @return the untouched io.reacted.patterns.Try
     */
    @SuppressWarnings("UnusedReturnValue")
    public <ExceptionT extends Throwable> Try<T> peekFailure(Class<ExceptionT> throwableClass,
                                                             Consumer<ExceptionT> ifError) {
        return peek(throwableClass, Try::noOp, ifError);
    }

    /**
     * Check the status of the current io.reacted.patterns.Try and apply accordingly one of the provided consumers.
     * The io.reacted.patterns.Try is left untouched and is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
     *    .peek(LOGGER::print, LOGGER::debug)
     * }</pre>
     *
     * @param ifSuccess consumer for the io.reacted.patterns.Try value in case the io.reacted.patterns.Try is a success
     * @param ifError   consumer for the Throwable in case io.reacted.patterns.Try is a failure
     * @return the untouched io.reacted.patterns.Try
     */
    public Try<T> peek(Consumer<? super T> ifSuccess, Consumer<? super Throwable> ifError) {
        return peek(Throwable.class, ifSuccess, ifError);
    }

    /**
     * Check the status of the current io.reacted.patterns.Try and apply accordingly one of the provided consumers.
     * The ifError consumer is applied only if this io.reacted.patterns.Try is an error and the cause is of the
     * specified class.
     * The io.reacted.patterns.Try is left untouched and is returned
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> anyCheckedOrUncheckedMethodYouWant(line))
     *    .peek(SpecificException.class, LOGGER::print, LOGGER::debug)
     * }</pre>
     *
     * @param throwableClass specific throwable class we want to intercept for applying ifError
     * @param ifSuccess      consumer for the io.reacted.patterns.Try value in case the io.reacted.patterns.Try is a
     *                       success
     * @param ifError        consumer for the Throwable in case io.reacted.patterns.Try is a failure
     * @return the untouched io.reacted.patterns.Try
     */
    @SuppressWarnings("unchecked")
    public <ExceptionT extends Throwable> Try<T> peek(Class<? extends ExceptionT> throwableClass,
                                                      Consumer<? super T> ifSuccess,
                                                      Consumer<? super ExceptionT> ifError) {
        if (isSuccess()) {
            Objects.requireNonNull(ifSuccess).accept(get());
        } else if (Objects.requireNonNull(throwableClass).isAssignableFrom(getCause().getClass())) {
            Objects.requireNonNull(ifError).accept((ExceptionT)getCause());
        }
        return this;
    }


    /**
     * A functional if then else: if the io.reacted.patterns.Try is successful the ifSuccess mapper is used,
     * otherwise the
     * throwable mapper.
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> riskyCall())
     *    .mapOrElse(processOutput, exception -> io.reacted.patterns.Try.of(() -> tryToRecover()).orElse(PERMANENT_FAILURE))
     *    .orElse(DEFAULT_RESULT)
     * }</pre>
     *
     * @param ifSuccess mapper in case the io.reacted.patterns.Try is a success
     * @param orElse    mapper in case the io.reacted.patterns.Try is a failure
     * @param <U>       the type of the value of the resulting io.reacted.patterns.Try
     * @return a io.reacted.patterns.Try mapped accordingly with the status of the base object
     */
    public <U> Try<U> mapOrElse(TryMapper<? super T, ? extends U> ifSuccess,
                                TryMapper<? super Throwable, ? extends U> orElse) {
        try {
            if(isSuccess()) {
                return Try.ofSuccess(Objects.requireNonNull(ifSuccess).apply(get()));
            }
            return Try.ofSuccess(Objects.requireNonNull(orElse).apply(getCause()));
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    /**
     * A functional if then else: if the io.reacted.patterns.Try is successful the successConsumer is used, otherwise
     * the
     * throwable consumer.
     *
     * <pre>{@code
     * io.reacted.patterns.Try.of(() -> riskyCall())
     *    .ifSuccessOrElse(processOutput, LOGGER::warn)
     * }</pre>
     *
     * @param successConsumer consumer in case the io.reacted.patterns.Try is a success
     * @param failureConsumer consumer in case the io.reacted.patterns.Try is a failure
     * @return a io.reacted.patterns.Try representing the result of the applied consumer
     */
    public Try<Void> ifSuccessOrElse(TryConsumer<? super T> successConsumer,
                                     TryConsumer<? super Throwable> failureConsumer) {
        try {
            if (isSuccess()) {
                Objects.requireNonNull(successConsumer).accept(get());
            } else {
                Objects.requireNonNull(failureConsumer).accept(getCause());
            }
            return Try.VOID;
        } catch (Throwable error) {
            return Try.ofFailure(error);
        }
    }

    public interface TryMapper<T, R> extends UnChecked.CheckedFunction<T, R> {
        @Override
        R apply(T a) throws Throwable;
    }

    public interface TryResourceSupplier<T> extends UnChecked.CheckedSupplier<T> {
        @Override
        T get() throws Throwable;
    }

    public interface TryValueSupplier<T> extends UnChecked.CheckedSupplier<T> {
        @Override
        T get() throws Throwable;
    }

    public interface TryConsumer<T> extends UnChecked.CheckedConsumer<T> {
        @Override
        void accept(T argument) throws Throwable;
    }

    public static final class Success<T> extends Try<T> {

        private final T successValue;

        private Success() {
            /* Never Here */
            throw new IllegalStateException("Illegal " + this.getClass().getSimpleName() + " Object State");
        }

        private Success(T successValue) {
            this.successValue = successValue;
        }

        public static <T> Success<T> valueOf(T successValue) {
            return new Success<>(successValue);
        }

        /**
         * A successful io.reacted.patterns.Try holds a value that can be retrieved with this method. Please note that
         * if the success value is NULL, you will get a io.reacted.patterns.Try.Success whose value returned by this
         * get() is NULL
         * <p>
         * If it's the case, please consider to use the toOptional() and stream() methods for safely process
         * successful non-null result values
         *
         * @return returns the value contained in the io.reacted.patterns.Try
         */
        @Override
        public T get() {
            return successValue;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        /**
         * A successful io.reacted.patterns.Try does not have any cause
         *
         * @return None
         * @throws UnsupportedOperationException This operation is not allowed on Success
         */
        @Override
        public Throwable getCause() {
            throw SUCCESS_UNSUPPORTED_OPERATION_EXCEPTION;
        }
    }

    public static final class Failure<T> extends Try<T> {

        private final Throwable failureValue;

        private Failure() {
            /* Never Here */
            throw new IllegalStateException("Illegal " + this.getClass().getSimpleName() + " Object State");
        }

        private Failure(Throwable failureValue) {
            this.failureValue = failureValue;
        }

        public static <T> Failure<T> valueOf(Throwable failureValue) {
            return new Failure<>(Objects.requireNonNull(failureValue));
        }

        /**
         * A failed io.reacted.patterns.Try does not hold any value
         *
         * @return Nothing
         * @throws UnsupportedOperationException This operation is not allowed on a Failure
         */
        @Override
        public T get() {
            throw FAILURE_UNSUPPORTED_OPERATION_EXCEPTION;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public Throwable getCause() {
            return failureValue;
        }
    }
}
