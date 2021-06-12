/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

@NonNullByDefault
@Immutable
public final class AsyncUtils {
    private AsyncUtils() { /* No implementations allowed */ }


    /**
     * Asynchronously executes {@code operation} {@code iterations} times. Every loops iteration begins when the
     * previous one is terminated. Silently ignore the errors and on error it maps the result of the iteration to
     * {@code onErrorAlternative}
     * 
     * @param operation operation to be ran in async
     * @param firstArgument argument for the first call
     * @param onErrorAlternative if the step results into an exception, provide this value as input for the next stage                      
     * @param iterations a positive number of iterations to be executed
     * @param <PayloadT> input type for the operation
     * @return a {@link CompletionStage}&lt;{@code PayloadT}&gt; that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              @Nullable PayloadT onErrorAlternative, long iterations) {
        return asyncLoop(operation, firstArgument, iterations, error -> onErrorAlternative,
                         ForkJoinPool.commonPool());
    }
    
    /**
     * Asynchronously executes {@code operation} {@code iterations} times. Every loops iteration begins when the
     * previous one is terminated
     *      
     * @param operation operation to be ran in async
     * @param firstArgument argument for the first call
     * @param onError mapper to handle exceptions in the middle of the loop                     
     * @param iterations a positive number of iterations to be executed
     * @param <PayloadT> input type for the operation
     * @return a {@link CompletionStage}&lt;{@code PayloadT}&gt; that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              Function<Throwable, PayloadT> onError, long iterations) {
        return asyncLoop(operation, firstArgument, iterations, onError, ForkJoinPool.commonPool());
    }

    /**
     * Asynchronously executes {@code operation} {@code iterations} times. Every loops iteration begins when the
     * previous one is terminated.
     * Silently ignore the errors and on error it maps the result of the iteration to {@code onErrorAlternative}
     * 
     * @param operation operation to be ran in async
     * @param firstArgument argument for the first call
     * @param iterations a positive number of iterations to be executed
     * @param <PayloadT> input type for the operation
     * @param onErrorAlternative mapper to handle exceptions in the middle of the loop
     * @param asyncExecutor async executor used to run the loop
     * @return a {@link CompletionStage}&lt;{@code PayloadT}&gt; that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              @Nullable PayloadT onErrorAlternative, long iterations, Executor asyncExecutor) {
        return asyncLoop(operation, firstArgument, iterations,
                         error -> onErrorAlternative,
                         asyncExecutor);
    }

    /**
     * Asynchronously executes {@code operation} {@code iterations} times. Every loops iteration begins when the
     * previous one is terminated
     * 
     * @param operation operation to be ran in async
     * @param firstArgument argument for the first call
     * @param iterations a positive number of iterations to be executed
     * @param <PayloadT> input type for the operation
     * @param onError mapper to handle exceptions in the middle of the loop
     * @param asyncExecutor async executor used to run the loop
     * @return a {@link CompletionStage}&lt;{@code PayloadT}&gt; that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              long iterations, Function<Throwable, PayloadT> onError, Executor asyncExecutor) {
        if (iterations <= 0) {
            throw new IllegalArgumentException("Iterations must be positive. Provided [" + iterations + "]");
        }
        var counter = new AtomicLong(0);
        return asyncLoop(operation, firstArgument, input ->  counter.getAndIncrement() < iterations,
                         onError, asyncExecutor);
    }

    public static <PayloadT> CompletionStage<Void>
    asyncForeach(Function<PayloadT, CompletionStage<PayloadT>> operation, Iterator<PayloadT> source,
                 Consumer<Throwable> onError, Executor asyncExecutor) {
        return source.hasNext() ? operation.apply(source.next())
                                           .exceptionally(error -> { onError.accept(error); return null;})
                                           .thenComposeAsync(prevStepResponse -> asyncForeach(operation, source, onError, asyncExecutor),
                                                             asyncExecutor)
                                : CompletableFuture.completedFuture(null);
    }
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              Predicate<PayloadT> shallContinue, Function<Throwable, PayloadT> onError,
              Executor asyncExecutor) {
        CompletableFuture<PayloadT> finalTrigger = new CompletableFuture<>();
        asyncExecutor.execute(() -> asyncMainLoop(operation, firstArgument, shallContinue, finalTrigger,
                                                  onError, asyncExecutor));
        return finalTrigger;
    }

    private static <PayloadT> CompletionStage<PayloadT>
    asyncMainLoop(Function<PayloadT, CompletionStage<PayloadT>> operation,
                  @Nullable PayloadT firstArgument, Predicate<PayloadT> shallContinue,
                  CompletionStage<PayloadT> finalTrigger, Function<Throwable, PayloadT> onError,
                  Executor executorService) {
        if (shallContinue.test(firstArgument)) {
            return operation.apply(firstArgument)
                            .exceptionally(onError)
                            .thenComposeAsync(result -> asyncMainLoop(operation, result, shallContinue, finalTrigger,
                                                                      onError, executorService), executorService);
        } else {
            finalTrigger.toCompletableFuture()
                        .complete(firstArgument);
            return finalTrigger;
        }
    }
}
