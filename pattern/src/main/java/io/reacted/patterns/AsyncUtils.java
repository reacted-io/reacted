/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.patterns;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.processing.Completions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Supplier;

@NonNullByDefault
@Immutable
public final class AsyncUtils {
    private AsyncUtils() { /* No implementations allowed */ }

    public static CompletionStage<Void> ifThenElse(boolean condition,
                                                   Supplier<CompletionStage<?>> ifTrue,
                                                   Supplier<CompletionStage<?>> ifFalse) {
        return (condition ? ifTrue : ifFalse).get().thenAccept(res -> {});
    }

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
     * @return a {@link CompletionStage&lt;{@code PayloadT}&gt;} that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              @Nullable PayloadT onErrorAlternative, long iterations) {
        return asyncLoop(operation, firstArgument, iterations,
                         error -> CompletableFuture.completedFuture(onErrorAlternative),
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
     * @return a {@link CompletionStage&lt;{@code PayloadT}&gt;} that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              Function<Throwable, CompletionStage<PayloadT>> onError, long iterations) {
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
     * @return a {@link CompletionStage&lt;{@code PayloadT}&gt;} that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              @Nullable PayloadT onErrorAlternative, long iterations, Executor asyncExecutor) {
        return asyncLoop(operation, firstArgument, iterations,
                         error -> CompletableFuture.completedFuture(onErrorAlternative),
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
     * @return a {@link CompletionStage&lt;{@code PayloadT}&gt;} that is going to contain the output of the last
     * operation
     * @throws IllegalArgumentException if {@code iterations} is negative
     */
    public static <PayloadT> CompletionStage<PayloadT>
    asyncLoop(Function<PayloadT, CompletionStage<PayloadT>> operation, @Nullable PayloadT firstArgument,
              long iterations, Function<Throwable, CompletionStage<PayloadT>> onError, Executor asyncExecutor) {
        if (iterations <= 0) {
            throw new IllegalArgumentException("Iterations must be positive. Provided [" + iterations + "]");
        }
        CompletableFuture<PayloadT> finalTrigger = new CompletableFuture<>();
        asyncExecutor.execute(() -> asyncMainLoop(operation, firstArgument, iterations, finalTrigger, 
                                                  onError, asyncExecutor));
        return finalTrigger;
    }

    private static <PayloadT> CompletionStage<PayloadT>
    asyncMainLoop(Function<PayloadT, CompletionStage<PayloadT>> operation,
                  @Nullable PayloadT firstArgument, long iterations, CompletionStage<PayloadT> finalTrigger,
                  Function<Throwable, CompletionStage<PayloadT>> onError, Executor executorService) {
        return operation.apply(firstArgument)
                        .exceptionallyComposeAsync(onError, executorService)
                        .thenComposeAsync(result -> {
                            long leftIterations = iterations - 1;
                            if (leftIterations > 0) {
                                return asyncMainLoop(operation, result, leftIterations,
                                                     finalTrigger, onError, executorService);
                            }
                            finalTrigger.toCompletableFuture().complete(result);
                            return finalTrigger;
                        }, executorService);
    }
}
