/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.flow;

import io.reacted.patterns.NonNullByDefault;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
@NonNullByDefault
class StreamProxy<T> implements Stream<T> {
  private final Stream<T> sourceStream;
  StreamProxy(Stream<T> sourceStream) {
    this.sourceStream = Objects.requireNonNull(sourceStream, "Source stream cannot be null");
  }

  @Override
  public Stream<T> takeWhile(Predicate<? super T> predicate) {
    return sourceStream.takeWhile(predicate);
  }

  @Override
  public Stream<T> dropWhile(Predicate<? super T> predicate) {
    return sourceStream.dropWhile(predicate);
  }

  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    return sourceStream.filter(predicate);
  }

  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    return sourceStream.map(mapper);
  }

  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    return sourceStream.mapToInt(mapper);
  }

  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    return sourceStream.mapToLong(mapper);
  }

  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return sourceStream.mapToDouble(mapper);
  }

  @Override
  public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
    return sourceStream.flatMap(mapper);
  }

  @Override
  public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
    return sourceStream.flatMapToInt(mapper);
  }

  @Override
  public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
    return sourceStream.flatMapToLong(mapper);
  }

  @Override
  public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
    return sourceStream.flatMapToDouble(mapper);
  }

  @Override
  public Stream<T> distinct() { return sourceStream.distinct(); }

  @Override
  public Stream<T> sorted() { return sourceStream.sorted(); }

  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    return sourceStream.sorted(comparator);
  }

  @Override
  public Stream<T> peek(Consumer<? super T> action) { return sourceStream.peek(action); }

  @Override
  public Stream<T> limit(long maxSize) { return sourceStream.limit(maxSize); }

  @Override
  public Stream<T> skip(long n) { return sourceStream.skip(n); }

  @Override
  public void forEach(Consumer<? super T> action) { sourceStream.forEach(action); }

  @Override
  public void forEachOrdered(Consumer<? super T> action) { sourceStream.forEachOrdered(action); }

  @Override
  public Object[] toArray() { return sourceStream.toArray(); }

  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) { return sourceStream.toArray(generator); }

  @Override
  public T reduce(T identity, BinaryOperator<T> accumulator) {
    return sourceStream.reduce(identity, accumulator);
  }

  @Override
  public Optional<T> reduce(BinaryOperator<T> accumulator) {
    return sourceStream.reduce(accumulator);
  }

  @Override
  public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator,
                      BinaryOperator<U> combiner) {
    return sourceStream.reduce(identity, accumulator, combiner);
  }

  @Override
  public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator,
                       BiConsumer<R, R> combiner) {
    return sourceStream.collect(supplier, accumulator, combiner);
  }

  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return sourceStream.collect(collector);
  }

  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return sourceStream.min(comparator);
  }

  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return sourceStream.max(comparator);
  }

  @Override
  public long count() { return sourceStream.count(); }

  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return sourceStream.anyMatch(predicate);
  }

  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return sourceStream.allMatch(predicate);
  }

  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    return sourceStream.noneMatch(predicate);
  }

  @Override
  public Optional<T> findFirst() { return sourceStream.findFirst(); }

  @Override
  public Optional<T> findAny() { return sourceStream.findAny(); }

  @Override
  public Iterator<T> iterator() { return sourceStream.iterator(); }

  @Override
  public Spliterator<T> spliterator() { return sourceStream.spliterator(); }

  @Override
  public boolean isParallel() { return sourceStream.isParallel(); }

  @Override
  public Stream<T> sequential() { return sourceStream.sequential(); }

  @Override
  public Stream<T> parallel() { return sourceStream.parallel(); }

  @Override
  public Stream<T> unordered() { return sourceStream.unordered(); }

  @Override
  public Stream<T> onClose(Runnable closeHandler) { return sourceStream.onClose(closeHandler); }

  @Override
  public void close() { sourceStream.close(); }
}
