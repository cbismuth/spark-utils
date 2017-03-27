/*
 * The MIT License (MIT)
 * Copyright (c) 2016-2017 Christophe Bismuth
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.github.cbismuth.spark.utils.cluster.mapper.partition;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public final class PartitionToPairMapper<T, K, R> implements PairFlatMapFunction<Iterator<T>, K, R> {

    private static final long serialVersionUID = -3475108324090125L;

    private final int ordered;
    private final boolean parallel;

    public static <T, K, R> PartitionToPairMapper<T, K, R> newPartitionToPairMapper(final SerializableFunction<T, Tuple2<K, R>> function) {
        return newPartitionToPairMapper(function, Spliterator.ORDERED, false);
    }

    public static <T, K, R> PartitionToPairMapper<T, K, R> newPartitionToPairMapper(final SerializableFunction<T, Tuple2<K, R>> function,
                                                                                    final int ordered, final boolean parallel) {
        return new PartitionToPairMapper<>(function, ordered, parallel);
    }

    private final SerializableFunction<T, Tuple2<K, R>> function;

    private PartitionToPairMapper(final SerializableFunction<T, Tuple2<K, R>> function,
                                  final int ordered, final boolean parallel) {
        this.ordered = ordered;
        this.parallel = parallel;
        this.function = function;
    }

    @Override
    public Iterator<Tuple2<K, R>> call(final Iterator<T> i) throws Exception {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(i, ordered), parallel)
                            .map(function)
                            .iterator();
    }
}
