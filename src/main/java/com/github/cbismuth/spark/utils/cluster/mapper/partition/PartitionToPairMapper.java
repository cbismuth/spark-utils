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
