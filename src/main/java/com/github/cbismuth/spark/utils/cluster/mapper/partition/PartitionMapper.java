package com.github.cbismuth.spark.utils.cluster.mapper.partition;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public final class PartitionMapper<T, R> implements FlatMapFunction<Iterator<T>, R> {

    private static final long serialVersionUID = -3475108324090125L;

    private final int ordered;
    private final boolean parallel;

    public static <T, R> PartitionMapper<T, R> newPartitionMapper(final SerializableFunction<T, R> function) {
        return newPartitionMapper(function, Spliterator.ORDERED, false);
    }

    public static <T, R> PartitionMapper<T, R> newPartitionMapper(final SerializableFunction<T, R> function,
                                                                  final int ordered, final boolean parallel) {
        return new PartitionMapper<>(function, ordered, parallel);
    }

    private final SerializableFunction<T, R> function;

    private PartitionMapper(final SerializableFunction<T, R> function,
                            final int ordered, final boolean parallel) {
        this.ordered = ordered;
        this.parallel = parallel;
        this.function = function;
    }

    @Override
    public Iterator<R> call(final Iterator<T> i) throws Exception {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(i, ordered), parallel)
                            .map(function)
                            .iterator();
    }
}
