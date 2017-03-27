package com.github.cbismuth.spark.utils.cluster.mapper.partition;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    // NOP
}
