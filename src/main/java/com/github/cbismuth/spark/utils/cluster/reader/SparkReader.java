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

package com.github.cbismuth.spark.utils.cluster.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkReader {

    private final Class<AvroKeyInputFormat<GenericRecord>> fClass = classOf(new AvroKeyInputFormat<GenericRecord>());
    private final Class<AvroKey<GenericRecord>> kClass = classOf(new AvroKey<GenericRecord>());

    private <T> Class<T> classOf(final T instance) {
        return (Class<T>) instance.getClass();
    }

    public JavaRDD<GenericRecord> read(final Configuration config,
                                       final JavaSparkContext sparkContext,
                                       final Schema schema,
                                       final String path) {
        final JavaPairRDD<AvroKey<GenericRecord>, NullWritable> rdd = sparkContext.newAPIHadoopFile(
            path,
            fClass, kClass, NullWritable.class,
            setSchemaInputKey(config, schema)
        );

        return mapToGenericRecord(rdd);
    }

    private JavaRDD<GenericRecord> mapToGenericRecord(final JavaPairRDD<AvroKey<GenericRecord>, NullWritable> rdd) {
        return rdd.map(tuple -> tuple._1().datum())
                  .map(record -> new GenericData.Record((GenericData.Record) record, false));
    }

    private Configuration setSchemaInputKey(final Configuration config,
                                            final Schema schema) {
        config.set("avro.schema.input.key", schema.toString());

        return config;
    }

}
