/*
 * The MIT License (MIT)
 * Copyright (c) 2016 Christophe Bismuth
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

package com.github.cbismuth.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AutoCloseableBroadcastTest {

    private static final Object[][] DATA = {
        { true },
        { false },
        };
    public static final Long EXPECTED_BROADCAST_VALUE = 0L;

    @Parameterized.Parameters(name = "blocking: {0}")
    public static Collection<Object[]> data() {
        return Arrays.stream(DATA).collect(toList());
    }

    private final boolean blocking;
    private final String name = getClass().getSimpleName();

    public AutoCloseableBroadcastTest(final boolean blocking) {
        this.blocking = blocking;
    }

    @Test
    public void testAutoCloseableBroadcast_withBaseConstructor() {
        final SparkConf sparkConf = new SparkConf().setAppName(randomUUID().toString())
                                                   .setMaster("local");

        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
             final AutoCloseableBroadcast<Long> ignored = new AutoCloseableBroadcast<>(sparkContext.broadcast(EXPECTED_BROADCAST_VALUE))) {

            assertEquals(EXPECTED_BROADCAST_VALUE, ignored.value().getValue());

        }
    }

    @Test
    public void testAutoCloseableBroadcast_withConstructorWithoutName() {
        final SparkConf sparkConf = new SparkConf().setAppName(randomUUID().toString())
                                                   .setMaster("local");

        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
             final AutoCloseableBroadcast<Long> ignored = new AutoCloseableBroadcast<>(sparkContext.broadcast(EXPECTED_BROADCAST_VALUE), blocking)) {

            assertEquals(EXPECTED_BROADCAST_VALUE, ignored.value().getValue());

        }
    }

    @Test
    public void testAutoCloseableBroadcast_valid() {
        final SparkConf sparkConf = new SparkConf().setAppName(randomUUID().toString())
                                                   .setMaster("local");

        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
             final AutoCloseableBroadcast<Long> ignored = new AutoCloseableBroadcast<>(sparkContext.broadcast(EXPECTED_BROADCAST_VALUE), blocking, name)) {

            assertEquals(EXPECTED_BROADCAST_VALUE, ignored.value().getValue());

        }
    }

    @Test
    public void testAutoCloseableBroadcast_invalid() {
        final SparkConf sparkConf = new SparkConf().setAppName(randomUUID().toString())
                                                   .setMaster("local");

        try (final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            final Broadcast<Integer> broadcast = sparkContext.broadcast(0);

            broadcast.destroy(true);

            try (final AutoCloseableBroadcast<Integer> ignored = new AutoCloseableBroadcast<>(broadcast, blocking, name)) {
                // no exception raised
            }
        }
    }

    @Test
    public void testAutoCloseableBroadcast_null() {
        final Broadcast<Integer> broadcast = null;

        try (final AutoCloseableBroadcast<Integer> ignored = new AutoCloseableBroadcast<>(broadcast, blocking, name)) {
            // no exception raised
        }
    }

}
