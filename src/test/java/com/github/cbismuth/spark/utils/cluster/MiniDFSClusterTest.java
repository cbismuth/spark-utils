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

package com.github.cbismuth.spark.utils.cluster;

import com.github.cbismuth.spark.utils.cluster.mapper.PersonRecordMapper;
import com.github.cbismuth.spark.utils.cluster.mapper.RecordMapper;
import com.github.cbismuth.spark.utils.cluster.model.Person;
import com.github.cbismuth.spark.utils.cluster.reader.SparkReader;
import com.github.cbismuth.spark.utils.cluster.writer.AvroWriter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.FIRST_NAME;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.ID;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.LAST_NAME;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class MiniDFSClusterTest {

    // Hadoop configuration
    private final HadoopFactory hadoopFactory = new HadoopFactory();
    private final Configuration config = hadoopFactory.config();

    // Avro data model
    private final Schema schema = new Schema.Parser().parse(MiniDFSClusterTest.class.getResourceAsStream(Person.SCHEMA));

    // I/O
    private final AvroWriter writer = new AvroWriter();
    private final SparkReader sparkReader = new SparkReader();
    private final String outputPath = format("%s/%s/part-m-00001/avro",
                                             config.get("fs.defaultFS"),
                                             Person.class.getSimpleName());

    // Clustering
    private FileSystem fileSystem;
    private MiniDFSCluster cluster;

    public MiniDFSClusterTest() throws IOException {
        // NOP - here to please the Java compiler ...
    }

    @Before
    public void setUp() throws IOException {
        cluster = hadoopFactory.cluster(config);
        fileSystem = hadoopFactory.fileSystem(cluster);
    }

    @After
    public void tearDown() throws IOException {
        fileSystem.close();
        cluster.shutdown();
    }

    @Test
    public void testReadJavaRDDFromMiniDFSCluster() throws IOException {
        // GIVEN
        final List<Person> expected = newArrayList(
            new Person(1L, "James", "Gosling"),
            new Person(2L, "Joshua", "Bloch"),
            new Person(3L, "Doug", "Lea")
        );
        final RecordMapper<Person> mapper = new PersonRecordMapper();

        // WHEN
        writer.write(fileSystem, schema, expected, mapper, outputPath);

        // THEN
        try (final JavaSparkContext sparkContext = hadoopFactory.sparkContext()) {
            final List<Person> actual = sparkReader.read(config, sparkContext, schema, outputPath)
                                                   .map(record -> new Person(
                                                       (Long) record.get(ID.name()),
                                                       (String) record.get(FIRST_NAME.name()),
                                                       (String) record.get(LAST_NAME.name())
                                                   ))
                                                   .collect()
                                                   .stream()
                                                   .sorted()
                                                   .collect(toList());

            assertEquals(expected, actual);
        }
    }

    @Test(expected = Exception.class)
    public void testReadJavaRDDFromMiniDFSCluster_ensureException() throws IOException {
        // GIVEN
        final RecordMapper<Person> mapperWithException = mock(PersonRecordMapper.class);
        doThrow(Exception.class)
            .when(mapperWithException)
            .mapRecord(any(Schema.class),
                       any(Person.class));

        // WHEN
        writer.write(fileSystem, schema, singleton(mock(Person.class)), mapperWithException, outputPath);

        // THEN
        try (final JavaSparkContext sparkContext = hadoopFactory.sparkContext()) {
            sparkReader.read(config, sparkContext, schema, outputPath)
                       .map(record -> mock(Person.class))
                       .collect()
                       .stream()
                       .sorted()
                       .collect(toList());

            fail("Exception should have been raised!");
        }
    }

}
