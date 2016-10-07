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
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.FIRST_NAME;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.ID;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.LAST_NAME;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

public class MiniDFSClusterTest {

    private static final Logger LOGGER = getLogger(MiniDFSClusterTest.class);

    // infrastructure
    private final HadoopFactory hadoopFactory = new HadoopFactory();
    private final Configuration config = hadoopFactory.config();
    private final MiniDFSCluster cluster = hadoopFactory.cluster(config);
    private final FileSystem fileSystem = hadoopFactory.fileSystem(cluster);

    // I/O
    private final AvroWriter writer = new AvroWriter();
    private final SparkReader sparkReader = new SparkReader();

    // Model-related instances
    private final RecordMapper<Person> mapper = new PersonRecordMapper();
    private final Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/schema/person.avsc"));
    private final String outputPath = String.format("%s/%s/part-m-00001/avro", config.get("fs.defaultFS"), Person.class.getSimpleName());

    public MiniDFSClusterTest() throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    LOGGER.info("Closing Hadoop file system ...");
                    fileSystem.close();
                    LOGGER.info("Hadoop file system closed");

                    LOGGER.info("Shutting down MiniDFSCluster ...");
                    cluster.shutdown();
                    LOGGER.info("MiniDFSCluster shut down");
                } catch (final IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        });
    }

    @Test
    public void testReadJavaRDDFromMiniDFSCluster() throws IOException {
        // GIVEN
        final List<Person> expected = newArrayList(
            new Person(1L, "James", "Gosling"),
            new Person(2L, "Joshua", "Bloch"),
            new Person(3L, "Doug", "Lea")
        );

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

}
