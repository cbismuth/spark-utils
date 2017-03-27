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

package com.github.cbismuth.spark.utils.cluster.writer;

import com.github.cbismuth.spark.utils.cluster.mapper.RecordMapper;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import static org.slf4j.LoggerFactory.getLogger;

public class AvroWriter {

    private static final Logger LOGGER = getLogger(AvroWriter.class);

    public <T> void write(final FileSystem fileSystem,
                          final Schema schema,
                          final Collection<T> objects,
                          final RecordMapper<T> mapper,
                          final String path) throws IOException {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

        try (final OutputStream outputStream = fileSystem.create(new Path(path));
             final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
             final DataFileWriter<GenericRecord> ignored = dataFileWriter.create(schema, outputStream)) {

            objects.forEach(object -> {
                try {
                    final GenericRecord record = mapper.mapRecord(schema, object);

                    dataFileWriter.append(record);
                } catch (final Exception e) {
                    LOGGER.error(e.getMessage(), e);

                    throw Throwables.propagate(e);
                }
            });
        }
    }

}
