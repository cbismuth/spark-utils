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

package com.github.cbismuth.spark.utils.cluster.mapper;

import com.github.cbismuth.spark.utils.cluster.model.Person;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.FIRST_NAME;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.ID;
import static com.github.cbismuth.spark.utils.cluster.model.Person.Sqoop.LAST_NAME;

public class PersonRecordMapper implements RecordMapper<Person> {

    @Override
    public GenericRecord mapRecord(final Schema schema, final Person person) {
        final GenericRecord record = new GenericData.Record(schema);

        record.put(ID.name(), person.getId());
        record.put(FIRST_NAME.name(), person.getFirstName());
        record.put(LAST_NAME.name(), person.getLastName());

        return record;
    }

}
