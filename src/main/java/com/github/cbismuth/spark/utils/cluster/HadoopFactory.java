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

package com.github.cbismuth.spark.utils.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static java.lang.String.format;
import static org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR;

public final class HadoopFactory {

    public JavaSparkContext sparkContext() {
        return new JavaSparkContext(new SparkConf().setAppName(getClass().getSimpleName())
                                                   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                   .setMaster("local"));
    }

    public FileSystem fileSystem(final MiniDFSCluster cluster) throws IOException {
        return cluster.getFileSystem();
    }

    public MiniDFSCluster cluster(final Configuration config) throws IOException {
        final MiniDFSCluster cluster = new Builder(config).build();

        config.set("fs.defaultFS", format("hdfs://localhost:%d", cluster.getNameNodePort()));

        return cluster;
    }

    public Configuration config() throws IOException {
        final File baseDir = Files.createTempDirectory(getClass().getSimpleName())
                                  .toFile()
                                  .getAbsoluteFile();

        final Configuration config = new Configuration(false);
        config.set(HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        return config;
    }

}
