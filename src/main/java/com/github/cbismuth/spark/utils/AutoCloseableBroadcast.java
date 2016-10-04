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

import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class AutoCloseableBroadcast<T> implements AutoCloseable {

    private static final Logger LOGGER = getLogger(AutoCloseableBroadcast.class);

    private final Broadcast<T> broadcast;
    private final boolean blocking;
    private final String name;

    public AutoCloseableBroadcast(final Broadcast<T> broadcast) {
        this(broadcast, true, null);
    }

    public AutoCloseableBroadcast(final Broadcast<T> broadcast, final boolean blocking) {
        this(broadcast, blocking, null);
    }

    public AutoCloseableBroadcast(final Broadcast<T> broadcast, final boolean blocking, final String name) {
        this.broadcast = broadcast;
        this.blocking = blocking;
        this.name = name;
    }

    @Override
    public void close() throws Exception {
        final String name = this.name != null ? this.name : "null";

        if (broadcast != null) {
            if (broadcast.isValid()) {
                broadcast.destroy(blocking);
                LOGGER.info("Broadcast with name [{}] destroyed with blocking flag set to [{}]", name, blocking);
            } else {
                LOGGER.warn("Can't destroy invalid broadcast with name [{}]", name);
            }
        } else {
            LOGGER.warn("Can't destroy null broadcast with name [{}]", name);
        }
    }

}
