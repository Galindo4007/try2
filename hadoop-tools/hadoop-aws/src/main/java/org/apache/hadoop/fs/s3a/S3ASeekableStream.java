/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSInputStream;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class S3ASeekableStream extends FSInputStream {

    private S3SeekableInputStream inputStream;
    private long lastReadCurrentPos = 0;
    private final String key;

    public static final Logger LOG = LoggerFactory.getLogger(S3ASeekableStream.class);

    public S3ASeekableStream(String bucket, String key, S3SeekableInputStreamFactory s3SeekableInputStreamFactory) {
        this.inputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(bucket, key));
        this.key = key;
    }

    @Override
    public int read() throws IOException {
        throwIfClosed();
        return inputStream.read();
    }

    @Override
    public void seek(long pos) throws IOException {
        throwIfClosed();
        inputStream.seek(pos);
    }


    @Override
    public synchronized long getPos() {
        if (!isClosed()) {
            lastReadCurrentPos = inputStream.getPos();
        }
        return lastReadCurrentPos;
    }


    /**
     * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
     * reached. Leaves the position of the stream unaltered.
     *
     * @param buf buffer to read data into
     * @param off start position in buffer at which data is written
     * @param n the number of bytes to read; the n-th byte should be the last byte of the stream.
     * @return the total number of bytes read into the buffer
     */
    public void readTail(byte[] buf, int off, int n) throws IOException {
        throwIfClosed();
        inputStream.readTail(buf, off, n);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        throwIfClosed();
        return inputStream.read(buf, off, len);
    }


    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
            super.close();
        }
    }

    protected void throwIfClosed() throws IOException {
        if (isClosed()) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    protected boolean isClosed() {
        return inputStream == null;
    }
}