/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageStatistics;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.util.Progressable;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;
import static org.apache.hadoop.fs.s3a.Constants.MIN_MULTIPART_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_RECV_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_SEND_BUFFER;

/**
 * Scale test which creates a huge file.
 *
 * <b>Important:</b> the order in which these tests execute is fixed to
 * alphabetical order. Test cases are numbered {@code test_123_} to impose
 * an ordering based on the numbers.
 *
 * Having this ordering allows the tests to assume that the huge file
 * exists. Even so: they should all have an assumes() check at the start,
 * in case an individual test is executed.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class STestS3AHugeFileCreate extends S3AScaleTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(
      STestS3AHugeFileCreate.class);
  private Path scaleTestDir;
  private Path hugefile;
  private Path hugefileRenamed;

  public static final int BLOCKSIZE = 64 * 1024;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    final Path testPath = getTestPath();
    scaleTestDir = new Path(testPath, "scale");
    hugefile = new Path(scaleTestDir, "hugefile");
    hugefileRenamed = new Path(scaleTestDir, "hugefileRenamed");
  }

  @Override
  public void tearDown() throws Exception {
    // do nothing. Specifically: do not delete the test dir
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    configuration.setBoolean(Constants.FAST_UPLOAD, true);
    configuration.setLong(MIN_MULTIPART_THRESHOLD, 5 * _1MB);
    configuration.setLong(SOCKET_SEND_BUFFER, BLOCKSIZE);
    configuration.setLong(SOCKET_RECV_BUFFER, BLOCKSIZE);
    return configuration;
  }

  @Test
  public void test_010_CreateHugeFile() throws IOException {
    long mb = getTestProperty(KEY_HUGE_FILESIZE, DEFAULT_HUGE_FILESIZE);
    long filesize = _1MB * mb;

    describe("Creating file %s of size %d MB", hugefile, mb);
    try {
      S3AFileStatus status = fs.getFileStatus(hugefile);
      long actualSize = status.getLen();
      if (actualSize == filesize) {
        String message = "File of size " + mb + " MB exists: " + status;
        LOG.info(message);
        throw new AssumptionViolatedException(message);
      }
    } catch (FileNotFoundException e) {
      // the file doesn't exist and so must be created.
    }
    byte[] data = new byte[BLOCKSIZE];
    for (int i = 0; i < BLOCKSIZE; i++) {
      data[i] = (byte)(i % 256);
    }

    assertEquals (
        "File size set in " + KEY_HUGE_FILESIZE+ " = " + filesize
        +" is not a multiple of " + BLOCKSIZE,
        0, filesize % BLOCKSIZE);
    long blocks = filesize / BLOCKSIZE;
    long blocksPerMB = _1MB / BLOCKSIZE;

    // perform the upload.
    // there's lots of logging here, so that a tail -f on the output log
    // can give a view of what is happening.
    StorageStatistics storageStatistics = fs.getStorageStatistics();
    String putRequests = Statistic.OBJECT_PUT_REQUESTS.getSymbol();
    String putBytes = Statistic.OBJECT_PUT_BYTES.getSymbol();

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();

    try(FSDataOutputStream out = fs.create(hugefile,
        true,
        BLOCKSIZE,
        new ProgressCallback())) {

      for (long block = 1; block <= blocks; block++) {
        out.write(data);
        if (block % blocksPerMB == 0) {
          long written = block * BLOCKSIZE;
          long percentage = written * 100 / filesize;
          LOG.info(String.format("[%03d%%] Written %.2f MB out of %.2f MB;" +
                  " PUT = %d bytes in %d operations",
              percentage,
              1.0 * written / _1MB,
              1.0 * filesize / _1MB,
              storageStatistics.getLong(putBytes),
              storageStatistics.getLong(
                  putRequests)
          ));
        }
      }
      // now close the file
      LOG.info("Closing file and completing write operation");
      ContractTestUtils.NanoTimer closeTimer
          = new ContractTestUtils.NanoTimer();
      out.close();
      closeTimer.end("Time to close() output stream");
    }

    timer.end("Time to write %d MB in blocks of %d", mb,
        BLOCKSIZE);
    logFSState();
    if (mb > 0) {
      LOG.info("Time per MB to write = {} nS", toHuman(timer.duration() / mb));
    }
    LOG.info("Effective Bandwidth: {} MB/s", timer.bandwidth(filesize));
    Long putRequestCount = storageStatistics.getLong(putRequests);
    Long putByteCount = storageStatistics.getLong(putBytes);
    LOG.info("PUT {} bytes in {} operations; {} MB/operation",
        putByteCount, putRequestCount,
        putByteCount  / (putRequestCount * _1MB));
    LOG.info("Time per PUT {} nS",
        toHuman(timer.nanosPerOperation(putRequestCount)));
    S3AFileStatus status = fs.getFileStatus(hugefile);
    assertEquals("File size in " + status, filesize, status.getLen());
  }

  /**
   * Progress callback from AWS. Likely to come in on a different thread.
   */
  private static class ProgressCallback implements Progressable,
      ProgressListener {
    private int counter = 0;

    @Override
    public void progress() {
      counter++;
    }

    public int getCounter() {
      return counter;
    }

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
      counter++;
      if (progressEvent.getEventType().isByteCountEvent()) {
        LOG.debug("Event {}", progressEvent);
      } else {
        LOG.info("Event {}", progressEvent);
      }
    }
  }

  void assumeHugeFileExists() throws IOException {
    Assume.assumeTrue("No file " + hugefile, fs.exists(hugefile));
  }

  @Test
  public void test_050_readHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("Reading %s", hugefile);
    S3AFileStatus status = fs.getFileStatus(hugefile);
    long filesize = status.getLen();
    long blocks = filesize / BLOCKSIZE;
    byte[] data = new byte[BLOCKSIZE];

    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try (FSDataInputStream in = fs.open(hugefile, BLOCKSIZE)) {
      for (long block = 0; block < blocks; block++) {
        in.readFully(data);
      }
    }

    long mb = Math.max(filesize / _1MB, 1);
    timer.end("Time to read file of %d MB ", mb);
    LOG.info("Time per MB to read = {} nS", toHuman(timer.duration() / mb));
    LOG.info("Effective Bandwidth: {} MB/s", timer.bandwidth(filesize));
    logFSState();
  }

  private void logFSState() {
    LOG.info("File System state after operation:\n{}", fs);
  }

  @Test
  public void test_100_renameHugeFile() throws Throwable {
    assumeHugeFileExists();
    describe("renaming %s to %s", hugefile, hugefileRenamed);
    S3AFileStatus status = fs.getFileStatus(hugefile);
    long filesize = status.getLen();
    fs.delete(hugefileRenamed, false);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.rename(hugefile, hugefileRenamed);
    long mb = Math.max(filesize / _1MB, 1);
    timer.end("Time to rename file of %d MB", mb);
    LOG.info("Time per MB to rename = {} nS", toHuman(timer.duration() / mb));
    LOG.info("Effective Bandwidth: {} MB/s" , timer.bandwidth(filesize));
    logFSState();
    S3AFileStatus destFileStatus = fs.getFileStatus(hugefileRenamed);
    assertEquals(filesize, destFileStatus.getLen());

    // rename back
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();
    fs.rename(hugefileRenamed, hugefile);
    timer2.end("Renaming back");
  }

  @Test
  public void test_999_DeleteHugeFiles() throws IOException {
    describe("Deleting %s", hugefile);
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    fs.delete(hugefile, false);
    timer.end("Time to delete %s", hugefile);
    ContractTestUtils.NanoTimer timer2 = new ContractTestUtils.NanoTimer();

    fs.delete(hugefileRenamed, false);
    timer2.end("Time to delete %s", hugefileRenamed);
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

}
