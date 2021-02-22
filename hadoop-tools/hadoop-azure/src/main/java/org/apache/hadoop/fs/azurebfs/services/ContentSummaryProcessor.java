/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.ABFSContentSummary;

public class ContentSummaryProcessor {
  private static final int CORE_POOL_SIZE = 1;
  private static final int MAX_THREAD_COUNT = 16;
  private static final int KEEP_ALIVE_TIME = 5;
  private static final int POLL_TIMEOUT = 100;
  private static final Logger LOG = LoggerFactory.getLogger(ContentSummaryProcessor.class);
  private final AtomicLong fileCount = new AtomicLong(0L);
  private final AtomicLong directoryCount = new AtomicLong(0L);
  private final AtomicLong totalBytes = new AtomicLong(0L);
  private final AtomicInteger numTasks = new AtomicInteger(0);
  private final ListingSupport abfsStore;
  private final ExecutorService executorService = new ThreadPoolExecutor(
      CORE_POOL_SIZE, MAX_THREAD_COUNT, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
      new SynchronousQueue<>());
  private final CompletionService<Void> completionService =
      new ExecutorCompletionService<>(executorService);
  private final LinkedBlockingQueue<FileStatus> queue = new LinkedBlockingQueue<>();

  /**
   * Processes a given path for count of subdirectories, files and total number
   * of bytes
   * @param abfsStore Instance of AzureBlobFileSystemStore, used to make
   * listStatus calls to server
   */
  public ContentSummaryProcessor(ListingSupport abfsStore) {
    this.abfsStore = abfsStore;
  }

  public ABFSContentSummary getContentSummary(Path path)
          throws IOException, ExecutionException, InterruptedException {
    try {
      processDirectoryTree(path);
      while (!queue.isEmpty() || numTasks.get() > 0) {
        try {
          completionService.take().get();
        } finally {
          numTasks.decrementAndGet();
          LOG.debug("FileStatus queue size = {}, number of submitted unfinished tasks = {}, active thread count = {}",
              queue.size(), numTasks, ((ThreadPoolExecutor) executorService).getActiveCount());
        }
      }
    } finally {
      executorService.shutdownNow();
      LOG.debug("Executor shutdown");
    }
    LOG.debug("Processed content summary of subtree under given path");
    return new ABFSContentSummary(totalBytes.get(), directoryCount.get(),
        fileCount.get(), totalBytes.get());
  }

  /**
   * Calls listStatus on given path and populated fileStatus queue with
   * subdirectories. Is called by new tasks to process the complete subtree
   * under a given path
   * @param path: Path to a file or directory
   * @throws IOException: listStatus error
   * @throws InterruptedException: error while inserting into queue
   */
  private void processDirectoryTree(Path path)
      throws IOException, InterruptedException {
    FileStatus[] fileStatuses = abfsStore.listStatus(path);

    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        queue.put(fileStatus);
        processDirectory();
        conditionalSubmitTaskToExecutor();
      } else {
        processFile(fileStatus);
      }
    }
  }

  private void processDirectory() {
    directoryCount.incrementAndGet();
  }

  /**
   * Increments file count and byte count
   * @param fileStatus: Provides file size to update byte count
   */
  private void processFile(FileStatus fileStatus) {
    fileCount.incrementAndGet();
    totalBytes.addAndGet(fileStatus.getLen());
  }

  /**
   * Submit task for processing a subdirectory based on current size of
   * filestatus queue and number of already submitted tasks
   */
  private synchronized void conditionalSubmitTaskToExecutor() {
    if (!queue.isEmpty() && numTasks.get() < MAX_THREAD_COUNT) {
      numTasks.incrementAndGet();
      completionService.submit(() -> {
        FileStatus fileStatus1;
        while ((fileStatus1 = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS))
                != null) {
          processDirectoryTree(fileStatus1.getPath());
        }
        return null;
      });
    }
  }

}
