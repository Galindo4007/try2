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

package org.apache.hadoop.fs.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.util.DurationInfo;

import static java.util.Objects.requireNonNull;

/**
 * Support for committing work through {@link ResilientCommitByRename}
 * where present.
 * This is for internal use only and will be removed when there is a
 * public rename operation which takes etags and FileStatus entries.
 */
@InterfaceAudience.LimitedPrivate({"Filesystems", "hadoop-mapreduce-client-core"})
@InterfaceStability.Unstable
public class ResilientCommitByRenameHelper {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResilientCommitByRenameHelper.class);

  /**
   * Target FS.
   */
  private final FileSystem fileSystem;

  /**
   * Was one more commits through the new API rejected?
   */
  private final AtomicBoolean commitRejected;

  /**
   * Instantiate.
   * @param fileSystem filesystem to work with.
   */
  public ResilientCommitByRenameHelper(final FileSystem fileSystem) {
    this.fileSystem = requireNonNull(fileSystem);
    commitRejected = new AtomicBoolean(false);
  }

  /**
   * Is resilient commit available on this filesystem/path?
   * @param sourcePath path to commit under.
   * @return true if the resilient commit API can b eused
   */
  public boolean resilientCommitAvailable(Path sourcePath) {
    if (commitRejected.get()) {
      return false;
    }
    final FileSystem fs = this.fileSystem;
    return filesystemHasResilientCommmit(fs, sourcePath);
  }

  /**
   * What is the resilence of this filesystem?
   * @param fs filesystem
   * @param sourcePath path to use
   * @return true if the conditions of use are met.
   */
  public static boolean filesystemHasResilientCommmit(
      final FileSystem fs,
      final Path sourcePath) {
    try {
      return fs instanceof ResilientCommitByRename
          && fs.hasPathCapability(sourcePath,
          ResilientCommitByRename.RESILIENT_COMMIT_BY_RENAME_PATH_CAPABILITY);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Commit a file.
   * If the store supports {@link ResilientCommitByRename} then
   * its API is used to commit the file, passing in the etag.
   * @param sourceStatus source status file
   * @param dest destination path
   * @param options rename flags
   * @return the outcome
   * @throws IOException any failure in resilient commit other than rejection
   * of the operation, and/or classic rename failed.
   */
  public ResilientCommitByRename.CommitByRenameOutcome commitFile(
      final FileStatus sourceStatus, final Path dest,
      final ResilientCommitByRename.CommitFlqgs... options)
      throws IOException {
    final Path sourcePath = sourceStatus.getPath();
    boolean rejected = false;
    if (resilientCommitAvailable(sourcePath)) {

      // use the better file rename operation.
      try (DurationInfo du = new DurationInfo(LOG, "commit(%s, %s) with status %s",
          sourcePath, dest, sourceStatus)) {
        return ((ResilientCommitByRename) fileSystem).commitSingleFileByRename(
            sourcePath,
            dest,
            null,
            sourceStatus,
            options);
      } catch (ResilientCommitByRename.ResilientCommitByRenameUnsupported
          | UnsupportedOperationException e) {
        commitWasRejected(sourcePath, e);
        rejected = true;
      }
    }
    // fall back to rename.
    try (DurationInfo du = new DurationInfo(LOG, "rename(%s, %s)",
        sourcePath, dest, sourceStatus)) {
      Set<ResilientCommitByRename.CommitFlqgs> flags = new HashSet<>(Arrays.asList(options));
      if (!flags.contains(ResilientCommitByRename.CommitFlqgs.DESTINATION_DOES_NOT_EXIST)) {
        try {
          final FileStatus destStatus = fileSystem.getFileStatus(dest);
          if (!flags.contains(ResilientCommitByRename.CommitFlqgs.OVERWRITE)
              || destStatus.isDirectory()) {
            // don't support renaming over a dir or, if not overwriting, a file
            throw new FileAlreadyExistsException(dest.toUri().toString());
          }
        } catch (FileNotFoundException ignored) {

        }
      }

      if (!fileSystem.rename(sourcePath, dest)) {
        escalateRenameFailure(sourcePath, dest);
      }
    }
    return new ResilientCommitByRename.CommitByRenameOutcome(false, rejected,true);
  }

  /**
   * The commit was rejected.
   * Log once and remember, so don't bother trying again through
   * the rest of this commit.
   * @param path source.
   * @param e exception.
   */
  private void commitWasRejected(Path path, final Exception e) {
    if (!commitRejected.getAndSet(true)) {
      LOG.warn("Resilent Commit to {} rejected as unsupported", path);
      LOG.debug("full exception", e);
    }
  }

  /**
   * Escalate a rename failure to an exception.
   * This never returns
   * @param source source path
   * @param dest dest path
   * @throws IOException always
   */
  private void escalateRenameFailure(Path source, Path dest)
      throws IOException {
    // rename just returned false.
    // collect information for a meaningful error message
    // and include in an exception raised.

    // get the source status; this will implicitly raise
    // a FNFE.
    final FileStatus sourceStatus = fileSystem.getFileStatus(source);

    // and look to see if there is anything at the destination
    FileStatus destStatus;
    try {
      destStatus = fileSystem.getFileStatus(dest);
    } catch (IOException e) {
      destStatus = null;
    }

    LOG.error("Failure to rename {} to {} with" +
            " source status {} " +
            " and destination status {}",
        source, dest,
        sourceStatus, destStatus);

    throw new PathIOException(source.toString(),
        "Failed to rename to " + dest);
  }

}
