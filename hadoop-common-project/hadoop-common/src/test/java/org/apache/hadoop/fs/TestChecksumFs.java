/**
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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.EnumSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.rules.Timeout;

import static org.apache.hadoop.fs.CreateFlag.*;
import static org.junit.Assert.*;

/**
 * This class tests the functionality of ChecksumFs.
 */
public class TestChecksumFs {
  /**
   * System property name to set the test timeout: {@value}.
   */
  public static final String PROPERTY_TEST_DEFAULT_TIMEOUT =
      "test.default.timeout";

  /**
   * The default timeout (in milliseconds) if the system property
   * {@link #PROPERTY_TEST_DEFAULT_TIMEOUT}
   * is not set: {@value}.
   */
  public static final int TEST_DEFAULT_TIMEOUT_VALUE = 100000;

  /**
   * The JUnit rule that sets the default timeout for tests.
   */
  @Rule
  public Timeout defaultTimeout = retrieveTestTimeout();

  /**
   * Retrieve the test timeout from the system property
   * {@link #PROPERTY_TEST_DEFAULT_TIMEOUT}, falling back to
   * the value in {@link #TEST_DEFAULT_TIMEOUT_VALUE} if the
   * property is not defined.
   * @return the recommended timeout for tests
   */
  private static Timeout retrieveTestTimeout() {
    String propval = System.getProperty(PROPERTY_TEST_DEFAULT_TIMEOUT,
        Integer.toString(
            TEST_DEFAULT_TIMEOUT_VALUE));
    int millis;
    try {
      millis = Integer.parseInt(propval);
    } catch (NumberFormatException e) {
      //fall back to the default value, as the property cannot be parsed
      millis = TEST_DEFAULT_TIMEOUT_VALUE;
    }
    return new Timeout(millis);
  }

  private Configuration conf;
  private Path testRootDirPath;
  private FileContext fc;

  @Before
  public void setUp() throws Exception {
    conf = getTestConfiguration();
    fc = FileContext.getFileContext(conf);
    testRootDirPath = new Path(GenericTestUtils.getRandomizedTestDir()
        .getAbsolutePath());
    mkdirs(testRootDirPath);
  }

  @After
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(testRootDirPath, true);
    }
  }

  @Test
  public void testRenameFileToFile() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDst");
    verifyRename(srcPath, dstPath, false);
  }

  @Test
  public void testRenameFileToFileWithOverwrite() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDst");
    verifyRename(srcPath, dstPath, true);
  }

  @Test
  public void testRenameFileIntoDirFile() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDir/testRenameDst");
    mkdirs(dstPath);
    verifyRename(srcPath, dstPath, false);
  }

  @Test
  public void testRenameFileIntoDirFileWithOverwrite() throws Exception {
    Path srcPath = new Path(testRootDirPath, "testRenameSrc");
    Path dstPath = new Path(testRootDirPath, "testRenameDir/testRenameDst");
    mkdirs(dstPath);
    verifyRename(srcPath, dstPath, true);
  }

  private void verifyRename(Path srcPath, Path dstPath,
      boolean overwrite) throws Exception {
    ChecksumFs fs = (ChecksumFs) fc.getDefaultFileSystem();

    fs.delete(srcPath, true);
    fs.delete(dstPath, true);

    Options.Rename renameOpt = Options.Rename.NONE;
    if (overwrite) {
      renameOpt = Options.Rename.OVERWRITE;
      createTestFile(fs, dstPath, 2);
    }

    // ensure file + checksum are moved
    createTestFile(fs, srcPath, 1);
    assertTrue("Checksum file doesn't exist for source file - " + srcPath,
        fc.util().exists(fs.getChecksumFile(srcPath)));
    fs.rename(srcPath, dstPath, renameOpt);
    assertTrue("Checksum file doesn't exist for dest file - " + srcPath,
        fc.util().exists(fs.getChecksumFile(dstPath)));
    try (FSDataInputStream is = fs.open(dstPath)) {
      assertEquals(1, is.readInt());
    }
  }

  private static Configuration getTestConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");
    conf.setClass("fs.AbstractFileSystem.file.impl",
        org.apache.hadoop.fs.local.LocalFs.class,
        org.apache.hadoop.fs.AbstractFileSystem.class);
    return conf;
  }

  private void createTestFile(ChecksumFs fs, Path path, int content)
      throws IOException {
    try (FSDataOutputStream fout = fs.create(path,
        EnumSet.of(CREATE, OVERWRITE),
        Options.CreateOpts.perms(FsPermission.getDefault()))) {
      fout.writeInt(content);
    }
  }

  private void mkdirs(Path dirPath) throws IOException {
    fc.mkdir(dirPath, FileContext.DEFAULT_PERM, true);
  }
}
