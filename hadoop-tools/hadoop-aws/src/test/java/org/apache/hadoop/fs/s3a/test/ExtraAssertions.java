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

package org.apache.hadoop.fs.s3a.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.s3a.S3AUtils.applyLocatedFiles;
import static org.junit.Assert.assertTrue;

/**
 * Some extra assertions for tests.
 */
@InterfaceAudience.Private
public class ExtraAssertions {

  /**
   * Assert that the number of files in a destination matches that expected.
   * @param text text to use in the message
   * @param fs filesystem
   * @param path path to list (recursively)
   * @param expected expected count
   * @throws IOException IO problem
   */
  public static void assertFileCount(String text, FileSystem fs,
      Path path, long expected)
      throws IOException {
    List<String> files = new ArrayList<>();
    applyLocatedFiles(fs.listFiles(path, true),
        (status) -> files.add(status.getPath().toString()));
    long actual = files.size();
    if (actual != expected) {
      String ls = files.stream().collect(Collectors.joining("\n"));
      Assert.fail(text + ": expected " + expected + " files in " + path
          + " but got " + actual + "\n" + ls);
    }
  }

  /**
   * Assert that a string contains a piece of text.
   * @param text text to can.
   * @param contained text to look for.
   */
  public static void assertTextContains(String text, String contained) {
    assertTrue("string \"" + contained + "\" not found in \"" + text + "\"",
        text != null && text.contains(contained));
  }

  /**
   * If the condition is met, throw an AssertionError with the message
   * and any nested exception.
   * @param condition condition
   * @param message text to use in the exception
   * @param cause a (possibly null) throwable to init the cause with
   * @throws AssertionError with the text and throwable if condition == true.
   */
  public static void failIf(boolean condition,
      String message,
      Throwable cause) {
    if (condition) {
      ContractTestUtils.fail(message, cause);
    }
  }

  /**
   * If the condition is met, throw an AssertionError with the message
   * and any nested exception.
   * @param condition condition
   * @param message text to use in the exception
   * @param cause a (possibly null) throwable to init the cause with
   * @throws AssertionError with the text and throwable if condition == true.
   */
  public static void failUnless(boolean condition,
      String message,
      Throwable cause) {
    failIf(!condition, message, cause);
  }

  /**
   * Extract the inner cause of an exception.
   * @param expected  expected class of the cuse
   * @param thrown thrown exception.
   * @param <T> type of the cause
   * @return the extracted exception.
   * @throws AssertionError with the text and throwable if the cause is not as
   * expected
   */
  public static <T extends Throwable> T extractCause(Class<T> expected,
      Throwable thrown) {
    Throwable cause = thrown.getCause();
    failIf(cause == null,
        "No inner cause",
        thrown);
    failUnless(cause.getClass().equals(expected),
        "Inner cause is of wrong type : expected " + expected,
        thrown);
    return (T)cause;
  }
}
