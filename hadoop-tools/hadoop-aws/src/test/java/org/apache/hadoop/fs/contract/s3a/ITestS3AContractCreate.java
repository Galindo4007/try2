/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.Constants.CONNECTION_EXPECT_CONTINUE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * S3A contract tests creating files.
 */
@RunWith(Parameterized.class)
public class ITestS3AContractCreate extends AbstractContractCreateTest {

  /**
   * This test suite is parameterized for the different create file
   * options.
   * @return a list of test parameters.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {false},
        {true}
    });
  }

  /**
   * Expect a 100-continue response?
   */
  private final boolean expectContinue;

  public ITestS3AContractCreate(final boolean expectContinue) {
    this.expectContinue = expectContinue;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf =
        super.createConfiguration();

    removeBaseAndBucketOverrides(
        conf,
        CONNECTION_EXPECT_CONTINUE);
    conf.setBoolean(CONNECTION_EXPECT_CONTINUE, expectContinue);
    S3ATestUtils.disableFilesystemCaching(conf);
    return conf;
  }

}
