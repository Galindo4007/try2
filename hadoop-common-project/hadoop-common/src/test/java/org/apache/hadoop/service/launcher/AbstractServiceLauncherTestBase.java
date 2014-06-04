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

package org.apache.hadoop.service.launcher;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

public class AbstractServiceLauncherTestBase extends Assert {
  @Rule
  public Timeout testTimeout = new Timeout(10000);

  @Rule
  public TestName methodName = new TestName();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  @BeforeClass
  public static void disableJVMExits() {
    ExitUtil.disableSystemExit();
    ExitUtil.disableSystemHalt();
  }

  protected void assertInState(Service service, Service.STATE expected) {
    Service.STATE actual = service.getServiceState();
    if (actual != expected) {
      fail("Service " + service.getName() + " in state " + actual
           + " -expected " + expected);
    }
  }

  protected void assertStopped(Service service) {
    assertInState(service, Service.STATE.STOPPED);
  }

}
