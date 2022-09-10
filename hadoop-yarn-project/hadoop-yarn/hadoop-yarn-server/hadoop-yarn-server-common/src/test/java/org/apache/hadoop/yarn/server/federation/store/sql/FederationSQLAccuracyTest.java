/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class FederationSQLAccuracyTest {

  protected final String HSQLDB_DRIVER = "org.hsqldb.jdbc.JDBCDataSource";
  protected final String DATABASE_URL = "jdbc:hsqldb:mem:state";
  protected final String DATABASE_USERNAME = "SA";
  protected final String DATABASE_PASSWORD = "";

  private FederationStateStore stateStore;

  protected abstract FederationStateStore createStateStore();

  private Configuration conf;

  @Before
  public void before() throws IOException, YarnException {
     stateStore = createStateStore();
     stateStore.init(conf);
  }

  @After
  public void after() throws Exception {
     stateStore.close();
  }

  protected void setConf(Configuration conf) {
     this.conf = conf;
  }

  protected Configuration getConf() {
     return conf;
  }

  protected FederationStateStore getStateStore() {
    return stateStore;
  }
}
