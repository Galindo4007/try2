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
package org.apache.hadoop.yarn.server.webapp;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.webapp.dao.AppInfo;

/**
 * Utility class that wraps application information
 * required by the {@link LogServlet} class.
 */
class BasicAppInfo {
  private final YarnApplicationState appState;
  private final String user;

  BasicAppInfo(YarnApplicationState appState, String user) {
    this.appState = appState;
    this.user = user;
  }

  static BasicAppInfo fromAppInfo(AppInfo report) {
    return new BasicAppInfo(report.getAppState(), report.getUser());
  }

  YarnApplicationState getAppState() {
    return this.appState;
  }

  String getUser() {
    return this.user;
  }
}
