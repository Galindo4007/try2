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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.Closeable;
import java.io.IOException;

/**
 * This represents state which may be passed to bulk IO operations
 * to enable them to store information about the state of the ongoing
 * operation across invocations.
 *
 * A bulk operation state <i>MUST</i> only be be used for the single store
 * from which it was created, and <i>MUST</i>only for the duration of a single
 * bulk update operation.
 *
 * After the operation has completed, it <i>MUST</i> be closed so
 * as to guarantee that all state is released.
 */
public class BulkOperationState implements Closeable {

  @Override
  public void close() throws IOException {

  }
}
