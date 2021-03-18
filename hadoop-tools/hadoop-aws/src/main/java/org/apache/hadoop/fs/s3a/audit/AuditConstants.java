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

package org.apache.hadoop.fs.s3a.audit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants related to auditing.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class AuditConstants {

  private AuditConstants() {
  }

  /**
   * Name of class used for audit logs: {@value}.
   */
  public static final String AUDIT_SERVICE_CLASSNAME =
      "fs.s3a.audit.service.classname";

  /**
   * Classname of the logging auditor: {@value}.
   */
  public static final String LOGGING_AUDIT_SERVICE =
      "org.apache.hadoop.fs.s3a.audit.LoggingAuditor";

  /**
   * Classname of the No-op auditor: {@value}.
   */
  public static final String NOOP_AUDIT_SERVICE =
      "org.apache.hadoop.fs.s3a.audit.NoopAuditor";

  /**
   * List of extra AWS SDK request handlers: {@value}.
   * These are added to the SDK request chain <i>after</i>
   * any audit service.
   */
  public static final String AUDIT_REQUEST_HANDLERS =
      "fs.s3a.audit.request.handlers";

  /**
   * Should operations outside spans be rejected?
   * This is for testing coverage of the span code; if used
   * in production there's a risk of unexpected failures.
   * {@value}.
   */
  public static final String REJECT_OUT_OF_SPAN_OPERATIONS
      = "fs.s3a.audit.reject.out.of.span.operations";

  /**
   * The host from where requests originate.
   * example.org is used as the IETF require that it never resolves.
   * This isn't always met by some mobile/consumer DNS services, but
   * we don't worry about that. What is important is that
   * a scan for "example.org" in the logs will exclusively find
   * entries from this referrer.
   */
  public static final String REFERRER_ORIGIN_HOST = "h1.example.org";

  /**
   * Path to build.
   */
  public static final String PATH_FORMAT
      = "/audit/%1$s/s/%2$s/";

  /**
   * JobID query header: {@value}.
   */
  public static final String JOB_ID = "job";

  /**
   * Principal query header: {@value}.
   */
  public static final String PRINCIPAL = "pr";

  /**
   * Principal query header: {@value}.
   */
  public static final String FILESYSTEM_ID = "fs";

  public static final String PROCESS_ID = "ps";

  public static final String OP = "op";

  public static final String PATH = "path";

  public static final String PATH2 = "path2";

  public static final String PROCESS = "ps";

  /**
   * Thread header: {@value}.
   */
  public static final String THREAD = "tr";

  /**
   * Thread header: {@value}.
   */
  public static final String THREAD2 = "t2";

  /**
   * Span name used during initialization.
   */
  public static final String INITIALIZE_SPAN = "initialize";

  /**
   * Operation name for any operation outside of an explicit
   * span.
   */
  public static final String OUTSIDE_SPAN =
      "outside-span";

}
