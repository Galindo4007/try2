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

package org.apache.hadoop.fs.azurebfs.utils;

import java.util.UUID;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingContext {
  private final String clientCorrelationID;
  private final String fileSystemID;
  private String clientRequestID = "";
  private String primaryRequestID;
  private String streamID;
  private int retryCount;
  private final String hadoopOpName;
  private final TracingContextFormat format;

  private static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);
  public static final int MAX_CLIENT_CORRELATION_ID_LENGTH = 72;
  public static final String CLIENT_CORRELATION_ID_PATTERN = "[a-zA-Z0-9-]*";

  public static enum TracingContextFormat {
    SINGLE_ID_FORMAT,
    ALL_ID_FORMAT,
    TWO_ID_FORMAT;
  }

  public TracingContext(String clientCorrelationID, String fileSystemID,
      String hadoopOpName, TracingContextFormat tracingContextFormat) {
    this.fileSystemID = fileSystemID;
    this.hadoopOpName = hadoopOpName;
    this.clientCorrelationID = validateClientCorrelationID(clientCorrelationID);
    streamID = EMPTY_STRING;
    retryCount = 0;
    primaryRequestID = "";
    format = tracingContextFormat;
  }

  public TracingContext(String clientCorrelationID, String fileSystemID,
      String hadoopOpName, boolean needsPrimaryReqId,
      TracingContextFormat tracingContextFormat) {
    this(clientCorrelationID, fileSystemID, hadoopOpName, tracingContextFormat);
    primaryRequestID = needsPrimaryReqId? UUID.randomUUID().toString() : "";
  }

  public TracingContext(TracingContext originalTracingContext) {
    this.fileSystemID = originalTracingContext.fileSystemID;
    this.streamID = originalTracingContext.streamID;
    this.clientCorrelationID = originalTracingContext.clientCorrelationID;
    this.hadoopOpName = originalTracingContext.hadoopOpName;
    this.retryCount = 0;
    this.primaryRequestID = originalTracingContext.primaryRequestID;
    this.format = originalTracingContext.format;
  }

  public String validateClientCorrelationID(String clientCorrelationID) {
    if ((clientCorrelationID.length() > MAX_CLIENT_CORRELATION_ID_LENGTH)
        || (!clientCorrelationID.matches(CLIENT_CORRELATION_ID_PATTERN))) {
      LOG.debug(
          "Invalid config provided; correlation id not included in header.");
      return EMPTY_STRING;
    }
    return clientCorrelationID;
  }

  public void generateClientRequestID() {
    clientRequestID = UUID.randomUUID().toString();
  }

  public void setPrimaryRequestID() {
    primaryRequestID = UUID.randomUUID().toString();
  }

  public void setStreamID(String stream) {
    streamID = stream;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public String toString() {
    String header = clientRequestID; //case 0, no IDs for correlation
    switch (format) {
    case ALL_ID_FORMAT: header =
        clientCorrelationID + ":" + header + ":" + fileSystemID + ":" + primaryRequestID
          + ":" + streamID + ":" + hadoopOpName + ":" + retryCount;
        break;
    case TWO_ID_FORMAT: header = clientCorrelationID + ":" + header;
    }
    return header;
  }
}
