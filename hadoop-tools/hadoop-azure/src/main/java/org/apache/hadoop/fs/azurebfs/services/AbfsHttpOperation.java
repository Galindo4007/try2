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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;

/**
 * Represents an HTTP operation.
 */
public abstract class AbfsHttpOperation implements AbfsPerfLoggable {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);

  protected static final int CONNECT_TIMEOUT = 30 * 1000;
  protected static final int READ_TIMEOUT = 30 * 1000;

  protected static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  protected static final int ONE_THOUSAND = 1000;
  protected static final int ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;

  protected final String method;
  protected final URL url;
  protected String maskedUrl;
  protected String maskedEncodedUrl;

  protected int statusCode;
  protected String statusDescription;
  protected String storageErrorCode = "";
  protected String storageErrorMessage  = "";
  protected String clientRequestId = "";
  protected String requestId  = "";
  protected String expectedAppendPos = "";

  // metrics
  protected int bytesSent;
  protected long bytesReceived;

  // optional trace enabled metrics
  protected final boolean isTraceEnabled;
  protected long connectionTimeMs;
  protected long sendRequestTimeMs;
  protected long recvResponseTimeMs;
  private boolean shouldMask = false;

  protected AbfsRestOperationType opType;
  protected List<AbfsHttpHeader> requestHeaders;
  protected AuthType authType;
  protected String authToken;

  protected byte[] responseContentBuffer = null;

  public AbfsHttpOperation(final AbfsRestOperationType opType,
      final URL url,
      final String method,
      final AuthType authType,
      final String authToken,
      List<AbfsHttpHeader> requestHeaders) throws IOException {

    this.opType = opType;
    this.isTraceEnabled = LOG.isTraceEnabled();
    this.url = url;
    this.method = method;
    this.clientRequestId = UUID.randomUUID().toString();
  }

  public AbfsHttpOperation(final URL url, final String method, List<AbfsHttpHeader> requestHeaders) throws IOException {
    this.isTraceEnabled = LOG.isTraceEnabled();
    this.url = url;
    this.method = method;
    this.clientRequestId = UUID.randomUUID().toString();
  }

  public String getMethod() {
    return method;
  }

  public String getHost() {
    return url.getHost();
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getStatusDescription() {
    return statusDescription;
  }

  public String getStorageErrorCode() {
    return storageErrorCode;
  }

  public String getStorageErrorMessage() {
    return storageErrorMessage;
  }

  public String getExpectedAppendPos() {
    return expectedAppendPos;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setMaskForSAS() {
    shouldMask = true;
  }

  public int getBytesSent() {
    return bytesSent;
  }

  public long getBytesReceived() {
    return bytesReceived;
  }

  public abstract String getResponseHeader(String httpHeader);

  public abstract Map<String, List<String>> getRequestHeaders();

  public abstract String getRequestHeader(String header);

  public abstract String getClientRequestId();

  public abstract void setHeader(String header, String value);

  /**
   * Gets and processes the HTTP response.
   *
   * @param buffer a buffer to hold the response entity body
   * @param offset an offset in the buffer where the data will being.
   * @param length the number of bytes to be written to the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public abstract void processResponse(byte[] buffer, int offset, int length) throws IOException;

  public int getResponseContentBuffer(byte[] buffer) {
    // Immutable byte[] is not possible, hence return a copy
    // spotbugs -  EI_EXPOSE_REP
    int length = Math.min(responseContentBuffer.length, buffer.length);
    System.arraycopy(responseContentBuffer, 0, buffer, 0, length);
    return length;
  }

  // Returns a trace message for the request
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(statusCode);
    sb.append(",");
    sb.append(storageErrorCode);
    sb.append(",");
    sb.append(expectedAppendPos);
    sb.append(",cid=");
    sb.append(getClientRequestId());
    sb.append(",rid=");
    sb.append(requestId);
    if (isTraceEnabled) {
      sb.append(",connMs=");
      sb.append(connectionTimeMs);
      sb.append(",sendMs=");
      sb.append(sendRequestTimeMs);
      sb.append(",recvMs=");
      sb.append(recvResponseTimeMs);
    }
    sb.append(",sent=");
    sb.append(bytesSent);
    sb.append(",recv=");
    sb.append(bytesReceived);
    sb.append(",");
    sb.append(method);
    sb.append(",");
    sb.append(getMaskedUrl());
    return sb.toString();
  }

  // Returns a trace message for the ABFS API logging service to consume
  public String getLogString() {

    final StringBuilder sb = new StringBuilder();
    sb.append("s=")
      .append(statusCode)
      .append(" e=")
      .append(storageErrorCode)
      .append(" ci=")
      .append(getClientRequestId())
      .append(" ri=")
      .append(requestId);

    if (isTraceEnabled) {
      sb.append(" ct=")
        .append(connectionTimeMs)
        .append(" st=")
        .append(sendRequestTimeMs)
        .append(" rt=")
        .append(recvResponseTimeMs);
    }

    sb.append(" bs=")
      .append(bytesSent)
      .append(" br=")
      .append(bytesReceived)
      .append(" m=")
      .append(method)
      .append(" u=")
      .append(getMaskedEncodedUrl());

    return sb.toString();
  }

  public String getMaskedUrl() {
    if (!shouldMask) {
      return url.toString();
    }
    if (maskedUrl != null) {
      return maskedUrl;
    }
    maskedUrl = UriUtils.getMaskedUrl(url);
    return maskedUrl;
  }

  public String getMaskedEncodedUrl() {
    if (maskedEncodedUrl != null) {
      return maskedEncodedUrl;
    }
    maskedEncodedUrl = UriUtils.encodedUrlStr(getMaskedUrl());
    return maskedEncodedUrl;
  }

  /**
   * Returns the elapsed time in milliseconds.
   * @param startTime request start time
   * @return total elapsed time
   */
  protected long elapsedTimeMs(final long startTime) {
    return (System.nanoTime() - startTime) / ONE_MILLION;
  }

  /**
   * Check null stream, this is to pass findbugs's redundant check for NULL
   * @param stream InputStream
   * @return if inputStream is null
   */
  protected boolean isNullInputStream(InputStream stream) {
    return stream == null ? true : false;
  }

  public static AbfsHttpOperation getAbfsHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsHttpOperationWithFixedResult httpOp
        = new AbfsHttpOperationWithFixedResult(url, method, httpStatus);
    return httpOp;
  }

  /**
   * Constructor for FixedResult instance, avoiding connection init.
   * @param url request url
   * @param method Http method
   * @param httpStatus HttpStatus
   */
  protected AbfsHttpOperation(final URL url,
      final String method,
      final int httpStatus) {
    this.isTraceEnabled = LOG.isTraceEnabled();
    this.url = url;
    this.method = method;
    this.statusCode = httpStatus;
  }

  public static class AbfsHttpOperationWithFixedResult extends AbfsHttpOperation {
    /**
     * Creates an instance to represent fixed results.
     * This is used in idempotency handling.
     *
     * @param url The full URL including query string parameters.
     * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
     * @param httpStatus StatusCode to hard set
     */
    public AbfsHttpOperationWithFixedResult(final URL url,
        final String method,
        final int httpStatus) {
      super(url, method, httpStatus);
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }

    @Override
    public Map<String, List<String>> getRequestHeaders() {
      return new HashMap<>();
    }


    @Override
    public String getRequestHeader(final String header) {
      return null;
    }

    @Override
    public String getClientRequestId() {
      return "";
    }

    @Override
    public void setHeader(final String header, final String value) { }

    @Override
    public void processResponse(final byte[] buffer,
        final int offset,
        final int length) throws IOException {

    }
  }
}
