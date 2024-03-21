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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_FALLBACK_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;

/**
 * Implementation of {@link HttpOperation} for orchestrating calls using JDK's HttpURLConnection.
 */
public class AbfsHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsHttpOperation.class);

  private HttpURLConnection connection;

  private boolean connectionDisconnectedOnError = false;

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
    super(LOG, url, method, httpStatus);
  }

  protected HttpURLConnection getConnection() {
    return connection;
  }

  public String getClientRequestId() {
    return this.connection
        .getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
  }

  public String getResponseHeader(String httpHeader) {
    return connection.getHeaderField(httpHeader);
  }

  /**
   * Initializes a new HTTP request and opens the connection.
   *
   * @param url The full URL including query string parameters.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   * @param connectionTimeout The Connection Timeout value to be used while establishing http connection
   * @param readTimeout The Read Timeout value to be used with http connection while making a request
   * @throws IOException if an error occurs.
   */
  public AbfsHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final int connectionTimeout,
      final int readTimeout)
      throws IOException {
    super(LOG, url, method);

    this.connection = openConnection();
    if (this.connection instanceof HttpsURLConnection) {
      HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
      SSLSocketFactory sslSocketFactory
          = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        secureConn.setSSLSocketFactory(sslSocketFactory);
      }
    }

    this.connection.setConnectTimeout(connectionTimeout);
    this.connection.setReadTimeout(readTimeout);
    this.connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      setRequestProperty(header.getName(), header.getValue());
    }
  }

  /**
   * Sends the HTTP request.  Note that HttpUrlConnection requires that an
   * empty buffer be sent in order to set the "Content-Length: 0" header, which
   * is required by our endpoint.
   *
   * @param buffer the request entity body.
   * @param offset an offset into the buffer where the data beings.
   * @param length the length of the data in the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void sendPayload(byte[] buffer, int offset, int length)
      throws IOException {
    this.connection.setDoOutput(true);
    this.connection.setFixedLengthStreamingMode(length);
    if (buffer == null) {
      // An empty buffer is sent to set the "Content-Length: 0" header, which
      // is required by our endpoint.
      buffer = new byte[]{};
      offset = 0;
      length = 0;
    }

    // send the request body

    long startTime = 0;
    startTime = System.nanoTime();
    OutputStream outputStream = null;
    // Updates the expected bytes to be sent based on length.
    setExpectedBytesToBeSent(length);
    try {
      try {
        /* Without expect header enabled, if getOutputStream() throws
           an exception, it gets caught by the restOperation. But with
           expect header enabled we return back without throwing an exception
           for the correct response code processing.
         */
        outputStream = getConnOutputStream();
      } catch (IOException e) {
        connectionDisconnectedOnError = true;
        /* If getOutputStream fails with an expect-100 exception , we return back
           without throwing an exception to the caller. Else, we throw back the exception.
         */
        String expectHeader = getConnProperty(EXPECT);
        if (expectHeader != null && expectHeader.equals(HUNDRED_CONTINUE)
            && e instanceof ProtocolException
            && EXPECT_100_JDK_ERROR.equals(e.getMessage())) {
          LOG.debug(
              "Getting output stream failed with expect header enabled, returning back ",
              e);
          /*
           * In case expect-100 assertion has failed, headers and inputStream should not
           * be parsed. Reason being, conn.getHeaderField(), conn.getHeaderFields(),
           * conn.getInputStream() will lead to repeated server call.
           * ref: https://bugs.openjdk.org/browse/JDK-8314978.
           * Reading conn.responseCode() and conn.getResponseMessage() is safe in
           * case of Expect-100 error. Reason being, in JDK, it stores the responseCode
           * in the HttpUrlConnection object before throwing exception to the caller.
           */
          setStatusCode(getConnResponseCode());
          setStatusDescription(getConnResponseMessage());
          return;
        } else {
          LOG.debug(
              "Getting output stream failed without expect header enabled, throwing exception ",
              e);
          throw e;
        }
      }
      // update bytes sent for successful as well as failed attempts via the
      // accompanying statusCode.
      setBytesSent(length);

      // If this fails with or without expect header enabled,
      // it throws an IOException.
      outputStream.write(buffer, offset, length);
    } finally {
      // Closing the opened output stream
      if (outputStream != null) {
        outputStream.close();
      }
      setSendRequestTimeMs(elapsedTimeMs(startTime));
    }
  }

  @Override
  String getRequestProperty(final String headerName) {
    return connection.getRequestProperty(headerName);
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    return connection.getRequestProperties();
  }

  @Override
  InputStream getContentInputStream() throws IOException {
    return connection.getInputStream();
  }

  /**
   * Gets and processes the HTTP response.
   *
   * @param buffer a buffer to hold the response entity body
   * @param offset an offset in the buffer where the data will being.
   * @param length the number of bytes to be written to the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    if (connectionDisconnectedOnError) {
      LOG.debug("This connection was not successful or has been disconnected, "
          + "hence not parsing headers and inputStream");
      return;
    }
    processConnHeadersAndInputStreams(buffer, offset, length);
  }

  void processConnHeadersAndInputStreams(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    setStatusCode(getConnResponseCode());
    setRecvResponseTimeMs(elapsedTimeMs(startTime));

    setStatusDescription(getConnResponseMessage());

    String requestId = this.connection.getHeaderField(
        HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (requestId == null) {
      requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    setRequestId(requestId);

    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(getMethod())) {
      // If it is HEAD, and it is ERROR
      return;
    }

    parseResponse(buffer, offset, length);
  }

  public void setRequestProperty(String key, String value) {
    StringBuilder stringBuilder = new StringBuilder(value);
    if (X_MS_CLIENT_REQUEST_ID.equals(key)) {
      if (!ApacheHttpClientHealthMonitor.usable()) {
        stringBuilder.append("_").append(JDK_FALLBACK_IMPL);
      } else {
        stringBuilder.append("_").append(JDK_IMPL);
      }
    }
    this.getConnection().setRequestProperty(key, stringBuilder.toString());
  }

  /**
   * Open the HTTP connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    long start = System.nanoTime();
    try {
      return (HttpURLConnection) getUrl().openConnection();
    } finally {
      setConnectionTimeMs(elapsedTimeMs(start));
    }
  }

  @Override
  protected InputStream getErrorStream() {
    return connection.getErrorStream();
  }

  /**
   * Gets the connection request property for a key.
   * @param key The request property key.
   * @return request peoperty value.
   */
  String getConnProperty(String key) {
    return connection.getRequestProperty(key);
  }

  /**
   * Gets the connection url.
   * @return url.
   */
  URL getConnUrl() {
    return connection.getURL();
  }

  /**
   * Gets the connection request method.
   * @return request method.
   */
  String getConnRequestMethod() {
    return connection.getRequestMethod();
  }

  /**
   * Gets the connection response code.
   * @return response code.
   * @throws IOException
   */
  Integer getConnResponseCode() throws IOException {
    return connection.getResponseCode();
  }

  /**
   * Gets the connection output stream.
   * @return output stream.
   * @throws IOException
   */
  OutputStream getConnOutputStream() throws IOException {
    return connection.getOutputStream();
  }

  /**
   * Gets the connection response message.
   * @return response message.
   * @throws IOException
   */
  String getConnResponseMessage() throws IOException {
    return connection.getResponseMessage();
  }

  @VisibleForTesting
  boolean getConnectionDisconnectedOnError() {
    return connectionDisconnectedOnError;
  }

  public static class AbfsHttpOperationWithFixedResult
      extends AbfsHttpOperation {

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
  }
}
