package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

public class AbfsAHCHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsAHCHttpOperation.class);

  private static Map<String, AbfsApacheHttpClient> abfsApacheHttpClientMap = new HashMap<>();

  private AbfsApacheHttpClient abfsApacheHttpClient;

  public HttpRequestBase httpRequestBase;

  private HttpResponse httpResponse;

  private final AbfsApacheHttpClient.AbfsHttpClientContext abfsHttpClientContext
      = new AbfsApacheHttpClient.AbfsHttpClientContext();

  private synchronized void setAbfsApacheHttpClient(final AbfsConfiguration abfsConfiguration, final String clientId) {
    AbfsApacheHttpClient client = abfsApacheHttpClientMap.get(clientId);
    if(client == null) {
      client = new AbfsApacheHttpClient(DelegatingSSLSocketFactory.getDefaultFactory(), abfsConfiguration);
      abfsApacheHttpClientMap.put(clientId, abfsApacheHttpClient);
    }
    abfsApacheHttpClient = client;
  }

  static void removeClient(final String clientId) {
    abfsApacheHttpClientMap.remove(clientId);
  }

  private AbfsAHCHttpOperation(final URL url, final String method, final List<AbfsHttpHeader> requestHeaders) {
    super(LOG);
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
  }

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsConfiguration abfsConfiguration,
      final String clientId) {
    super(LOG);
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    setAbfsApacheHttpClient(abfsConfiguration, clientId);
  }


  public static AbfsAHCHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsAHCHttpOperation abfsApacheHttpClientHttpOperation = new AbfsAHCHttpOperation(url, method, new ArrayList<>());
    abfsApacheHttpClientHttpOperation.statusCode = httpStatus;
    return abfsApacheHttpClientHttpOperation;
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if(entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    return null;
  }

  @Override
  URL getConnUrl() {
    return url;
  }

  @Override
  String getConnRequestMethod() {
    return null;
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return null;
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return null;
  }

  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    try {
      long startTime = 0;
      startTime = System.nanoTime();
      httpResponse = abfsApacheHttpClient.execute(httpRequestBase, abfsHttpClientContext);
      sendRequestTimeMs = abfsHttpClientContext.sendTime;
      recvResponseTimeMs = abfsHttpClientContext.readTime;
    } catch (AbfsApacheHttpExpect100Exception ex) {
      LOG.debug(
          "Getting output stream failed with expect header enabled, returning back ",
          ex);
      httpResponse = ex.getHttpResponse();
    }
    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    this.statusCode = httpResponse.getStatusLine().getStatusCode();

    this.statusDescription = httpResponse.getStatusLine().getReasonPhrase();

    this.requestId = getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (this.requestId == null) {
      this.requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        getResponseHeaders(httpResponse));

    parseResponse(buffer, offset, length);

//    if(shouldKillConn()) {
//      abfsApacheHttpClient.destroyConn(
//          abfsHttpClientContext.httpClientConnection);
//    } else {
//      abfsApacheHttpClient.releaseConn(
//          abfsHttpClientContext.httpClientConnection, abfsHttpClientContext);
//    }
  }

  private boolean shouldKillConn() {
    return false;
  }

  private Map<String, List<String>> getResponseHeaders(final HttpResponse httpResponse) {
    if(httpResponse == null || httpResponse.getAllHeaders() == null) {
      return new HashMap<>();
    }
    Map<String, List<String>> map = new HashMap<>();
    for(Header header : httpResponse.getAllHeaders()) {
      map.put(header.getName(), new ArrayList<String>(
          Collections.singleton(header.getValue())));
    }
    return map;
  }

  @Override
  public void setRequestProperty(final String key, final String value) {
    setHeader(key, value);
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    Map<String, List<String>> map = new HashMap<>();
    for(AbfsHttpHeader header : requestHeaders) {
      map.put(header.getName(), new ArrayList<String>(){{add(header.getValue());}});
    }
    return map;
  }

  @Override
  public String getResponseHeader(final String headerName) {
    Header header = httpResponse.getFirstHeader(headerName);
    if(header != null) {
      return header.getValue();
    }
    return null;
  }

  @Override
  InputStream getContentInputStream()
      throws IOException {
    if(httpResponse == null) {
      return null;
    }
    HttpEntity entity = httpResponse.getEntity();
    if(entity != null) {
      return httpResponse.getEntity().getContent();
    }
    return null;
  }

  public void sendRequest(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    try {
      HttpRequestBase httpRequestBase = null;
      if (HTTP_METHOD_PUT.equals(method)) {
        httpRequestBase = new HttpPut(url.toURI());
      }
      if(HTTP_METHOD_PATCH.equals(method)) {
        httpRequestBase = new HttpPatch(url.toURI());
      }
      if(HTTP_METHOD_POST.equals(method)) {
        httpRequestBase = new HttpPost(url.toURI());
      }
      if(httpRequestBase != null) {

        this.expectedBytesToBeSent = length;
        this.bytesSent = length;
        if(buffer != null) {
          HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
              TEXT_PLAIN);
          ((HttpEntityEnclosingRequestBase)httpRequestBase).setEntity(httpEntity);
        }
      } else {
        if(HTTP_METHOD_GET.equals(method)) {
          httpRequestBase = new HttpGet(url.toURI());
        }
        if(HTTP_METHOD_DELETE.equals(method)) {
          httpRequestBase = new HttpDelete((url.toURI()));
        }
        if(HTTP_METHOD_HEAD.equals(method)) {
          httpRequestBase = new HttpHead(url.toURI());
        }
      }
      translateHeaders(httpRequestBase, requestHeaders);
      this.httpRequestBase = httpRequestBase;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpRequestBase httpRequestBase, final List<AbfsHttpHeader> requestHeaders) {
    for(AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  public void setHeader(String name, String val) {
    requestHeaders.add(new AbfsHttpHeader(name, val));
  }

  @Override
  public String getRequestProperty(String name) {
    for(AbfsHttpHeader header : requestHeaders) {
      if(header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return "";
  }

  public String getClientRequestId() {
    for(AbfsHttpHeader header : requestHeaders) {
      if(X_MS_CLIENT_REQUEST_ID.equals(header.getName())) {
        return header.getValue();
      }
    }
    return "";
  }

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
    sb.append(",connMs=");
    sb.append(connectionTimeMs);
    sb.append(",sendMs=");
    sb.append(sendRequestTimeMs);
    sb.append(",recvMs=");
    sb.append(recvResponseTimeMs);
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
}
