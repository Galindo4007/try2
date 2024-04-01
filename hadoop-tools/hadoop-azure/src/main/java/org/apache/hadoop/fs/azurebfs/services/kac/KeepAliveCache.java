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

package org.apache.hadoop.fs.azurebfs.services.kac;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KAC_DEFAULT_CONN_TTL;

/**
 * Connection-pooling heuristics adapted from JDK's connection pooling `KeepAliveCache`
 * <p>
 * Why this implementation is required in comparison to {@link org.apache.http.impl.conn.PoolingHttpClientConnectionManager}
 * connection-pooling:
 * <ol>
 * <li>PoolingHttpClientConnectionManager heuristic caches all the reusable connections it has created.
 * JDK's implementation only caches limited number of connections. The limit is given by JVM system
 * property "http.maxConnections". If there is no system-property, it defaults to 5.</li>
 * <li>In PoolingHttpClientConnectionManager, it expects the application to provide `setMaxPerRoute` and `setMaxTotal`,
 * which the implementation uses as the total number of connections it can create. For application using ABFS, it is not
 * feasible to provide a value in the initialisation of the connectionManager. JDK's implementation has no cap on the
 * number of connections it can create.</li>
 * </ol>
 */
public final class KeepAliveCache
    extends HashMap<KeepAliveCache.KeepAliveKey, KeepAliveCache.ClientVector>
    implements Runnable {

  private boolean threadShouldPause = true;

  private int maxConn;

  private long connectionIdleTTL = KAC_DEFAULT_CONN_TTL;

  private KeepAliveCache() {
    Thread thread = new Thread(this);
    thread.start();
    setMaxConn();
  }

  private void setMaxConn() {
    String sysPropMaxConn = System.getProperty(HTTP_MAX_CONN_SYS_PROP);
    if (sysPropMaxConn == null) {
      maxConn = DEFAULT_MAX_CONN_SYS_PROP;
    } else {
      maxConn = Integer.parseInt(sysPropMaxConn);
    }
  }

  public void setAbfsConfig(AbfsConfiguration abfsConfiguration) {
    this.maxConn = abfsConfiguration.getMaxApacheHttpClientCacheConnections();
    this.connectionIdleTTL = abfsConfiguration.getMaxApacheHttpClientConnectionIdleTime();
  }

  public long getConnectionIdleTTL() {
    return connectionIdleTTL;
  }

  private static final KeepAliveCache INSTANCE = new KeepAliveCache();

  @VisibleForTesting
  void close() {
    clear();
    setMaxConn();
  }

  public static KeepAliveCache getInstance() {
    return INSTANCE;
  }

  @VisibleForTesting
  void pauseThread() {
    threadShouldPause = false;
  }

  @VisibleForTesting
  void resumeThread() {
    threadShouldPause = true;
  }

  private int getKacSize() {
    return INSTANCE.maxConn;
  }

  @Override
  public void run() {
    while (true) {
      if (threadShouldPause) {
        kacCleanup();
      }
    }
  }

  private void kacCleanup() {
    try {
      Thread.sleep(connectionIdleTTL);
    } catch (InterruptedException ex) {
      return;
    }
    synchronized (this) {
      /* Remove all unused HttpClients.  Starting from the
       * bottom of the stack (the least-recently used first).
       * REMIND: It'd be nice to not remove *all* connections
       * that aren't presently in use.  One could have been added
       * a second ago that's still perfectly valid, and we're
       * needlessly axing it.  But it's not clear how to do this
       * cleanly, and doing it right may be more trouble than it's
       * worth.
       */

      long currentTime = System.currentTimeMillis();

      ArrayList<KeepAliveKey> keysToRemove
          = new ArrayList<KeepAliveKey>();

      for (Map.Entry<KeepAliveKey, ClientVector> entry : entrySet()) {
        KeepAliveKey key = entry.getKey();
        ClientVector v = entry.getValue();
        synchronized (v) {
          int i;

          for (i = 0; i < v.size(); i++) {
            KeepAliveEntry e = v.elementAt(i);
            if ((currentTime - e.idleStartTime) > v.nap) {
              HttpClientConnection hc = e.httpClientConnection;
              closeHtpClientConnection(hc);
            } else {
              break;
            }
          }
          v.subList(0, i).clear();

          if (v.size() == 0) {
            keysToRemove.add(key);
          }
        }
      }

      for (KeepAliveKey key : keysToRemove) {
        removeVector(key);
      }
    }
  }

  synchronized void removeVector(KeepAliveKey k) {
    super.remove(k);
  }

  public synchronized void put(final HttpRoute httpRoute,
      final HttpClientConnection httpClientConnection) {
    KeepAliveKey key = new KeepAliveKey(httpRoute);
    ClientVector v = super.get(key);
    if (v == null) {
      v = new ClientVector((int) connectionIdleTTL);
      v.put(httpClientConnection);
      super.put(key, v);
    } else {
      v.put(httpClientConnection);
    }
  }

  public synchronized HttpClientConnection get(HttpRoute httpRoute)
      throws IOException {

    KeepAliveKey key = new KeepAliveKey(httpRoute);
    ClientVector v = super.get(key);
    if (v == null) { // nothing in cache yet
      return null;
    }
    return v.get();
  }

  class ClientVector extends java.util.Stack<KeepAliveEntry> {

    private static final long serialVersionUID = -8680532108106489459L;

    // sleep time in milliseconds, before cache clear
    private int nap;

    ClientVector(int nap) {
      this.nap = nap;
    }

    synchronized HttpClientConnection get() throws IOException {
      if (empty()) {
        return null;
      } else {
        // Loop until we find a connection that has not timed out
        HttpClientConnection hc = null;
        long currentTime = System.currentTimeMillis();
        do {
          KeepAliveEntry e = pop();
          if ((currentTime - e.idleStartTime) > nap) {
            e.httpClientConnection.close();
          } else {
            hc = e.httpClientConnection;
          }
        } while ((hc == null) && (!empty()));
        return hc;
      }
    }

    /* return a still valid, unused HttpClient */
    synchronized void put(HttpClientConnection h) {
      if (size() >= getKacSize()) {
        closeHtpClientConnection(h);
        return;
      }
      push(new KeepAliveEntry(h, System.currentTimeMillis()));
    }

    /*
     * Do not serialize this class!
     */
    private void writeObject(java.io.ObjectOutputStream stream)
        throws IOException {
      throw new NotSerializableException();
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
      throw new NotSerializableException();
    }

    @Override
    public synchronized boolean equals(final Object o) {
      return super.equals(o);
    }

    @Override
    public synchronized int hashCode() {
      return super.hashCode();
    }
  }

  private void closeHtpClientConnection(final HttpClientConnection h) {
    try {
      h.close();
    } catch (IOException ignored) {

    }
  }


  static class KeepAliveKey {

    private final HttpRoute httpRoute;


    KeepAliveKey(HttpRoute httpRoute) {
      this.httpRoute = httpRoute;
    }

    /**
     * Determine whether or not two objects of this type are equal
     */
    @Override
    public boolean equals(Object obj) {
      return obj instanceof KeepAliveKey && httpRoute.getTargetHost()
          .getHostName()
          .equals(((KeepAliveKey) obj).httpRoute.getTargetHost().getHostName());
    }

    /**
     * The hashCode() for this object is the string hashCode() of
     * concatenation of the protocol, host name and port.
     */
    @Override
    public int hashCode() {
      String str = httpRoute.getTargetHost().getHostName() + ":"
          + httpRoute.getTargetHost().getPort();
      return str.hashCode();
    }
  }

  static class KeepAliveEntry {

    private final HttpClientConnection httpClientConnection;

    private final long idleStartTime;

    KeepAliveEntry(HttpClientConnection hc, long idleStartTime) {
      this.httpClientConnection = hc;
      this.idleStartTime = idleStartTime;
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof KeepAliveEntry) {
        return httpClientConnection.equals(
            ((KeepAliveEntry) o).httpClientConnection);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return httpClientConnection.hashCode();
    }
  }
}
