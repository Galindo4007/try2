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
package org.apache.hadoop.hdfs.web.resources;

import java.net.HttpURLConnection;

/** Http GET operation parameter. */
public class GetOpParam extends HttpOpParam<GetOpParam.Op> {
  /** Get operations. */
  public enum Op implements HttpOpParam.Op {
    OPEN(true, HttpURLConnection.HTTP_OK),

    GETFILESTATUS(false, HttpURLConnection.HTTP_OK),
    LISTSTATUS(false, HttpURLConnection.HTTP_OK),
    GETCONTENTSUMMARY(false, HttpURLConnection.HTTP_OK),
    GETQUOTAUSAGE(false, HttpURLConnection.HTTP_OK),
    GETFILECHECKSUM(true, HttpURLConnection.HTTP_OK),

    GETHOMEDIRECTORY(false, HttpURLConnection.HTTP_OK),
    GETDELEGATIONTOKEN(false, HttpURLConnection.HTTP_OK, true),

    /**
     * GET_BLOCK_LOCATIONS is a private/stable API op. It returns a
     * {@link org.apache.hadoop.hdfs.protocol.LocatedBlocks}
     * json object.
     */
    GET_BLOCK_LOCATIONS(false, HttpURLConnection.HTTP_OK),
    /**
     * GETFILEBLOCKLOCATIONS is the public op that complies with
     * {@link org.apache.hadoop.fs.FileSystem#getFileBlockLocations}
     * interface.
     */
    GETFILEBLOCKLOCATIONS(false, HttpURLConnection.HTTP_OK),
    GETACLSTATUS(false, HttpURLConnection.HTTP_OK),
    GETXATTRS(false, HttpURLConnection.HTTP_OK),
    GETTRASHROOT(false, HttpURLConnection.HTTP_OK),
    LISTXATTRS(false, HttpURLConnection.HTTP_OK),

    GETALLSTORAGEPOLICY(false, HttpURLConnection.HTTP_OK),
    GETSTORAGEPOLICY(false, HttpURLConnection.HTTP_OK),

    GETECPOLICY(false, HttpURLConnection.HTTP_OK),

    NULL(false, HttpURLConnection.HTTP_NOT_IMPLEMENTED),

    CHECKACCESS(false, HttpURLConnection.HTTP_OK),
    LISTSTATUS_BATCH(false, HttpURLConnection.HTTP_OK),
    GETSERVERDEFAULTS(false, HttpURLConnection.HTTP_OK),
    GETSNAPSHOTDIFF(false, HttpURLConnection.HTTP_OK),
    GETSNAPSHOTDIFFLISTING(false, HttpURLConnection.HTTP_OK),
    GETSNAPSHOTTABLEDIRECTORYLIST(false, HttpURLConnection.HTTP_OK),
    GETLINKTARGET(false, HttpURLConnection.HTTP_OK),
    GETFILELINKSTATUS(false, HttpURLConnection.HTTP_OK),
    GETSTATUS(false, HttpURLConnection.HTTP_OK),
    GETECPOLICIES(false, HttpURLConnection.HTTP_OK),
    GETECCODECS(false, HttpURLConnection.HTTP_OK),
    GETTRASHROOTS(false, HttpURLConnection.HTTP_OK),
    GETSNAPSHOTLIST(false, HttpURLConnection.HTTP_OK);

    final boolean redirect;
    final int expectedHttpResponseCode;
    final boolean requireAuth;

    Op(final boolean redirect, final int expectedHttpResponseCode) {
      this(redirect, expectedHttpResponseCode, false);
    }

    Op(final boolean redirect, final int expectedHttpResponseCode,
        final boolean requireAuth) {
      this.redirect = redirect;
      this.expectedHttpResponseCode = expectedHttpResponseCode;
      this.requireAuth = requireAuth;
    }

    @Override
    public HttpOpParam.Type getType() {
      return HttpOpParam.Type.GET;
    }

    @Override
    public boolean getRequireAuth() {
      return requireAuth;
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public boolean getRedirect() {
      return redirect;
    }

    @Override
    public int getExpectedHttpResponseCode() {
      return expectedHttpResponseCode;
    }

    @Override
    public String toQueryString() {
      return NAME + "=" + this;
    }
  }

  private static final Domain<Op> DOMAIN = new Domain<>(NAME, Op.class);

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public GetOpParam(final String str) {
    super(DOMAIN, getOp(str));
  }

  private static Op getOp(String str) {
    try {
      return DOMAIN.parse(str);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(str + " is not a valid " + Type.GET
          + " operation.");
    }
  }

  @Override
  public String getName() {
    return NAME;
  }
}
