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

import java.net.*;
import java.util.regex.Pattern;

import org.apache.http.client.utils.URIBuilder;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SECURE_SCHEME;

/**
 * Utility class to help with Abfs url transformation to blob urls.
 */
public final class UriUtils {
  private static final String ABFS_URI_REGEX = "[^.]+\\.dfs\\.(preprod\\.){0,1}core\\.windows\\.net";
  private static final Pattern ABFS_URI_PATTERN = Pattern.compile(ABFS_URI_REGEX);

  /**
   * Checks whether a string includes abfs url.
   * @param string the string to check.
   * @return true if string has abfs url.
   */
  public static boolean containsAbfsUrl(final String string) {
    if (string == null || string.isEmpty()) {
      return false;
    }

    return ABFS_URI_PATTERN.matcher(string).matches();
  }

  /**
   * Extracts the account name from the host name.
   * @param hostName the fully-qualified domain name of the storage service
   *                 endpoint (e.g. {account}.dfs.core.windows.net.
   * @return the storage service account name.
   */
  public static String extractAccountNameFromHostName(final String hostName) {
    if (hostName == null || hostName.isEmpty()) {
      return null;
    }

    if (!containsAbfsUrl(hostName)) {
      return null;
    }

    String[] splitByDot = hostName.split("\\.");
    if (splitByDot.length == 0) {
      return null;
    }

    return splitByDot[0];
  }

  /**
   * Generate unique test path for multiple user tests.
   *
   * @return root test path
   */
  public static String generateUniqueTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? "/test" : "/" + testUniqueForkId + "/test";
  }

  private UriUtils() {
  }

  public static URL addSASToRequestUrl(URL requestUrl, String sasToken)
      throws URISyntaxException, MalformedURLException {
    URIBuilder uriBuilder = new URIBuilder(requestUrl.toURI());
    sasToken = (sasToken.startsWith("?") ? sasToken.substring(1) : sasToken);
    String[] sasQueryParamKVPairs = sasToken.split("&");
    for(String sasQueryParam : sasQueryParamKVPairs)
    {
      String key = sasQueryParam.substring(0, sasQueryParam.indexOf("="));
      String value =
          URLDecoder.decode(sasQueryParam.substring(sasQueryParam.indexOf("=") + 1));// new PR handles it in AbfsUriBuilder

      uriBuilder.addParameter(key, value);
    }

    return uriBuilder.build().toURL();
  }

  public static URI getQualifiedPathURI(URL url) throws URISyntaxException {
    int indexOfContainerNameEnd = url.getPath().indexOf("/", 1);
    String containerName =  (indexOfContainerNameEnd == -1) ?
        url.getPath().substring(1) :
        url.getPath().substring(1, indexOfContainerNameEnd);

    String scheme = url.toString().toLowerCase().startsWith("https") ?
        ABFS_SECURE_SCHEME :
        ABFS_SCHEME;

    URI qualifiedPathUri = new URI(scheme,
        containerName + "@" + url.getAuthority(),
        ((indexOfContainerNameEnd == -1) ?
            "" :
            url.getPath().substring(indexOfContainerNameEnd)), null, null);

    return qualifiedPathUri;
  }
}
