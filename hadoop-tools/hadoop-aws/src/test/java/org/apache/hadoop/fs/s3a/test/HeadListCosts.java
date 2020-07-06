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

package org.apache.hadoop.fs.s3a.test;

/**
 * Declaration of the costs of head and list calls for various FS IO operations.
 */
public class HeadListCosts {

  /** Head costs for getFileStatus() directory probe: {@value}. */
  public static final int GFS_DIR_PROBE_H = 0;

  /** List costs for getFileStatus() directory probe: {@value}. */
  public static final int GFS_DIR_PROBE_L = 1;


  /** List costs for getFileStatus() on a non-empty directory: {@value}. */
  public static final int GFS_DIR_L = GFS_DIR_PROBE_L;

  /** List costs for getFileStatus() on an non-empty directory: {@value}. */
  public static final int GFS_EMPTY_DIR_L = GFS_DIR_PROBE_L;

  /** Head cost getFileStatus() file probe only. */
  public static final int GFS_FILE_PROBE_H = 1;

  /** Head costs getFileStatus() no file or dir. */
  public static final int GFS_FNFE_H = GFS_FILE_PROBE_H;

  /** List costs for getFileStatus() on an empty path: {@value}. */

  public static final int GFS_FNFE_L = GFS_DIR_PROBE_L;

  /**
   * Cost of renaming a file to a diffrent directory.
   * LIST on dest not found, look for dest dir, and then, at
   * end of rename, whether a parent dir needs to be created.
   */
  public static final int RENAME_SINGLE_FILE_RENAME_DIFFERENT_DIR_L =
      GFS_FNFE_L + GFS_DIR_L * 2;
                             
  /** source is found, dest not found, copy metadataRequests */
  public static final int RENAME_SINGLE_FILE_RENAME_H =
      GFS_FILE_PROBE_H + GFS_FNFE_H + 1;

  /** getFileStatus() directory which is non-empty. */
  public static final int GFS_DIR_H = GFS_FILE_PROBE_H;

  /** getFileStatus() directory marker which exists. */
  public static final int GFS_MARKER_H = GFS_FILE_PROBE_H;

  /** getFileStatus() on a file which exists. */
  public static final int GFS_SINGLE_FILE_H = GFS_FILE_PROBE_H;

  public static final int GFS_FILE_PROBE_L = 0;

  public static final int GFS_SINGLE_FILE_L = 0;

  public static final int DELETE_OBJECT_REQUEST = 1;

  public static final int DELETE_MARKER_REQUEST = 1;

  /** listLocatedStatus always does a list. */
  public static final int LIST_LOCATED_STATUS_L = 1;
  
  
}
