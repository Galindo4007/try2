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

import java.util.Comparator;

import org.apache.hadoop.fs.Path;

/**
 * A comparator of path ordering where those paths which are higher up
 * the tree come first.
 * This can be used to ensure the sort order of changes.
 *
 * Policy
 * <ol>
 *   <li>higher up entries come first</li>
 *   <li>Root is topmost</li>
 *   <li>repeated sorts do not change the order</li>
 * </ol>
 */
class PathOrderComparators {


  static final Comparator<Path> TOPMOST_PATH_FIRST
      = new TopmostFirst();

  static final Comparator<Path> TOPMOST_PATH_LAST
      = new TopmostLast();

  static final Comparator<PathMetadata> TOPMOST_PM_FIRST
      = new PathMetadataComparator(TOPMOST_PATH_FIRST);

  static final Comparator<PathMetadata> TOPMOST_PM_LAST
      = new PathMetadataComparator(TOPMOST_PATH_LAST);

  private static class TopmostFirst implements Comparator<Path> {

    @Override
    public int compare(Path pathL, Path pathR) {
      // exist fast on equal values.
      if (pathL.equals(pathR)) {
        return 0;
      }
      int depthL = pathL.depth();
      int depthR = pathR.depth();
      if (depthL < depthR) {
        // left is higher up than the right.
        return -1;
      }
      if (depthR < depthL) {
        // right is higher up than the left
        return 1;
      }
      // and if they are of equal depth, use the "classic" comparator
      // of paths.
      return pathL.compareTo(pathR);
    }
  }

  private static final class TopmostLast extends TopmostFirst {

    @Override
    public int compare(final Path pathL, final Path pathR) {
      int compare = super.compare(pathL, pathR);
      if (compare < 0) {
        return 1;
      }
      if (compare > 0) {
        return -1;
      }
      return 0;
    }

  }

  private static final class PathMetadataComparator implements
      Comparator<PathMetadata> {

    private final Comparator<Path> inner;

    public PathMetadataComparator(final Comparator<Path> inner) {
      this.inner = inner;
    }

    @Override
    public int compare(final PathMetadata o1, final PathMetadata o2) {
      return inner.compare(o1.getFileStatus().getPath(),
          o2.getFileStatus().getPath());
    }
  }
}
