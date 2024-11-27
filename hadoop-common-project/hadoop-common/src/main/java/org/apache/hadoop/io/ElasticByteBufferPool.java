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
package org.apache.hadoop.io;

import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a simple ByteBufferPool which just creates ByteBuffers as needed.
 * It also caches ByteBuffers after they're released.  It will always return
 * the smallest cached buffer with at least the capacity you request.
 * We don't try to do anything clever here like try to limit the maximum cache
 * size.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ElasticByteBufferPool implements ByteBufferPool {
  protected static final class Key implements Comparable<Key> {
    private final int capacity;
    private final long insertionTime;

    Key(int capacity, long insertionTime) {
      this.capacity = capacity;
      this.insertionTime = insertionTime;
    }

    @Override
    public int compareTo(Key other) {
      return ComparisonChain.start().
          compare(capacity, other.capacity).
          compare(insertionTime, other.insertionTime).
          result();
    }

    @Override
    public boolean equals(Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        Key o = (Key)rhs;
        return (compareTo(o) == 0);
      } catch (ClassCastException e) {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(capacity).
          append(insertionTime).
          toHashCode();
    }
  }

  /**
   * Get the size of the buffer pool, for the specified buffer type.
   *
   * @param direct Whether the size is returned for direct buffers
   * @return The size
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract int getCurrentBuffersCount(boolean direct);
}
