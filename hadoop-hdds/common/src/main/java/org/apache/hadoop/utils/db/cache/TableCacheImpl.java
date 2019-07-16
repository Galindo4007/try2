/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Cache implementation for the table. Depending on the cache clean up policy
 * this cache will be full cache or partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#AFTER_FLUSH},
 * this will be a partial cache.
 *
 * If cache cleanup policy is set as {@link CacheCleanupPolicy#NEVER},
 * this will be a full cache.
 */
@Private
@Evolving
public class TableCacheImpl<CACHEKEY extends CacheKey,
    CACHEVALUE extends CacheValue> implements TableCache<CACHEKEY, CACHEVALUE> {

  private final ConcurrentHashMap<CACHEKEY, CACHEVALUE> cache;
  private final TreeSet<EpochEntry<CACHEKEY>> epochEntries;
  private ExecutorService executorService;
  private CacheCleanupPolicy cleanupPolicy;



  public TableCacheImpl(CacheCleanupPolicy cleanupPolicy) {
    cache = new ConcurrentHashMap<>();
    epochEntries = new TreeSet<>();
    // Created a singleThreadExecutor, so one cleanup will be running at a
    // time.
    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("PartialTableCache Cleanup Thread - %d").build();
    executorService = Executors.newSingleThreadExecutor(build);
    this.cleanupPolicy = cleanupPolicy;
  }

  @Override
  public CACHEVALUE get(CACHEKEY cachekey) {
    return cache.get(cachekey);
  }

  @Override
  public void put(CACHEKEY cacheKey, CACHEVALUE value) {
    cache.put(cacheKey, value);
    epochEntries.add(new EpochEntry<>(value.getEpoch(), cacheKey));
  }

  @Override
  public void cleanup(long epoch) {
    // If it is never do nothing.
    if (cleanupPolicy == CacheCleanupPolicy.AFTER_FLUSH) {
      executorService.submit(() -> evictCache(epoch));
    }
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public Iterator<Map.Entry<CACHEKEY, CACHEVALUE>> iterator() {
    return cache.entrySet().iterator();
  }

  private void evictCache(long epoch) {
    EpochEntry<CACHEKEY> currentEntry = null;
    for (Iterator<EpochEntry<CACHEKEY>> iterator = epochEntries.iterator();
         iterator.hasNext();) {
      currentEntry = iterator.next();
      CACHEKEY cachekey = currentEntry.getCachekey();
      CacheValue cacheValue = cache.computeIfPresent(cachekey, ((k, v) -> {
        if (v.getEpoch() <= epoch) {
          iterator.remove();
          return null;
        }
        return v;
      }));
      // If currentEntry epoch is greater than epoch, we have deleted all
      // entries less than specified epoch. So, we can break.
      if (cacheValue != null && cacheValue.getEpoch() >= epoch) {
        break;
      }
    }
  }

  /**
   * Cleanup policies for table cache.
   */
  public enum CacheCleanupPolicy {
    NEVER, // Cache will not be cleaned up. This mean's the table maintains
    // full cache.
    AFTER_FLUSH // Cache will be cleaned up, once after flushing to DB.
  }
}
