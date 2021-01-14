/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.NetgroupCache.NetgroupCacheListAdder;

public class TestNetgroupCache {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestNetgroupCache.class);

  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String USER3 = "user3";
  private static final String GROUP1 = "group1";
  private static final String GROUP2 = "group2";

  /**
   * Pick items randomly from a list of String.
   *
   * @param originalList list to select items from.
   * @return a random selected set with size at least larger than half
   *         the original set.
   */
  private static Set<String> getRandomElements(List<String> originalList) {
    int cutOffElements =
        Math.abs(ThreadLocalRandom.current().nextInt())
            % (originalList.size() >> 1);
    int numberOfElements = originalList.size() - cutOffElements;
    Set<String> randomElements = new HashSet<>();
    for (int i = 0; i < numberOfElements; i++) {
      int rIndex =
          Math.abs(ThreadLocalRandom.current().nextInt()) % originalList.size();
      randomElements.add(originalList.get(rIndex));
    }
    return randomElements;
  }

  /**
   * Generate a random map between groups and users.
   *
   * @param groupsList the list where groups are added.
   * @param groupsCount number of groups generated.
   * @param usersList the list of the users to test.
   * @param usersCount the number of total users.
   * @return map between groups and users with size groupsCount.
   */
  private static Map<String, Set<String>> generateRandomMapping(
      List<String> groupsList, int groupsCount,
      List<String> usersList, int usersCount) {
    Map<String, Set<String>> groupToUsersMap = new HashMap<>();
    for (int i = 1; i <= usersCount; i++) {
      usersList.add(String.format("user-%03d", i));
    }
    for (int i = 1; i <= groupsCount; i++) {
      String groupName = String.format("group-%03d", i);
      groupsList.add(groupName);
      groupToUsersMap.put(groupName, getRandomElements(usersList));
    }
    return groupToUsersMap;
  }

  @After
  public void teardown() {
    NetgroupCache.clearDataForTesting();
  }

  /**
   * Cache two groups with a set of users.
   * Test membership correctness.
   */
  @Test
  public void testMembership() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER3);
    NetgroupCache.add(GROUP2, users);
    verifyGroupMembership(USER1, 2, GROUP1);
    verifyGroupMembership(USER1, 2, GROUP2);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 1, GROUP2);
  }

  /**
   * Cache a group with a set of users.
   * Test membership correctness.
   * Clear cache, remove a user from the group and cache the group
   * Test membership correctness.
   */
  @Test
  public void testUserRemoval() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 1, GROUP1);
    users.remove(USER2);
    NetgroupCache.clear();
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 0, null);
  }

  /**
   * Cache two groups with a set of users.
   * Test membership correctness.
   * Clear cache, cache only one group.
   * Test membership correctness.
   */
  @Test
  public void testGroupRemoval() {
    List<String> users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER3);
    NetgroupCache.add(GROUP2, users);
    verifyGroupMembership(USER1, 2, GROUP1);
    verifyGroupMembership(USER1, 2, GROUP2);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 1, GROUP2);
    NetgroupCache.clear();
    users = new ArrayList<String>();
    users.add(USER1);
    users.add(USER2);
    NetgroupCache.add(GROUP1, users);
    verifyGroupMembership(USER1, 1, GROUP1);
    verifyGroupMembership(USER2, 1, GROUP1);
    verifyGroupMembership(USER3, 0, null);
  }

  /**
   * A unit test to inject a race refreshing/adding the groups caching.
   *
   * @throws Exception if the unit test fails, or the injector throws
   *        an exception.
   */
  @Test
  public void testMultiThreadedAccess() throws Exception {
    int totalUsers = 100;
    int totalGroups = 20;
    List<String> usersList = new ArrayList<>();
    List<String> groupList = new ArrayList<>();

    Map<String, Set<String>> groupToUsersMap =
        generateRandomMapping(groupList, totalGroups, usersList, totalUsers);

    InjectRaceInNetgroupRefresh raceInjector =
        new InjectRaceInNetgroupRefresh(totalGroups);
    InjectRaceInNetgroupRefresh.set(raceInjector);
    JNINetgroupCacheAdderForTesting bulkAdder =
        new JNINetgroupCacheAdderForTesting(groupToUsersMap);
    int phaseACount = totalGroups / 2;
    Thread cacheAppendRunner = new Thread(() -> {
      // add the groups from 10-19 to the cache;
      raceInjector.blockForPhaseClearing(phaseACount);
      try {
        bulkAdder.addGroupToCacheInBulk(
            groupList.subList(phaseACount, totalGroups));
      } catch (IOException e) {
        raceInjector.setFailure(e);
        LOG.error("exception addGroupToCacheInBulk", e);
      }
    });
    // add the first 10 groups to the cache;
    cacheAppendRunner.start();
    bulkAdder.addGroupToCacheInBulk(groupList.subList(0, phaseACount));
    NetgroupCache.refreshCacheCB(bulkAdder);
    // now we should have 20 groups in the bulkAdder
    cacheAppendRunner.join();
    Assert.assertNull(raceInjector.getFailure());
    assertEquals(totalGroups, NetgroupCache.getCachedGroupsForTesting().size());
  }

  private void verifyGroupMembership(String user, int size, String group) {
    List<String> groups = new ArrayList<String>();
    NetgroupCache.getNetgroups(user, groups);
    assertEquals(size, groups.size());
    if (size > 0) {
      boolean present = false;
      for (String groupEntry:groups) {
        if (groupEntry.equals(group)) {
          present = true;
          break;
        }
      }
      assertTrue(present);
    }
  }

  static class InjectRaceInNetgroupRefresh extends NetgroupCacheFaultInjector {
    private final Object signalLock = new Object();
    private final int expectedGroupCount;
    private final Set<String> addedGroups;
    private final AtomicBoolean inClearingPhase;
    private Throwable throwableFailure;

    InjectRaceInNetgroupRefresh(int groupCount) {
      expectedGroupCount = groupCount;
      addedGroups = ConcurrentHashMap.newKeySet();
      throwableFailure = null;
      inClearingPhase = new AtomicBoolean(false);
    }

    @Override
    public void checkPointResettingBeforeClearing() {
      inClearingPhase.set(true);
      blockForGroupsToBeAdded(expectedGroupCount);
    }

    @Override
    public void checkPointPostAddingBulkGroup(Collection<String> groups) {
      addedGroups.addAll(groups);
      synchronized (signalLock) {
        signalLock.notifyAll();
      }
    }

    public void blockForPhaseClearing(int groupCnt) {
      while (!inClearingPhase.get()) {
        blockForGroupsToBeAdded(groupCnt);
      }
    }

    public void blockForGroupsToBeAdded(int groupCnt) {
      try {
        synchronized (signalLock) {
          while (addedGroups.size() < groupCnt) {
            signalLock.wait(1000);
          }
        }
      } catch (InterruptedException e) {
        throwableFailure = e;
        LOG.error("Error waiting for count latch", e);
      }
    }

    Throwable getFailure() {
      return throwableFailure;
    }

    void setFailure(Throwable failure) {
      if (throwableFailure == null) {
        throwableFailure = failure;
      }
    }
  }

  static class JNINetgroupCacheAdderForTesting extends NetgroupCacheListAdder {
    private final Map<String, Set<String>> groupUsersMap;

    JNINetgroupCacheAdderForTesting(Map<String, Set<String>> groupUserLookup) {
      groupUsersMap = groupUserLookup;
    }

    @Override
    public boolean isCacheableKey(String entryKey, boolean forceRefresh) {
      return !NetgroupCache.isCached(entryKey);
    }

    @Override
    Set<String> getValuesForEntryKey(String entryKey) {
      return groupUsersMap.get(entryKey);
    }

    @Override
    public void addGroupToCacheInBulkInternal(Collection<String> groups) {
      for (String group: groups) {
        NetgroupCache.add(group, groupUsersMap.get(group));
      }
    }
  }
}
