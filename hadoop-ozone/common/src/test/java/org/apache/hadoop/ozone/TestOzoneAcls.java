/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class is to test acl storage and retrieval in ozone store.
 */
public class TestOzoneAcls {

  @Test
  public void testAclParse() {
    HashMap<String, Boolean> testMatrix;
    testMatrix = new HashMap<>();

    testMatrix.put("user:bilbo:r", Boolean.TRUE);
    testMatrix.put("user:bilbo:w", Boolean.TRUE);
    testMatrix.put("user:bilbo:rw", Boolean.TRUE);
    testMatrix.put("user:bilbo:a", Boolean.TRUE);
    testMatrix.put("    user:bilbo:a   ", Boolean.TRUE);


    // ACLs makes no judgement on the quality of
    // user names. it is for the userAuth interface
    // to determine if a user name is really a name
    testMatrix.put(" user:*:rw", Boolean.TRUE);
    testMatrix.put(" user:~!:rw", Boolean.TRUE);


    testMatrix.put("", Boolean.FALSE);
    testMatrix.put(null, Boolean.FALSE);
    testMatrix.put(" user:bilbo:", Boolean.FALSE);
    testMatrix.put(" user:bilbo:rx", Boolean.TRUE);
    testMatrix.put(" user:bilbo:mk", Boolean.FALSE);
    testMatrix.put(" user::rw", Boolean.FALSE);
    testMatrix.put("user11:bilbo:rw", Boolean.FALSE);
    testMatrix.put(" user:::rw", Boolean.FALSE);

    testMatrix.put(" group:hobbit:r", Boolean.TRUE);
    testMatrix.put(" group:hobbit:w", Boolean.TRUE);
    testMatrix.put(" group:hobbit:rw", Boolean.TRUE);
    testMatrix.put(" group:hobbit:a", Boolean.TRUE);
    testMatrix.put(" group:*:rw", Boolean.TRUE);
    testMatrix.put(" group:~!:rw", Boolean.TRUE);

    testMatrix.put(" group:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:hobbit:rx", Boolean.TRUE);
    testMatrix.put(" group:hobbit:mk", Boolean.FALSE);
    testMatrix.put(" group::", Boolean.FALSE);
    testMatrix.put(" group::rw", Boolean.FALSE);
    testMatrix.put(" group22:hobbit:", Boolean.FALSE);
    testMatrix.put(" group:::rw", Boolean.FALSE);

    testMatrix.put("JUNK group:hobbit:r", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:w", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:hobbit:a", Boolean.FALSE);
    testMatrix.put("JUNK group:*:rw", Boolean.FALSE);
    testMatrix.put("JUNK group:~!:rw", Boolean.FALSE);

    testMatrix.put(" world::r", Boolean.TRUE);
    testMatrix.put(" world::w", Boolean.TRUE);
    testMatrix.put(" world::rw", Boolean.TRUE);
    testMatrix.put(" world::a", Boolean.TRUE);

    testMatrix.put(" world:bilbo:w", Boolean.FALSE);
    testMatrix.put(" world:bilbo:rw", Boolean.FALSE);

    Set<String> keys = testMatrix.keySet();
    for (String key : keys) {
      if (testMatrix.get(key)) {
        OzoneAcl.parseAcl(key);
      } else {
        try {
          OzoneAcl.parseAcl(key);
          // should never get here since parseAcl will throw
          fail("An exception was expected but did not happen. Key: " + key);
        } catch (IllegalArgumentException e) {
          // nothing to do
        }
      }
    }
  }

  @Test
  public void testAclValues() {
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    assertEquals(acl.getName(), "bilbo");
    assertEquals(Arrays.asList(ACLType.READ, ACLType.WRITE), acl.getRights());
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:a");
    assertEquals("bilbo", acl.getName());
    assertEquals(Arrays.asList(ACLType.ALL), acl.getRights());
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:r");
    assertEquals("bilbo", acl.getName());
    assertEquals(Arrays.asList(ACLType.READ), acl.getRights());
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("user:bilbo:w");
    assertEquals("bilbo", acl.getName());
    assertEquals(Arrays.asList(ACLType.WRITE), acl.getRights());
    assertEquals(ACLIdentityType.USER, acl.getType());

    acl = OzoneAcl.parseAcl("group:hobbit:a");
    assertEquals(acl.getName(), "hobbit");
    assertEquals(Arrays.asList(ACLType.ALL), acl.getRights());
    assertEquals(ACLIdentityType.GROUP, acl.getType());

    acl = OzoneAcl.parseAcl("world::a");
    assertEquals(acl.getName(), "");
    assertEquals(Arrays.asList(ACLType.ALL), acl.getRights());
    assertEquals(ACLIdentityType.WORLD, acl.getType());
  }

}
