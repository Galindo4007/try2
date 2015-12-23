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

package org.apache.hadoop.yarn.client.api.impl;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestTimelineClient {

  private TimelineClientImpl client;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    client = createTimelineClient(conf);
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.stop();
    }
  }

  @Test
  public void testPostEntities() throws Exception {
    mockEntityClientResponse(client, ClientResponse.Status.OK, false, false);
    TimelinePutResponse response = client.putEntities(generateEntity());
    Assert.assertEquals(0, response.getErrors().size());
  }

  @Test
  public void testPostEntitiesWithError() throws Exception {
    mockEntityClientResponse(client, ClientResponse.Status.OK, true, false);
    TimelinePutResponse response = client.putEntities(generateEntity());
    Assert.assertEquals(1, response.getErrors().size());
    Assert.assertEquals("test entity id", response.getErrors().get(0)
        .getEntityId());
    Assert.assertEquals("test entity type", response.getErrors().get(0)
        .getEntityType());
    Assert.assertEquals(TimelinePutResponse.TimelinePutError.IO_EXCEPTION,
        response.getErrors().get(0).getErrorCode());
  }

  @Test
  public void testPostIncompleteEntities() throws Exception {
    try {
      client.putEntities(new TimelineEntity());
      Assert.fail("Exception should have been thrown");
    } catch (YarnException e) {
    }
  }

  @Test
  public void testPostEntitiesNoResponse() throws Exception {
    mockEntityClientResponse(
        client, ClientResponse.Status.INTERNAL_SERVER_ERROR, false, false);
    try {
      client.putEntities(generateEntity());
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      assertExceptionTextContains(e, TimelineClientImpl.ERROR_NO_ATS_RESPONSE);
    }
  }

  @Test
  public void testPostEntitiesConnectionRefused() throws Exception {
    mockEntityClientResponse(client, null, false, true);
    try {
      client.putEntities(generateEntity());
      Assert.fail("Exception is expected");
    } catch (RuntimeException re) {
      Assert.assertTrue(re instanceof ClientHandlerException);
    }
  }

  @Test
  public void testPutDomain() throws Exception {
    mockDomainClientResponse(client, ClientResponse.Status.OK, false);
    client.putDomain(generateDomain());
  }

  @Test
  public void testPutDomainNoResponse() throws Exception {
    mockDomainClientResponse(client, ClientResponse.Status.FORBIDDEN, false);
    try {
      client.putDomain(generateDomain());
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      assertExceptionTextContains(e, TimelineClientImpl.ERROR_NO_ATS_RESPONSE);
    }
  }

  private void assertExceptionTextContains(Exception e, String text) {
    if (!e.toString().contains(text)) {
      throw new AssertionError("Did not find \"" + text + "\" in " + e, e);
    }
  }

  @Test
  public void testPutDomainConnectionRefused() throws Exception {
    mockDomainClientResponse(client, null, true);
    try {
      client.putDomain(generateDomain());
      Assert.fail("Exception is expected");
    } catch (ClientHandlerException re) {
      // expected
    }
  }

  @Test
  public void testCheckRetryCount() throws Exception {
    try {
      YarnConfiguration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        -2);
      createTimelineClient(conf);
      Assert.fail("IllegalArgumentException is expected");
    } catch(IllegalArgumentException e) {
      assertExceptionTextContains(e,
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
    }

    try {
      YarnConfiguration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
          0);
      createTimelineClient(conf);
      Assert.fail("Exception is expected");
    } catch(IllegalArgumentException e) {
      assertExceptionTextContains(e,
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
    }
    int newMaxRetries = 5;
    long newIntervalMs = 500;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        newMaxRetries);
    conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        newIntervalMs);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    ServiceOperations.stop(client);
    client = createTimelineClient(conf);
    try {
      // This call should fail because there is no timeline server
      client.putEntities(generateEntity());
      Assert.fail("Exception expected! "
          + "Timeline server should be off to run this test. ");
    } catch (RuntimeException ce) {
      assertRetryException(client, ce);
    }
  }

  @Test
  public void testDelegationTokenOperationsRetry() throws Exception {
    int newMaxRetries = 5;
    long newIntervalMs = 500;
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
        newMaxRetries);
    conf.setLong(YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        newIntervalMs);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    // use kerberos to bypass the issue in HADOOP-11215
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    ServiceOperations.stop(client);
    client = createTimelineClient(conf);
    TestTimelineDelegationTokenSecretManager dtManager =
        new TestTimelineDelegationTokenSecretManager();
    try {
      dtManager.startThreads();
      Thread.sleep(3000);

      try {
        // try getting a delegation token
        client.getDelegationToken(
          UserGroupInformation.getCurrentUser().getShortUserName());
        assertFail();
      } catch (RuntimeException ce) {
        assertRetryException(client, ce);
      }

      try {
        // try renew a delegation token
        TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier(
                new Text("tester"), new Text("tester"), new Text("tester"));
        client.renewDelegationToken(
            new Token<TimelineDelegationTokenIdentifier>(timelineDT.getBytes(),
                dtManager.createPassword(timelineDT),
                timelineDT.getKind(),
                new Text("0.0.0.0:8188")));
        assertFail();
      } catch (RuntimeException ce) {
        assertRetryException(client, ce);
      }

      try {
        // try cancel a delegation token
        TimelineDelegationTokenIdentifier timelineDT =
            new TimelineDelegationTokenIdentifier(
                new Text("tester"), new Text("tester"), new Text("tester"));
        client.cancelDelegationToken(
            new Token<TimelineDelegationTokenIdentifier>(timelineDT.getBytes(),
                dtManager.createPassword(timelineDT),
                timelineDT.getKind(),
                new Text("0.0.0.0:8188")));
        assertFail();
      } catch (RuntimeException ce) {
        assertRetryException(client, ce);
      }
    } finally {
      dtManager.stopThreads();
    }
  }

  private static void assertFail() {
    Assert.fail("Exception expected! "
        + "Timeline server should be off to run this test.");
  }

  private void assertRetryException(TimelineClientImpl timelineClient, Exception ce) {
    assertExceptionTextContains(ce, TimelineClientImpl.ERROR_RETRIES_EXCEEDED);
    // we would expect this exception here, check if the client has retried
    Assert.assertTrue("Retry filter didn't perform any retries! ",
        timelineClient.connectionRetry.getRetired());
  }

  private static ClientResponse mockEntityClientResponse(
      TimelineClientImpl client, ClientResponse.Status status,
      boolean hasError, boolean hasRuntimeError) {
    ClientResponse response = mock(ClientResponse.class);
    if (hasRuntimeError) {
      doThrow(new ClientHandlerException(new ConnectException())).when(client)
          .doPostingObject(any(TimelineEntities.class), any(String.class));
      return response;
    }
    doReturn(response).when(client)
        .doPostingObject(any(TimelineEntities.class), any(String.class));
    when(response.getClientResponseStatus()).thenReturn(status);
    TimelinePutResponse.TimelinePutError error =
        new TimelinePutResponse.TimelinePutError();
    error.setEntityId("test entity id");
    error.setEntityType("test entity type");
    error.setErrorCode(TimelinePutResponse.TimelinePutError.IO_EXCEPTION);
    TimelinePutResponse putResponse = new TimelinePutResponse();
    if (hasError) {
      putResponse.addError(error);
    }
    when(response.getEntity(TimelinePutResponse.class)).thenReturn(putResponse);
    return response;
  }

  private static ClientResponse mockDomainClientResponse(
      TimelineClientImpl client, ClientResponse.Status status,
      boolean hasRuntimeError) {
    ClientResponse response = mock(ClientResponse.class);
    if (hasRuntimeError) {
      doThrow(new ClientHandlerException(new ConnectException())).when(client)
          .doPostingObject(any(TimelineDomain.class), any(String.class));
      return response;
    }
    doReturn(response).when(client)
        .doPostingObject(any(TimelineDomain.class), any(String.class));
    when(response.getClientResponseStatus()).thenReturn(status);
    return response;
  }

  private static TimelineEntity generateEntity() {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("entity id");
    entity.setEntityType("entity type");
    entity.setStartTime(System.currentTimeMillis());
    for (int i = 0; i < 2; ++i) {
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(System.currentTimeMillis());
      event.setEventType("test event type " + i);
      event.addEventInfo("key1", "val1");
      event.addEventInfo("key2", "val2");
      entity.addEvent(event);
    }
    entity.addRelatedEntity("test ref type 1", "test ref id 1");
    entity.addRelatedEntity("test ref type 2", "test ref id 2");
    entity.addPrimaryFilter("pkey1", "pval1");
    entity.addPrimaryFilter("pkey2", "pval2");
    entity.addOtherInfo("okey1", "oval1");
    entity.addOtherInfo("okey2", "oval2");
    entity.setDomainId("domain id 1");
    return entity;
  }

  public static TimelineDomain generateDomain() {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("namesapce id");
    domain.setDescription("domain description");
    domain.setOwner("domain owner");
    domain.setReaders("domain_reader");
    domain.setWriters("domain_writer");
    domain.setCreatedTime(0L);
    domain.setModifiedTime(1L);
    return domain;
  }

  private static TimelineClientImpl createTimelineClient(
      YarnConfiguration conf) {
    TimelineClientImpl client =
        spy((TimelineClientImpl) TimelineClient.createTimelineClient());
    client.init(conf);
    client.start();
    return client;
  }

  private static class TestTimelineDelegationTokenSecretManager extends
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier> {

    public TestTimelineDelegationTokenSecretManager() {
      super(100000, 100000, 100000, 100000);
    }

    @Override
    public TimelineDelegationTokenIdentifier createIdentifier() {
      return new TimelineDelegationTokenIdentifier();
    }

    @Override
    public synchronized byte[] createPassword(TimelineDelegationTokenIdentifier identifier) {
      return super.createPassword(identifier);
    }
  }
}
