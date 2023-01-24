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

package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.*;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

public class FederationRMAdminInterceptor extends AbstractRMAdminRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMAdminInterceptor.class);

  private Map<SubClusterId, ResourceManagerAdministrationProtocol> adminRMProxies;
  private FederationStateStoreFacade federationFacade;
  private final Clock clock = new MonotonicClock();
  private RouterMetrics routerMetrics;
  private ThreadPoolExecutor executorService;
  private Configuration conf;
  private long heartbeatExpirationMillis;

  @Override
  public void init(String userName) {
    super.init(userName);

    int numThreads = getConf().getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router RMAdminClient-" + userName + "-%d ").build();

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        0L, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    federationFacade = FederationStateStoreFacade.getInstance();
    this.conf = this.getConf();
    this.adminRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();

    this.heartbeatExpirationMillis =
        this.conf.getTimeDuration(YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME,
        YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_EXPIRATION_TIME, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  protected ResourceManagerAdministrationProtocol getAdminRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    if (adminRMProxies.containsKey(subClusterId)) {
      return adminRMProxies.get(subClusterId);
    }

    ResourceManagerAdministrationProtocol adminRMProxy = null;
    try {
      boolean serviceAuthEnabled = this.conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation realUser = user;
      if (serviceAuthEnabled) {
        realUser = UserGroupInformation.createProxyUser(
            user.getShortUserName(), UserGroupInformation.getLoginUser());
      }
      adminRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ResourceManagerAdministrationProtocol.class, subClusterId, realUser);
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(e,
          "Unable to create the interface to reach the SubCluster %s", subClusterId);
    }
    adminRMProxies.put(subClusterId, adminRMProxy);
    return adminRMProxy;
  }

  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
       + "FederationRMAdminRequestInterceptor, which should be the last one "
       + "in the chain. Check if the interceptor pipeline configuration "
       + "is correct");
  }

  /**
   * Refresh queue requests.
   *
   * The Router supports refreshing all SubCluster queues at once,
   * and also supports refreshing queues by SubCluster.
   *
   * @param request RefreshQueuesRequest, If subClusterId is not empty,
   * it means that we want to refresh the queue of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all queues.
   *
   * @return RefreshQueuesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {

    // parameter verification.
    if (request == null) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshQueues request.", null);
    }

    // call refreshQueues of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
           new Class[] {RefreshQueuesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshQueuesResponse> refreshQueueResps =
          remoteMethod.invokeConcurrent(this, RefreshQueuesResponse.class, subClusterId);

      // If we get the return result from refreshQueueResps,
      // it means that the call has been successful,
      // and the RefreshQueuesResponse method can be reconstructed and returned.
      if (CollectionUtils.isNotEmpty(refreshQueueResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshQueuesRetrieved(stopTime - startTime);
        return RefreshQueuesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshQueue due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshQueuesFailedRetrieved();
    throw new YarnException("Unable to refreshQueue.");
  }

  /**
   * Refresh node requests.
   *
   * The Router supports refreshing all SubCluster nodes at once,
   * and also supports refreshing node by SubCluster.
   *
   * @param request RefreshNodesRequest, If subClusterId is not empty,
   * it means that we want to refresh the node of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all nodes.
   *
   * @return RefreshNodesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {

    // parameter verification.
    // We will not check whether the DecommissionType is empty,
    // because this parameter has a default value at the proto level.
    if (request == null) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshNodes request.", null);
    }

    // call refreshNodes of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
          new Class[] {RefreshNodesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshNodesResponse> refreshNodesResps =
          remoteMethod.invokeConcurrent(this, RefreshNodesResponse.class, subClusterId);

      if (CollectionUtils.isNotEmpty(refreshNodesResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshNodesRetrieved(stopTime - startTime);
        return RefreshNodesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshNodes due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshNodesFailedRetrieved();
    throw new YarnException("Unable to refreshNodes due to exception.");
  }

  /**
   * Refresh SuperUserGroupsConfiguration requests.
   *
   * The Router supports refreshing all subCluster SuperUserGroupsConfiguration at once,
   * and also supports refreshing SuperUserGroupsConfiguration by SubCluster.
   *
   * @param request RefreshSuperUserGroupsConfigurationRequest,
   * If subClusterId is not empty, it means that we want to
   * refresh the superuser groups configuration of the specified subClusterId.
   * If subClusterId is empty, it means we want to
   * refresh all subCluster superuser groups configuration.
   *
   * @return RefreshSuperUserGroupsConfigurationResponse,
   * There is no specific information in the response, as long as it is not empty,
   * it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {

    // parameter verification.
    if (request == null) {
      routerMetrics.incrRefreshSuperUserGroupsConfigurationFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshSuperUserGroupsConfiguration request.",
          null);
    }

    // call refreshSuperUserGroupsConfiguration of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
          new Class[] {RefreshSuperUserGroupsConfigurationRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshSuperUserGroupsConfigurationResponse> refreshSuperUserGroupsConfResps =
          remoteMethod.invokeConcurrent(this, RefreshSuperUserGroupsConfigurationResponse.class,
          subClusterId);

      if (CollectionUtils.isNotEmpty(refreshSuperUserGroupsConfResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshSuperUserGroupsConfRetrieved(stopTime - startTime);
        return RefreshSuperUserGroupsConfigurationResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshSuperUserGroupsConfigurationFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshSuperUserGroupsConfiguration due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshSuperUserGroupsConfigurationFailedRetrieved();
    throw new YarnException("Unable to refreshSuperUserGroupsConfiguration.");
  }

  /**
   * Refresh UserToGroupsMappings requests.
   *
   * The Router supports refreshing all subCluster UserToGroupsMappings at once,
   * and also supports refreshing UserToGroupsMappings by subCluster.
   *
   * @param request RefreshUserToGroupsMappingsRequest,
   * If subClusterId is not empty, it means that we want to
   * refresh the user groups mapping of the specified subClusterId.
   * If subClusterId is empty, it means we want to
   * refresh all subCluster user groups mapping.
   *
   * @return RefreshUserToGroupsMappingsResponse,
   * There is no specific information in the response, as long as it is not empty,
   * it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request) throws StandbyException, YarnException,
      IOException {

    // parameter verification.
    if (request == null) {
      routerMetrics.incrRefreshUserToGroupsMappingsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshUserToGroupsMappings request.", null);
    }

    // call refreshUserToGroupsMappings of activeSubClusters.
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
          new Class[] {RefreshUserToGroupsMappingsRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshUserToGroupsMappingsResponse> refreshUserToGroupsMappingsResps =
          remoteMethod.invokeConcurrent(this, RefreshUserToGroupsMappingsResponse.class,
          subClusterId);

      if (CollectionUtils.isNotEmpty(refreshUserToGroupsMappingsResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshUserToGroupsMappingsRetrieved(stopTime - startTime);
        return RefreshUserToGroupsMappingsResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshUserToGroupsMappingsFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshUserToGroupsMappings due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshUserToGroupsMappingsFailedRetrieved();
    throw new YarnException("Unable to refreshUserToGroupsMappings.");
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(RefreshAdminAclsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(RefreshServiceAclsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(UpdateNodeResourceRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(RefreshNodesResourcesRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(ReplaceLabelsOnNodeRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public NodesToAttributesMappingResponse mapAttributesToNodes(
      NodesToAttributesMappingRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  /**
   * In YARN Federation mode, We allow users to mark subClusters
   * With no heartbeat for a long time as SC_LOST state.
   *
   * If we include a specific subClusterId in the request, check for the specified subCluster.
   * If subClusterId is empty, all subClusters are checked.
   *
   * @param request deregisterSubCluster request.
   * The request contains the id of to deregister sub-cluster.
   * @return Response from deregisterSubCluster.
   * @throws YarnException exceptions from yarn servers.
   */
  @Override
  public DeregisterSubClusterResponse deregisterSubCluster(DeregisterSubClusterRequest request)
      throws YarnException {

    if (request == null) {
      routerMetrics.incrDeregisterSubClusterFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing DeregisterSubCluster request.", null);
    }

    try {
      long startTime = clock.getTime();
      List<DeregisterSubClusters> deregisterSubClusterList = new ArrayList<>();
      String reqSubClusterId = request.getSubClusterId();
      if (StringUtils.isNotBlank(reqSubClusterId)) {
        // If subCluster is not empty, process the specified subCluster.
        DeregisterSubClusters deregisterSubClusters = deregisterSubCluster(reqSubClusterId);
        deregisterSubClusterList.add(deregisterSubClusters);
      } else {
        // Traversing all Active SubClusters,
        // for subCluster whose heartbeat times out, update the status to SC_LOST.
        Map<SubClusterId, SubClusterInfo> subClusterInfo = federationFacade.getSubClusters(true);
        for (Map.Entry<SubClusterId, SubClusterInfo> entry : subClusterInfo.entrySet()) {
          SubClusterId subClusterId = entry.getKey();
          DeregisterSubClusters deregisterSubClusters = deregisterSubCluster(subClusterId.getId());
          deregisterSubClusterList.add(deregisterSubClusters);
        }
      }
      long stopTime = clock.getTime();
      routerMetrics.succeededDeregisterSubClusterRetrieved(stopTime - startTime);
      return DeregisterSubClusterResponse.newInstance(deregisterSubClusterList);
    } catch (Exception e) {
      routerMetrics.incrDeregisterSubClusterFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to deregisterSubCluster due to exception. " + e.getMessage());
    }

    routerMetrics.incrDeregisterSubClusterFailedRetrieved();
    throw new YarnException("Unable to deregisterSubCluster.");
  }

  /**
   * deregisterSubCluster by SubClusterId.
   *
   * @param reqSubClusterId subClusterId.
   * @throws YarnException indicates exceptions from yarn servers.
   */
  private DeregisterSubClusters deregisterSubCluster(String reqSubClusterId) {

    DeregisterSubClusters deregisterSubClusters = null;

    try {
      // Step1. Get subCluster information.
      SubClusterId subClusterId = SubClusterId.newInstance(reqSubClusterId);
      SubClusterInfo subClusterInfo = federationFacade.getSubCluster(subClusterId);
      SubClusterState subClusterState = subClusterInfo.getState();
      long lastHeartBeat = subClusterInfo.getLastHeartBeat();
      Date lastHeartBeatDate = new Date(lastHeartBeat);

      deregisterSubClusters = DeregisterSubClusters.newInstance(
          reqSubClusterId, "UNKNOWN", lastHeartBeatDate.toString(), "", subClusterState.name());

      // Step2. Deregister subCluster.
      if (subClusterState.isUsable()) {
        LOG.warn("Deregister SubCluster {} in State {} last heartbeat at {}.",
            subClusterId, subClusterState, lastHeartBeatDate);
        // heartbeat interval time.
        long heartBearTimeInterval = Time.now() - lastHeartBeat;
        if (heartBearTimeInterval - heartbeatExpirationMillis < 0) {
          boolean deregisterSubClusterFlag =
              federationFacade.deregisterSubCluster(subClusterId, SubClusterState.SC_LOST);
          if (deregisterSubClusterFlag) {
            deregisterSubClusters.setDeregisterState("SUCCESS");
            deregisterSubClusters.setSubClusterState("SC_LOST");
            deregisterSubClusters.setInformation("Heartbeat Time >= 30 minutes.");
          } else {
            deregisterSubClusters.setDeregisterState("FAILED");
            deregisterSubClusters.setInformation("DeregisterSubClusters Failed.");
          }
        }
      } else {
        deregisterSubClusters.setDeregisterState("FAILED");
        deregisterSubClusters.setInformation("Heartbeat Time < 30 minutes. " +
            "DeregisterSubCluster does not need to be executed");
        LOG.warn("SubCluster {} in State {} does not need to update state.",
            subClusterId, subClusterState);
      }
      return deregisterSubClusters;
    } catch (YarnException e) {
      LOG.error("SubCluster {} DeregisterSubCluster Failed", reqSubClusterId, e);
      deregisterSubClusters = DeregisterSubClusters.newInstance(
          reqSubClusterId, "FAILED", "UNKNOWN", e.getMessage(), "UNKNOWN");
      return deregisterSubClusters;
    }
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return new String[0];
  }

  @VisibleForTesting
  public FederationStateStoreFacade getFederationFacade() {
    return federationFacade;
  }

  @VisibleForTesting
  public ThreadPoolExecutor getExecutorService() {
    return executorService;
  }
}
