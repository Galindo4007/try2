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
package org.apache.hadoop.hdfs;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_SUSPECT_NODE_QUEUE_MAX_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for dead node detection in DFSClient.
 */
public class TestDeadNodeDetection {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private ErasureCodingPolicy ecPolicy;
  private int dataBlocks;
  private int parityBlocks;

  @Before
  public void setUp() {
    cluster = null;
    ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
    dataBlocks = ecPolicy.getNumDataUnits();
    parityBlocks = ecPolicy.getNumParityUnits();

    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY, true);
    conf.setLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_DEAD_NODE_INTERVAL_MS_KEY,
        1000);
    conf.setLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_SUSPECT_NODE_INTERVAL_MS_KEY,
        100);
    conf.setLong(
        DFS_CLIENT_DEAD_NODE_DETECTION_PROBE_CONNECTION_TIMEOUT_MS_KEY,
        1000);
    conf.setInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 0);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDeadNodeDetectionInBackground() throws Exception {
    conf.set(DFS_CLIENT_CONTEXT, "testDeadNodeDetectionInBackground");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDetectDeadNodeInBackground");

    // 256 bytes data chunk for writes
    byte[] bytes = new byte[256];
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = '0';
    }

    // File with a 512 bytes block size
    FSDataOutputStream out = fs.create(filePath, true, 4096, (short) 3, 512);

    // Write a block to all 3 DNs (2x256bytes).
    out.write(bytes);
    out.write(bytes);
    out.hflush();
    out.close();

    // Remove three DNs,
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }

      DefaultCoordination defaultCoordination = new DefaultCoordination();
      defaultCoordination.startWaitForDeadNodeThread(dfsClient, 3);
      defaultCoordination.sync();

      assertEquals(3, dfsClient.getDeadNodes(din).size());
      assertEquals(3, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    } finally {
      in.close();
      fs.delete(filePath, true);
      // check the dead node again here, the dead node is expected be removed
      assertEquals(0, dfsClient.getDeadNodes(din).size());
      assertEquals(0, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    }
  }

  @Test
  public void testDeadNodeDetectionInMultipleDFSInputStream()
      throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodeMultipleDFSInputStream");
    createFile(fs, filePath);

    String datanodeUuid = cluster.getDataNodes().get(0).getDatanodeUuid();
    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din1 = (DFSInputStream) in1.getWrappedStream();
    DFSClient dfsClient1 = din1.getDFSClient();
    cluster.stopDataNode(0);

    FSDataInputStream in2 = fs.open(filePath);
    DFSInputStream din2 = null;
    DFSClient dfsClient2 = null;
    try {
      try {
        in1.read();
      } catch (BlockMissingException e) {
      }

      din2 = (DFSInputStream) in2.getWrappedStream();
      dfsClient2 = din2.getDFSClient();

      DefaultCoordination defaultCoordination = new DefaultCoordination();
      defaultCoordination.startWaitForDeadNodeThread(dfsClient2, 1);
      defaultCoordination.sync();
      assertEquals(dfsClient1.toString(), dfsClient2.toString());
      assertEquals(1, dfsClient1.getDeadNodes(din1).size());
      assertEquals(1, dfsClient2.getDeadNodes(din2).size());
      assertEquals(1, dfsClient1.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      assertEquals(1, dfsClient2.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      // check the dn uuid of dead node to see if its expected dead node
      assertEquals(datanodeUuid,
          ((DatanodeInfo) dfsClient1.getClientContext().getDeadNodeDetector()
              .clearAndGetDetectedDeadNodes().toArray()[0]).getDatanodeUuid());
      assertEquals(datanodeUuid,
          ((DatanodeInfo) dfsClient2.getClientContext().getDeadNodeDetector()
              .clearAndGetDetectedDeadNodes().toArray()[0]).getDatanodeUuid());
    } finally {
      in1.close();
      in2.close();
      deleteFile(fs, filePath);
      // check the dead node again here, the dead node is expected be removed
      assertEquals(0, dfsClient1.getDeadNodes(din1).size());
      assertEquals(0, dfsClient2.getDeadNodes(din2).size());
      assertEquals(0, dfsClient1.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      assertEquals(0, dfsClient2.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    }
  }

  @Test
  public void testDeadNodeDetectionDeadNodeRecovery() throws Exception {
    // prevent interrupt deadNodeDetectorThr in cluster.waitActive()
    DFSClient.setDisabledStopDeadNodeDetectorThreadForTest(true);
    conf.set(DFS_CLIENT_CONTEXT, "testDeadNodeDetectionDeadNodeRecovery");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    DFSClient.setDisabledStopDeadNodeDetectorThreadForTest(false);
    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodeDetectionDeadNodeRecovery");
    createFile(fs, filePath);

    // Remove three DNs,
    MiniDFSCluster.DataNodeProperties one = cluster.stopDataNode(0);
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }
      DefaultCoordination defaultCoordination = new DefaultCoordination();
      defaultCoordination.startWaitForDeadNodeThread(dfsClient, 3);
      defaultCoordination.sync();
      assertEquals(3, dfsClient.getDeadNodes(din).size());
      assertEquals(3, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());

      cluster.restartDataNode(one, true);

      defaultCoordination = new DefaultCoordination();
      defaultCoordination.startWaitForDeadNodeThread(dfsClient, 2);
      defaultCoordination.sync();
      assertEquals(2, dfsClient.getDeadNodes(din).size());
      assertEquals(2, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    } finally {
      in.close();
      deleteFile(fs, filePath);
      assertEquals(0, dfsClient.getDeadNodes(din).size());
      assertEquals(0, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
    }
  }

  @Test
  public void testDeadNodeDetectionMaxDeadNodesProbeQueue() throws Exception {
    conf.setInt(DFS_CLIENT_DEAD_NODE_DETECTION_DEAD_NODE_QUEUE_MAX_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodeDetectionMaxDeadNodesProbeQueue");
    createFile(fs, filePath);

    // Remove three DNs,
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);
    cluster.stopDataNode(0);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }

      Thread.sleep(1500);
      Assert.assertTrue((dfsClient.getClientContext().getDeadNodeDetector()
          .getDeadNodesProbeQueue().size()
          + dfsClient.getDeadNodes(din).size()) <= 4);
    } finally {
      in.close();
      deleteFile(fs, filePath);
    }
  }

  @Test
  public void testDeadNodeDetectionSuspectNode() throws Exception {
    conf.setInt(DFS_CLIENT_DEAD_NODE_DETECTION_SUSPECT_NODE_QUEUE_MAX_KEY, 1);
    DeadNodeDetector.setDisabledProbeThreadForTest(true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodeDetectionSuspectNode");
    createFile(fs, filePath);

    MiniDFSCluster.DataNodeProperties one = cluster.stopDataNode(0);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();
    DeadNodeDetector deadNodeDetector =
        dfsClient.getClientContext().getDeadNodeDetector();
    try {
      try {
        in.read();
      } catch (BlockMissingException e) {
      }
      waitForSuspectNode(din.getDFSClient());
      cluster.restartDataNode(one, true);
      Assert.assertEquals(1,
          deadNodeDetector.getSuspectNodesProbeQueue().size());
      Assert.assertEquals(0,
          deadNodeDetector.clearAndGetDetectedDeadNodes().size());
      deadNodeDetector.startProbeScheduler();
      Thread.sleep(1000);
      Assert.assertEquals(0,
          deadNodeDetector.getSuspectNodesProbeQueue().size());
      Assert.assertEquals(0,
          deadNodeDetector.clearAndGetDetectedDeadNodes().size());
    } finally {
      in.close();
      deleteFile(fs, filePath);
      assertEquals(0, dfsClient.getDeadNodes(din).size());
      assertEquals(0, dfsClient.getClientContext().getDeadNodeDetector()
          .clearAndGetDetectedDeadNodes().size());
      // reset disabledProbeThreadForTest
      DeadNodeDetector.setDisabledProbeThreadForTest(false);
    }
  }

  /* TODO: Delete when done
   * 1. Use DFSStripedInputStream to read ec block
   * 2. Let datanode dead/suspend to make read timed out
   * 3. Assert deadnode detector could catch the dn & clear it after turns OK
   * */
  @Test
  public void testDeadNodeDetectionUnderStripedStream() throws Exception {
    conf.setInt(DFS_CLIENT_DEAD_NODE_DETECTION_SUSPECT_NODE_QUEUE_MAX_KEY,
            parityBlocks + 1);
    DeadNodeDetector.setDisabledProbeThreadForTest(true);
    // EC need 9(6+3) DNs? or just one is OK?
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        dataBlocks + parityBlocks).build();
    cluster.waitActive();

    Path filePath = new Path("/testDeadNodeDetection4ECFile");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    DFSTestUtil.createStripedFile(cluster, filePath, null, 3, 2, false,
            ecPolicy);
    DFSInputStream in = fs.getClient().open(filePath.toString());
    assertTrue(in instanceof DFSStripedInputStream);

    for (int i = 0; i <= parityBlocks; i++) {
      cluster.stopDataNode(i);
    }
    DFSClient dfsClient = in.getDFSClient();
    DeadNodeDetector deadNodeDetector =
            dfsClient.getClientContext().getDeadNodeDetector();
    try {
      try {
        in.read();
      } catch (IOException e) {
        // Throw an exception when the missing block > parityBlocks
      }

      waitForSuspectNode(in.getDFSClient());
      for (int i = 0; i <= parityBlocks; i++) {
        cluster.restartDataNode(i, true);
      }
      assertEquals(parityBlocks + 1,
              deadNodeDetector.getSuspectNodesProbeQueue().size());
      assertEquals(0, deadNodeDetector.clearAndGetDetectedDeadNodes().size());
      deadNodeDetector.startProbeScheduler();
      Thread.sleep(1000 * parityBlocks);
      assertEquals(0, deadNodeDetector.getSuspectNodesProbeQueue().size());
      assertEquals(0, deadNodeDetector.clearAndGetDetectedDeadNodes().size());
    } finally {
      in.close();
      assertEquals(0, dfsClient.getDeadNodes(in).size());
      assertEquals(0, dfsClient.getClientContext().
              getDeadNodeDetector().clearAndGetDetectedDeadNodes().size());
      deleteFile(fs, filePath);
      // reset disabledProbeThreadForTest
      DeadNodeDetector.setDisabledProbeThreadForTest(false);
    }
  }

  private void createFile(FileSystem fs, Path filePath) throws IOException {
    FSDataOutputStream out = null;
    try {
      // 256 bytes data chunk for writes
      byte[] bytes = new byte[256];
      for (int index = 0; index < bytes.length; index++) {
        bytes[index] = '0';
      }

      // File with a 512 bytes block size
      out = fs.create(filePath, true, 4096, (short) 3, 512);

      // Write a block to all 3 DNs (2x256bytes).
      out.write(bytes);
      out.write(bytes);
      out.hflush();

    } finally {
      out.close();
    }
  }

  private void deleteFile(FileSystem fs, Path filePath) throws IOException {
    fs.delete(filePath, true);
  }

  private void waitForSuspectNode(DFSClient dfsClient) throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          if (dfsClient.getClientContext().getDeadNodeDetector()
              .getSuspectNodesProbeQueue().size() > 0) {
            return true;
          }
        } catch (Exception e) {
          // Ignore the exception
        }

        return false;
      }
    }, 500, 5000);
  }

  class DefaultCoordination {
    private Queue<Object> queue = new LinkedBlockingQueue<Object>(1);

    public boolean addToQueue() {
      return queue.offer(new Object());
    }

    public Object removeFromQueue() {
      return queue.poll();
    }

    public void sync() {
      while (removeFromQueue() == null) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    private void startWaitForDeadNodeThread(DFSClient dfsClient, int size) {
      new Thread(() -> {
        DeadNodeDetector deadNodeDetector =
            dfsClient.getClientContext().getDeadNodeDetector();
        while (deadNodeDetector.clearAndGetDetectedDeadNodes().size() != size) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        }
        addToQueue();
      }).start();
    }
  }
}
