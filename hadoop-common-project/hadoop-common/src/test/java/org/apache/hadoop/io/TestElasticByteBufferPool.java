package org.apache.hadoop.io;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TestElasticByteBufferPool {
  @Test
  public void testGarbageCollection() throws Exception {
    final ElasticByteBufferPool ebbp = new WeakReferencedElasticByteBufferPool();
    ByteBuffer buffer1 = ebbp.getBuffer(true, 5);
    ByteBuffer buffer2 = ebbp.getBuffer(true, 10);
    ByteBuffer buffer3 = ebbp.getBuffer(true, 15);

    // the pool is empty yet
    Assertions.assertThat(ebbp.getCurrentBuffersCount(true)).isEqualTo(0);

    // put the buffers back to the pool
    ebbp.putBuffer(buffer2);
    ebbp.putBuffer(buffer3);
    Assertions.assertThat(ebbp.getCurrentBuffersCount(true)).isEqualTo(2);

    // release the references to be garbage-collected
    buffer2 = null;
    buffer3 = null;
    System.gc();

    // call getBuffer() to trigger the cleanup
    ByteBuffer buffer4 = ebbp.getBuffer(true, 10);

    // the pool should be empty. buffer2, buffer3 should be garbage-collected
    Assertions.assertThat(ebbp.getCurrentBuffersCount(true)).isEqualTo(0);
  }
}
