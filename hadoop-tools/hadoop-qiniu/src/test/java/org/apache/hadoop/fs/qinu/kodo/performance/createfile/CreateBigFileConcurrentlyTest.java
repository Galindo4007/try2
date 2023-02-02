package org.apache.hadoop.fs.qinu.kodo.performance.createfile;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qinu.kodo.performance.QiniuKodoPerformanceBaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CreateBigFileConcurrentlyTest extends QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateBigFileConcurrentlyTest.class);
    private static final BlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);

    private long createBigFile(String workDir, FileSystem fs, int files, int blockSize, int blocks, int consumers) throws Exception {
        final String dir = workDir + "/testCreateBigFileConcurrently/";
        byte[] bs = new byte[blockSize];
        for (int i = 0; i < consumers; i++) {
            service.submit(() -> {
                while (true) {
                    try {
                        Integer e = queue.poll(2, TimeUnit.SECONDS);
                        if (e == null) {
                            // 如果超过1s收不到数据，那就退出线程
                            break;
                        }

                        FSDataOutputStream fos = fs.create(new Path(dir + "/" + e));
                        for (int j = 0; j < blocks; j++) {
                            fos.write(bs);
                        }
                        fos.close();

                    } catch (InterruptedException | IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        }
        // 建立父目录
        fs.mkdirs(new Path(dir));

        // 生产
        long ms = System.currentTimeMillis();

        for (int i = 0; i < files; i++) {
            boolean success;
            do {
                success = queue.offer(i, 1, TimeUnit.SECONDS);
            } while (!success);
        }

        return System.currentTimeMillis() - ms;
    }

    @Override
    protected long testImpl(String testDir, FileSystem fs) throws Exception {
        return createBigFile(testDir, fs, 100, 4 * 1024 * 1024, 2, 8);
    }
}
