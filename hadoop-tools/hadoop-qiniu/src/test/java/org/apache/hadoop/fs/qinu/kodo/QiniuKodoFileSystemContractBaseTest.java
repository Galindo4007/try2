package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Before;

import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemContractBaseTest extends FileSystemContractBaseTest {
    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        conf.setIfUnset("fs.contract.test.fs.qiniu", "qiniu://qshell-hadoop");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.qiniu")), conf);

    }

    @Override
    protected int getGlobalTimeout() {
        return Integer.MAX_VALUE;
    }
}
