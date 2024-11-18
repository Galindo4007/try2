/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_MECHANISM_KEY;

/** Test {@link SaslRpcServer.AuthMethod}. */
public class TestAuthMethod {
  @Test
  public void testConfiguration() throws Exception {
    final Logger log = LoggerFactory.getLogger("org.apache.hadoop.security.SaslMechanismFactory");
    GenericTestUtils.setLogLevel(log, Level.TRACE);

    // init the mechanism
    final String mechanism = "TEST-MECHANISM";
    initConf(mechanism, log);

    // test get from new conf
    final Configuration conf = new Configuration();
    final String confValue = conf.get(HADOOP_SECURITY_SASL_MECHANISM_KEY);
    log.info("getConf: {} = {}", HADOOP_SECURITY_SASL_MECHANISM_KEY, confValue);
    Assert.assertEquals(mechanism, confValue);

    // test AuthMethod
    Assert.assertEquals(mechanism, SaslRpcServer.AuthMethod.TOKEN.getMechanismName());

    // the mechanism won't change after initialized
    final String newMechanism = "NEW-MECHANISM";
    initConf(newMechanism, log);
    Assert.assertEquals(mechanism, SaslRpcServer.AuthMethod.TOKEN.getMechanismName());
  }

  static void initConf(String mechanism, Logger log) throws Exception {
    final Configuration conf = new Configuration();
    conf.set(HADOOP_SECURITY_SASL_MECHANISM_KEY, mechanism);
    log.info("setConf: {} = {}", HADOOP_SECURITY_SASL_MECHANISM_KEY, mechanism);

    final String coreSite = URLDecoder.decode(conf.getResource("core-site.xml").getPath(), "UTF-8");
    try (OutputStream out = Files.newOutputStream(Paths.get(coreSite), StandardOpenOption.CREATE)) {
      conf.writeXml(out);
    }
    Configuration.addDefaultResource(coreSite);
    log.info("writeXml: {}", coreSite);
  }
}