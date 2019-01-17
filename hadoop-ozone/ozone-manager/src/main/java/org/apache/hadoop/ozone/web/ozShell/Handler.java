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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.ozShell;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Common interface for command handling.
 */
@Command(mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public abstract class Handler implements Callable<Void> {

  protected static final Logger LOG = LoggerFactory.getLogger(Handler.class);

  @ParentCommand
  private GenericParentCommand parent;

  @Override
  public Void call() throws Exception {
    throw new UnsupportedOperationException();
  }

  public boolean isVerbose() {
    return parent.isVerbose();
  }

  public OzoneConfiguration createOzoneConfiguration() {
    return parent.createOzoneConfiguration();
  }

}
