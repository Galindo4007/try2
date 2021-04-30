/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;


/**
 * This is the committer factory to register as the source of committers
 * for the job/filesystem schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ManifestCommitterFactory extends PathOutputCommitterFactory {

  public static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitterFactory.class);

  /**
   * Name of this factory.
   */
  public static final String NAME
      = "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory";

  @Override
  public ManifestCommitter createOutputCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    return new ManifestCommitter(outputPath, context);
  }

}
