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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.functional.Tuples;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.BulkDeleteUtils.validateBulkDeletePaths;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Default implementation of the {@link BulkDelete} interface.
 */
public class DefaultBulkDeleteOperation implements BulkDelete {

    private static Logger LOG = LoggerFactory.getLogger(DefaultBulkDeleteOperation.class);

    private static final int DEFAULT_PAGE_SIZE = 1;

    private final Path basePath;

    private final FileSystem fs;

    public DefaultBulkDeleteOperation(Path basePath,
                                      FileSystem fs) {
        this.basePath = requireNonNull(basePath);
        this.fs = fs;
    }

    @Override
    public int pageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    @Override
    public Path basePath() {
        return basePath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map.Entry<Path, String>> bulkDelete(Collection<Path> paths)
            throws IOException, IllegalArgumentException {
        validateBulkDeletePaths(paths, DEFAULT_PAGE_SIZE, basePath);
        List<Map.Entry<Path, String>> result = new ArrayList<>();
        if (!paths.isEmpty()) {
            // As the page size is always 1, this should be the only one
            // path in the collection.
            Path pathToDelete = paths.iterator().next();
            try {
                boolean deleted = fs.delete(pathToDelete, false);
                if (deleted) {
                    return result;
                } else {
                    try {
                        FileStatus fileStatus = fs.getFileStatus(pathToDelete);
                        if (fileStatus.isDirectory()) {
                            result.add(Tuples.pair(pathToDelete, "Path is a directory"));
                        }
                    } catch (FileNotFoundException e) {
                        // Ignore FNFE and don't add to the result list.
                        LOG.debug("Couldn't delete {} - does not exist: {}", pathToDelete, e.toString());
                    } catch (Exception e) {
                        LOG.debug("Couldn't delete {} - exception occurred: {}", pathToDelete, e.toString());
                        result.add(Tuples.pair(pathToDelete, e.toString()));
                    }
                }
            } catch (Exception ex) {
                LOG.debug("Couldn't delete {} - exception occurred: {}", pathToDelete, ex.toString());
                result.add(Tuples.pair(pathToDelete, ex.toString()));
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {

    }
}
