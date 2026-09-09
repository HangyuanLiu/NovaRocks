/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.fixture;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

final class HookedTableOperations implements TableOperations {
  private final TableOperations delegate;
  private final String table;

  HookedTableOperations(TableOperations delegate, String table) {
    this.delegate = delegate;
    this.table = table;
  }

  @Override
  public TableMetadata current() {
    return delegate.current();
  }

  @Override
  public TableMetadata refresh() {
    TableMetadata metadata = delegate.refresh();
    FaultControlServer.refreshed(table, metadata);
    return metadata;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata updated) {
    FaultControlServer.CommitAttempt attempt =
        FaultControlServer.beforePersistentCommit(table, base, updated);
    try {
      delegate.commit(base, updated);
      FaultControlServer.commitSucceeded(table, attempt);
    } catch (RuntimeException failure) {
      FaultControlServer.commitFailed(table, attempt, failure);
      throw failure;
    }
  }

  @Override
  public FileIO io() {
    return delegate.io();
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return delegate.metadataFileLocation(fileName);
  }

  @Override
  public LocationProvider locationProvider() {
    return delegate.locationProvider();
  }
}
