// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Resolve the snapshot id at the head of a named Iceberg branch.
///
/// Returns `None` when the branch exists but has never had a snapshot committed
/// to it (unborn branch). Returns an error when the ref does not exist in the
/// table metadata.
pub(crate) fn resolve_branch_head_snapshot_id(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    branch_name: &str,
) -> Result<Option<i64>, String> {
    match metadata.refs().get(branch_name) {
        Some(snap_ref) => Ok(Some(snap_ref.snapshot_id)),
        None => {
            if branch_name == "main" && metadata.current_snapshot().is_none() {
                // Unborn main branch — no snapshot yet; caller should treat as empty.
                Ok(None)
            } else {
                Err(format!(
                    "iceberg ref: branch '{branch_name}' not found in table metadata"
                ))
            }
        }
    }
}
