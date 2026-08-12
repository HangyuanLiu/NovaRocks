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

//! Abort-time cleanup access for the Iceberg commit driver.
//!
//! Deleting staged files after a failed write needs an authorized object-store
//! operator plus the path mapping that turns a warehouse-absolute path into an
//! operator-relative key. Both are Iceberg storage facts derived from the
//! catalog entry, so they belong with the commit driver rather than with a SQL
//! write entry point.
//!
//! This used to live in `engine::iceberg_writer`, which meant the SQL
//! application layer constructed an `opendal::Operator`. The provider crate
//! already builds the same thing for its own commit paths
//! (`commit::write_control` and `catalog_control::data_mutation`); this module
//! is the legacy in-Core equivalent and disappears with the rest of
//! `connector/iceberg/**`.

use std::sync::Arc;

use super::CleanupPathMapper;

pub(crate) struct AbortCleanupOperator {
    pub(crate) fs: novarocks_connector_iceberg::opendal::Operator,
    pub(crate) path_mapper: Option<CleanupPathMapper>,
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let access = novarocks_connector_iceberg::fs_io::resolve_access_for_location(
            &entry.warehouse_uri,
            Some(s3_config),
        )
        .map_err(|e| format!("resolve warehouse URI for iceberg abort cleanup: {e}"))?;
        let bucket = access
            .handle()
            .authority()
            .ok_or_else(|| {
                format!(
                    "resolve warehouse URI for iceberg abort cleanup missing bucket: {}",
                    entry.warehouse_uri
                )
            })?
            .to_string();
        let fs = access.operator();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            novarocks_fs::parse_object_store_path_parse_only(path)
                .ok()
                .and_then(|(actual_bucket, key)| {
                    if actual_bucket == bucket {
                        Some(key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| path.to_string())
        });
        return Ok(AbortCleanupOperator {
            fs,
            path_mapper: Some(mapper),
        });
    }

    let fs = novarocks_fs::FsAccessResolver::new()
        .resolve_location("/__novarocks_local_root__", None)
        .map_err(|error| format!("build local-FS operator failed: {error}"))?
        .operator();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}
