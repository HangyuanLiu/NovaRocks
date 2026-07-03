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
use crate::cache::ExternalDataCacheRangeOptions;
use crate::connector::iceberg::delete_file::IcebergDeleteFileSpec;
use crate::fs::access::{FsAccessResolver, FsScheme};
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::novarocks_logging::debug;
use crate::runtime::profile::RuntimeProfile;

#[derive(Clone, Debug)]
pub struct FileScanRange {
    pub path: String,
    pub file_len: u64,
    pub offset: u64,
    pub length: u64,
    pub scan_range_id: i32,
    pub first_row_id: Option<i64>,
    /// Iceberg V3 data sequence number of the manifest entry this range belongs
    /// to. Used to synthesize `_last_updated_sequence_number` per-row.
    /// None for non-row-lineage scans.
    pub data_sequence_number: Option<i64>,
    pub ivm_change_op: Option<i8>,
    /// Optional absolute row positions to include from this data-file range.
    /// Positions use the same `_pos` coordinate as Iceberg position deletes.
    pub included_positions: Option<Vec<i64>>,
    pub external_datacache: Option<ExternalDataCacheRangeOptions>,
    /// Iceberg delete files attached to this data-file range. Empty for v1 or
    /// append-only scans. Populated by HDFS scan lowering and standalone
    /// write-side visibility planning.
    pub delete_files: Vec<IcebergDeleteFileSpec>,
    /// Per-file Iceberg statistics carried across the thrift/HDFS range
    /// boundary for later file-level pruning. None for non-Iceberg scans or
    /// files whose manifest stats are unavailable/unsupported.
    pub iceberg_file_pruning:
        Option<crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata>,
}

#[derive(Clone)]
pub struct FileScanContext {
    pub ranges: Vec<FileScanRange>,
    pub factory: OpendalRangeReaderFactory,
    pub scheme: FsScheme,
    pub root: Option<String>,
}

impl FileScanContext {
    /// Build a scan context for the given ranges.
    ///
    /// `oss_config` must be `Some` when the paths use the `oss://` / `s3://` scheme; it is
    /// unused for local and HDFS paths.  Callers are responsible for resolving the config from
    /// whatever source is appropriate (e.g. `THdfsScanNode.cloud_configuration` for Iceberg
    /// external tables, or the shard registry for native lake tablets).
    pub fn build(
        ranges: Vec<FileScanRange>,
        profile: Option<RuntimeProfile>,
        oss_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    ) -> Result<Self, String> {
        let paths = ranges.iter().map(|r| r.path.as_str()).collect::<Vec<_>>();
        let handle = FsAccessResolver::new().resolve_locations(paths, oss_config)?;
        let factory = handle.reader_factory()?.with_profile(profile);
        let resolved_paths = handle
            .paths()
            .iter()
            .map(|path| path.operator_relative_path().to_string())
            .collect::<Vec<_>>();

        let ranges = ranges
            .into_iter()
            .zip(resolved_paths)
            .map(|(range, path)| FileScanRange { path, ..range })
            .collect::<Vec<_>>();

        match handle.scheme() {
            FsScheme::Local => {
                let root = handle.root().unwrap_or(".");
                debug!("file scan (local): {} ranges root={}", ranges.len(), root);
            }
            FsScheme::ObjectStore => {
                debug!("file scan (object-store): {} ranges", ranges.len());
            }
            FsScheme::Hdfs => {
                let root = handle.root().unwrap_or("<unknown>");
                debug!(
                    "file scan (hdfs): {} ranges namenode={}",
                    ranges.len(),
                    root
                );
            }
        }

        Ok(Self {
            ranges,
            factory,
            scheme: handle.scheme(),
            root: handle.root().map(str::to_string),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::connector::iceberg::file_pruning::IcebergFilePruningMetadata;
    use crate::fs::access::FsScheme;
    use crate::sql::catalog::IcebergColumnStats;

    fn range(path: &str) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1,
            offset: 0,
            length: 1,
            scan_range_id: 0,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }
    }

    fn metadata() -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(10_i64.to_le_bytes().to_vec()),
                    upper_bound: Some(20_i64.to_le_bytes().to_vec()),
                },
            )]),
        }
    }

    #[test]
    fn build_local_scan_context_uses_resolver_relative_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("a.parquet");
        std::fs::write(&file, b"data").expect("write fixture");

        let ctx = FileScanContext::build(vec![range(file.to_string_lossy().as_ref())], None, None)
            .expect("build scan context");

        assert_eq!(ctx.ranges.len(), 1);
        assert_eq!(ctx.ranges[0].path, "a.parquet");
        assert_eq!(ctx.scheme, FsScheme::Local);
    }

    #[test]
    fn build_local_scan_context_preserves_iceberg_file_pruning_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("a.parquet");
        std::fs::write(&file, b"data").expect("write fixture");
        let mut range = range(file.to_string_lossy().as_ref());
        range.iceberg_file_pruning = Some(metadata());

        let ctx = FileScanContext::build(vec![range], None, None).expect("build scan context");

        let pruning = ctx.ranges[0]
            .iceberg_file_pruning
            .as_ref()
            .expect("iceberg metadata");
        assert_eq!(
            pruning.columns["id"].lower_bound,
            Some(10_i64.to_le_bytes().to_vec())
        );
    }

    #[test]
    fn build_object_store_context_requires_credentials_only_config() {
        let err = match FileScanContext::build(
            vec![range("s3://bucket-a/warehouse/t/a.parquet")],
            None,
            None,
        ) {
            Ok(_) => panic!("object-store scan requires credentials"),
            Err(err) => err,
        };

        assert!(
            err.contains("object-store location requires object store config"),
            "{err}"
        );
    }
}
