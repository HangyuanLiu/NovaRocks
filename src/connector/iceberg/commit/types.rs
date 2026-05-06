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

use std::collections::HashMap;

use iceberg::spec::{DataContentType, DataFileFormat, Struct};
use serde::{Deserialize, Serialize};

/// Selects which Iceberg commit action to run for a given write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
    /// Iceberg v3 row-lineage DELETE: writes Puffin deletion-vector files and
    /// rewrites touched delete manifests instead of producing v2 Parquet
    /// position-delete files.
    RowDeltaDv,
    /// Iceberg OPTIMIZE whole-table rewrite: replaces all current live data
    /// files with compacted data files and drops all current delete files.
    RewriteDataFiles,
    /// Iceberg v3 row-lineage UPDATE in copy-on-write mode: rewrites touched
    /// data files while preserving `_row_id`.
    CowUpdate,
    /// Iceberg `TRUNCATE TABLE`: writes a single `operation=delete` snapshot
    /// that marks every live data / DV / position-delete / equality-delete
    /// file as DELETED while preserving schema, partition spec, properties,
    /// and other refs.
    Truncate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergWriteMode {
    LegacyPositionDeletes,
    RowLineageV3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergSqlDeleteStrategy {
    PositionDeleteFiles,
    DeletionVectors,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergUpdateMode {
    CopyOnWrite,
    MergeOnRead,
}

impl IcebergUpdateMode {
    pub fn as_property_value(self) -> &'static str {
        match self {
            IcebergUpdateMode::CopyOnWrite => NOVAROCKS_UPDATE_MODE_COW,
            IcebergUpdateMode::MergeOnRead => NOVAROCKS_UPDATE_MODE_MOR,
        }
    }

    pub fn from_property_value(value: &str) -> Option<Self> {
        match value {
            NOVAROCKS_UPDATE_MODE_COW => Some(IcebergUpdateMode::CopyOnWrite),
            NOVAROCKS_UPDATE_MODE_MOR => Some(IcebergUpdateMode::MergeOnRead),
            _ => None,
        }
    }
}

pub const NOVAROCKS_ROW_LEVEL_OP: &str = "novarocks.row-level-op";
pub const NOVAROCKS_ROW_LEVEL_OP_UPDATE: &str = "update";
pub const NOVAROCKS_UPDATE_MODE: &str = "novarocks.update.mode";
pub const NOVAROCKS_UPDATE_MODE_COW: &str = "copy-on-write";
pub const NOVAROCKS_UPDATE_MODE_MOR: &str = "merge-on-read";
pub const NOVAROCKS_UPDATE_SIDECAR: &str = "novarocks.update.sidecar";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationSidecar {
    pub version: u32,
    pub operation: String,
    pub mode: String,
    pub base_snapshot_id: i64,
    pub target_table_uuid: String,
    pub updated_row_ids: Vec<i64>,
    pub touched_data_files: Vec<MutationSidecarFile>,
}

impl MutationSidecar {
    pub fn update(
        mode: IcebergUpdateMode,
        base_snapshot_id: i64,
        target_table_uuid: String,
        updated_row_ids: Vec<i64>,
        touched_data_files: Vec<MutationSidecarFile>,
    ) -> Self {
        Self {
            version: 1,
            operation: NOVAROCKS_ROW_LEVEL_OP_UPDATE.to_string(),
            mode: mode.as_property_value().to_string(),
            base_snapshot_id,
            target_table_uuid,
            updated_row_ids,
            touched_data_files,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationSidecarFile {
    pub old_file: String,
    pub new_files: Vec<String>,
    pub row_ids: Vec<i64>,
}

/// Metadata about a single Parquet file produced by `IcebergSink` during a
/// pipeline run. Mirrors the subset of `TIcebergDataFile` we need for commit
/// and abort flows. Constructed from `TSinkCommitInfo` after pipeline finish.
#[derive(Clone, Debug)]
pub struct WrittenFile {
    pub path: String,
    pub format: DataFileFormat,
    pub content: DataContentType,
    pub partition_values: Struct,
    pub partition_spec_id: i32,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub split_offsets: Vec<i64>,
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub key_metadata: Option<Vec<u8>>,
    /// Set only for content == PositionDeletes.
    pub referenced_data_file: Option<String>,
    /// Set only for content == EqualityDeletes.
    pub equality_ids: Option<Vec<i32>>,
    /// For Iceberg v3 row-lineage data files whose rows reuse pre-existing
    /// `_row_id`s (e.g. MOR UPDATE replacement files): the lineage `first_row_id`
    /// to record on the manifest entry. When set, readers prefer this over the
    /// manifest-level first_row_id and `df.first_row_id()` propagates without
    /// triggering fresh row-id allocation. `None` for normal INSERT writes.
    pub first_row_id: Option<i64>,
}

/// Result returned by a successful commit action.
#[derive(Clone, Debug)]
pub struct CommitOutcome {
    pub new_snapshot_id: i64,
    /// Manifest / manifest-list paths written by the commit-action; consumed by
    /// abort cleanup on failure.
    pub written_manifest_paths: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn written_file_can_be_constructed() {
        let f = WrittenFile {
            path: "s3://x/data/abc.parquet".to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 100,
            file_size_in_bytes: 4096,
            split_offsets: vec![4],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        };
        assert_eq!(f.record_count, 100);
        assert_eq!(f.content, DataContentType::Data);
    }

    #[test]
    fn op_kind_variants_are_distinct() {
        let variants = [
            CommitOpKind::FastAppend,
            CommitOpKind::Overwrite,
            CommitOpKind::RowDelta,
            CommitOpKind::RowDeltaDv,
            CommitOpKind::RewriteDataFiles,
            CommitOpKind::CowUpdate,
            CommitOpKind::Truncate,
        ];
        for (idx, left) in variants.iter().enumerate() {
            for right in variants.iter().skip(idx + 1) {
                assert_ne!(left, right);
            }
        }
    }

    #[test]
    fn mutation_sidecar_update_uses_typed_markers() {
        let sidecar = MutationSidecar::update(
            IcebergUpdateMode::MergeOnRead,
            42,
            "table-uuid".to_string(),
            vec![7, 9],
            vec![MutationSidecarFile {
                old_file: "old.parquet".to_string(),
                new_files: vec!["new.parquet".to_string()],
                row_ids: vec![7],
            }],
        );

        assert_eq!(NOVAROCKS_ROW_LEVEL_OP, "novarocks.row-level-op");
        assert_eq!(NOVAROCKS_UPDATE_MODE, "novarocks.update.mode");
        assert_eq!(NOVAROCKS_UPDATE_SIDECAR, "novarocks.update.sidecar");
        assert_eq!(sidecar.operation, NOVAROCKS_ROW_LEVEL_OP_UPDATE);
        assert_eq!(sidecar.mode, NOVAROCKS_UPDATE_MODE_MOR);
        assert_eq!(
            IcebergUpdateMode::from_property_value(&sidecar.mode),
            Some(IcebergUpdateMode::MergeOnRead)
        );

        let json = serde_json::to_string(&sidecar).expect("serialize sidecar");
        let decoded: MutationSidecar = serde_json::from_str(&json).expect("deserialize sidecar");
        assert_eq!(decoded, sidecar);
    }

    #[test]
    fn mutation_sidecar_round_trips_json() {
        let sidecar = MutationSidecar::update(
            IcebergUpdateMode::CopyOnWrite,
            101,
            "table-uuid".to_string(),
            vec![11, 12, 13],
            vec![MutationSidecarFile {
                old_file: "s3://bucket/table/data/old.parquet".to_string(),
                new_files: vec![
                    "s3://bucket/table/data/new-1.parquet".to_string(),
                    "s3://bucket/table/data/new-2.parquet".to_string(),
                ],
                row_ids: vec![11, 12, 13],
            }],
        );

        let json = serde_json::to_string(&sidecar).expect("serialize sidecar");
        let decoded: MutationSidecar = serde_json::from_str(&json).expect("deserialize sidecar");

        assert_eq!(decoded, sidecar);
    }
}
