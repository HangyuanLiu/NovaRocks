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

//! Protocol-neutral lake storage facts.
//!
//! The execution kernel mutates these values without depending on StarRocks
//! protobuf structs.  Compat owns the protobuf codec at the file/RPC boundary.

use std::collections::HashMap;

use crate::connector::starrocks::schema::{StarRocksTabletSchema, StarRocksTypeDesc};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageFile {
    pub name: Option<String>,
    pub size: Option<i64>,
    pub shared: Option<bool>,
    pub encryption_meta: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDeleteFile {
    pub name: Option<String>,
    pub origin_rowset_id: Option<u32>,
    pub op_offset: Option<u32>,
    pub encryption_meta: Option<Vec<u8>>,
    pub shared: Option<bool>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageSegment {
    pub sort_key_min: Option<StorageTuple>,
    pub sort_key_max: Option<StorageTuple>,
    pub num_rows: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageTuple {
    pub values: Vec<StorageVariant>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageVariant {
    pub type_desc: Option<StarRocksTypeDesc>,
    pub value: Option<String>,
    pub kind: Option<StorageVariantKind>,
}

/// Semantic value kind for a segment sort-key value.  Compat maps this value
/// to StarRocks' protobuf enum at the storage-file boundary; the execution
/// kernel must not carry that generated enum or its numeric discriminants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageVariantKind {
    Null,
    Normal,
    Unknown(i32),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageInPredicate {
    pub column_name: Option<String>,
    pub is_not_in: Option<bool>,
    pub values: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageBinaryPredicate {
    pub column_name: Option<String>,
    pub op: Option<String>,
    pub value: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageIsNullPredicate {
    pub column_name: Option<String>,
    pub is_not_null: Option<bool>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDeletePredicate {
    pub version: i32,
    pub sub_predicates: Vec<String>,
    pub in_predicates: Vec<StorageInPredicate>,
    pub binary_predicates: Vec<StorageBinaryPredicate>,
    pub is_null_predicates: Vec<StorageIsNullPredicate>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDelvecPage {
    pub version: Option<i64>,
    pub offset: Option<u64>,
    pub size: Option<u64>,
    pub crc32c: Option<u32>,
    pub crc32c_gen_version: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDelvecMetadata {
    pub version_to_file: HashMap<i64, StorageFile>,
    pub delvecs: HashMap<u32, StorageDelvecPage>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageColumnHashCongruence {
    pub modulus: Option<i64>,
    pub remainder: Option<i64>,
    pub column_names: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageRecordPredicate {
    pub kind: Option<i32>,
    pub children: Vec<StorageRecordPredicate>,
    pub column_hash_is_congruent: Option<StorageColumnHashCongruence>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoragePersistentIndexSstablePredicate {
    pub record_predicate: Option<StorageRecordPredicate>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoragePersistentIndexSstable {
    pub version: Option<i64>,
    pub filename: Option<String>,
    pub filesize: Option<i64>,
    pub max_rss_rowid: Option<u64>,
    pub encryption_meta: Option<Vec<u8>>,
    pub shared: Option<bool>,
    pub predicate: Option<StoragePersistentIndexSstablePredicate>,
    pub shared_rssid: Option<u32>,
    pub shared_version: Option<i64>,
    pub delvec: Option<StorageDelvecPage>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageFlatJsonConfig {
    pub enabled: Option<bool>,
    pub null_factor: Option<f64>,
    pub sparsity_factor: Option<f64>,
    pub max_column_max: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoragePersistentIndexSstableMeta {
    pub sstables: Vec<StoragePersistentIndexSstable>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDeltaColumnGroupVersion {
    pub unique_column_ids: Vec<Vec<u32>>,
    pub column_files: Vec<String>,
    pub versions: Vec<i64>,
    pub encryption_metas: Vec<Vec<u8>>,
    pub shared_files: Vec<bool>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageDeltaColumnGroupMetadata {
    pub groups: HashMap<u32, StorageDeltaColumnGroupVersion>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageRowset {
    pub id: Option<u32>,
    pub overlapped: Option<bool>,
    pub segments: Vec<String>,
    pub num_rows: Option<i64>,
    pub data_size: Option<i64>,
    pub delete_predicate: Option<StorageDeletePredicate>,
    pub num_dels: Option<i64>,
    pub segment_size: Vec<u64>,
    pub max_compact_input_rowset_id: Option<u32>,
    pub version: Option<i64>,
    pub del_files: Vec<StorageDeleteFile>,
    pub segment_encryption_metas: Vec<Vec<u8>>,
    pub next_compaction_offset: Option<u32>,
    pub bundle_file_offsets: Vec<i64>,
    pub shared_segments: Vec<bool>,
    pub record_predicate: Option<StorageRecordPredicate>,
    pub segment_metas: Vec<StorageSegment>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageTabletMetadata {
    pub id: Option<i64>,
    pub version: Option<i64>,
    pub schema: Option<StarRocksTabletSchema>,
    pub rowsets: Vec<StorageRowset>,
    pub next_rowset_id: Option<u32>,
    pub cumulative_point: Option<u32>,
    pub delvec_meta: Option<StorageDelvecMetadata>,
    pub compaction_inputs: Vec<StorageRowset>,
    pub prev_garbage_version: Option<i64>,
    pub orphan_files: Vec<StorageFile>,
    pub enable_persistent_index: Option<bool>,
    pub persistent_index_type: Option<i32>,
    pub commit_time: Option<i64>,
    pub source_schema: Option<StarRocksTabletSchema>,
    pub sstable_meta: Option<StoragePersistentIndexSstableMeta>,
    pub dcg_meta: Option<StorageDeltaColumnGroupMetadata>,
    pub historical_schemas: HashMap<i64, StarRocksTabletSchema>,
    pub rowset_to_schema: HashMap<u32, i64>,
    pub gtid: Option<i64>,
    pub compaction_strategy: Option<i32>,
    pub flat_json_config: Option<StorageFlatJsonConfig>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StoragePagePointer {
    pub offset: u64,
    pub size: u32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageBundleMetadata {
    pub tablet_to_schema: HashMap<i64, i64>,
    pub schemas: HashMap<i64, StarRocksTabletSchema>,
    pub tablet_meta_pages: HashMap<i64, StoragePagePointer>,
}

/// Complete bundle-file facts at the storage-file boundary.  Individual tablet
/// pages remain encoded here so the execution kernel can preserve an existing
/// page verbatim while compat owns the protobuf framing and page rewriting.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageBundleFile {
    pub tablet_metadata_pages: HashMap<i64, Vec<u8>>,
    pub tablet_to_schema: HashMap<i64, i64>,
    pub schemas: HashMap<i64, StarRocksTabletSchema>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageSchemaKey {
    pub db_id: Option<i64>,
    pub table_id: Option<i64>,
    pub schema_id: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageFooterPointer {
    pub position: Option<u64>,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageRowsetTxnMeta {
    pub partial_update_column_ids: Vec<u32>,
    pub partial_update_column_unique_ids: Vec<u32>,
    pub partial_rowset_footers: Vec<StorageFooterPointer>,
    pub merge_condition: Option<String>,
    pub auto_increment_partial_update_column_id: Option<i32>,
    pub partial_update_mode: Option<i32>,
    pub auto_increment_partial_update_column_uid: Option<i32>,
    pub column_to_expr_value: HashMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageWriteOperation {
    pub rowset: Option<StorageRowset>,
    pub txn_meta: Option<StorageRowsetTxnMeta>,
    pub dels: Vec<String>,
    pub rewrite_segments: Vec<String>,
    pub del_encryption_metas: Vec<Vec<u8>>,
    pub ssts: Vec<StorageFile>,
    pub schema_key: Option<StorageSchemaKey>,
}

/// Compaction facts needed by the storage kernel.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageCompactionOperation {
    pub input_rowsets: Vec<u32>,
    pub output_rowset: Option<StorageRowset>,
    pub input_sstables: Vec<StoragePersistentIndexSstable>,
    pub output_sstable: Option<StoragePersistentIndexSstable>,
    pub compact_version: Option<i64>,
    pub new_segment_offset: Option<i32>,
    pub new_segment_count: Option<i32>,
    pub ssts: Vec<StorageFile>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageSchemaChangeOperation {
    pub rowsets: Vec<StorageRowset>,
    pub linked_segment: Option<bool>,
    pub alter_version: Option<i64>,
    pub delvec_meta: Option<StorageDelvecMetadata>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageMetadataUpdate {
    pub enable_persistent_index: Option<bool>,
    pub persistent_index_type: Option<i32>,
    pub bundle_tablet_metadata: Option<bool>,
    pub compaction_strategy: Option<i32>,
    pub flat_json_config: Option<StorageFlatJsonConfig>,
    pub tablet_schema: Option<StarRocksTabletSchema>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageAlterMetadataOperation {
    pub metadata_updates: Vec<StorageMetadataUpdate>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageTransactionLog {
    pub tablet_id: Option<i64>,
    pub txn_id: Option<i64>,
    pub write: Option<StorageWriteOperation>,
    pub compaction: Option<StorageCompactionOperation>,
    pub schema_change: Option<StorageSchemaChangeOperation>,
    pub alter_metadata: Option<StorageAlterMetadataOperation>,
    /// Replication is not supported by the execution kernel yet. Keep it
    /// lossless at the boundary until its execution semantics are added.
    pub replication: Option<Vec<u8>>,
    pub partition_id: Option<i64>,
    pub load_id: Option<(i64, i64)>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageCombinedTransactionLog {
    pub transaction_logs: Vec<StorageTransactionLog>,
}

impl StorageTabletMetadata {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(schema) = self.schema.as_ref() {
            schema.validate()?;
        }
        if self
            .rowsets
            .iter()
            .chain(self.compaction_inputs.iter())
            .any(|rowset| {
                rowset
                    .id
                    .is_some_and(|id| self.rowset_to_schema.contains_key(&id))
                    && self.schema.is_none()
            })
        {
            return Err(
                "storage metadata has rowset schema mapping without a tablet schema".to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_metadata_keeps_rowset_and_schema_facts_separate_from_wire() {
        let metadata = StorageTabletMetadata {
            id: Some(7),
            version: Some(3),
            rowsets: vec![StorageRowset {
                id: Some(11),
                segments: vec!["segment.dat".to_string()],
                num_rows: Some(9),
                ..StorageRowset::default()
            }],
            ..StorageTabletMetadata::default()
        };

        assert_eq!(metadata.rowsets[0].segments, ["segment.dat"]);
        assert_eq!(metadata.rowsets[0].num_rows, Some(9));
    }
}
