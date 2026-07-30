//! Protocol-neutral command and result types for lake service operations.
//!
//! StarRocks protobuf is interpreted by compat at the RPC boundary. The core
//! storage kernel receives these facts plus explicit capabilities instead of
//! generated wire messages.

use std::collections::HashMap;

use crate::common::types::UniqueId;
use crate::connector::starrocks::lake::storage_domain::{
    StorageDeletePredicate, StorageSchemaKey, StorageTransactionLog,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LakeTransactionType {
    Normal,
    Replication,
    Empty,
    TabletReshard,
    /// Preserve a newer FE enum value without making the execution kernel
    /// silently reinterpret it as a normal transaction.
    Unknown(i32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeTransactionInfo {
    pub txn_id: i64,
    pub commit_time: Option<i64>,
    pub combined_txn_log: bool,
    pub transaction_type: LakeTransactionType,
    pub force_publish: bool,
    pub rebuild_pindex: bool,
    pub gtid: i64,
    pub load_ids: Vec<UniqueId>,
}

/// Lossless, protocol-neutral representation of one publish resharding fact.
/// The wire permits any combination of the optional variants, so retain them
/// independently instead of collapsing the value into an enum.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LakeReshardingTabletInfo {
    pub splitting: Option<LakeSplittingTabletInfo>,
    pub merging: Option<LakeMergingTabletInfo>,
    pub identical: Option<LakeIdenticalTabletInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeSplittingTabletInfo {
    pub old_tablet_id: Option<i64>,
    pub new_tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeMergingTabletInfo {
    pub old_tablet_ids: Vec<i64>,
    pub new_tablet_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LakeIdenticalTabletInfo {
    pub old_tablet_id: Option<i64>,
    pub new_tablet_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishVersionCommand {
    pub tablet_ids: Vec<i64>,
    pub transaction_ids: Vec<i64>,
    pub base_version: Option<i64>,
    pub new_version: Option<i64>,
    pub commit_time: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub transactions: Vec<LakeTransactionInfo>,
    pub rebuild_pindex_tablet_ids: Vec<i64>,
    pub enable_aggregate_publish: Option<bool>,
    pub resharding_tablet_infos: Vec<LakeReshardingTabletInfo>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PublishVersionResult {
    pub failed_tablets: Vec<i64>,
    pub compaction_scores: HashMap<i64, f64>,
    pub tablet_row_nums: HashMap<i64, i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishLogVersionCommand {
    pub tablet_ids: Vec<i64>,
    pub transaction_id: Option<i64>,
    pub version: Option<i64>,
    pub transaction: Option<LakeTransactionInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishLogVersionBatchCommand {
    pub tablet_ids: Vec<i64>,
    pub transaction_ids: Vec<i64>,
    pub versions: Vec<i64>,
    pub transactions: Vec<LakeTransactionInfo>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FailedTabletsResult {
    pub failed_tablets: Vec<i64>,
}

/// Successful lake operations whose protobuf response contains only the
/// standard OK status are represented by this unit result.  Formatting that
/// status remains at the compat wire boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LakeOkResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortTransactionCommand {
    pub tablet_ids: Vec<i64>,
    pub transaction_ids: Vec<i64>,
    pub skip_cleanup: Option<bool>,
    pub transaction_types: Vec<LakeTransactionType>,
    pub transactions: Vec<LakeTransactionInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropLakeTableCommand {
    pub tablet_id: Option<i64>,
    pub path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteTabletsCommand {
    pub tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteDataCommand {
    pub tablet_ids: Vec<i64>,
    pub txn_id: Option<i64>,
    pub delete_predicate: Option<StorageDeletePredicate>,
    pub schema_key: Option<StorageSchemaKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TabletVersion {
    pub tablet_id: i64,
    pub version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TabletStatsCommand {
    pub tablet_versions: Vec<TabletVersion>,
    pub timeout_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TabletStat {
    pub tablet_id: i64,
    pub num_rows: i64,
    pub data_size: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TabletStatsResult {
    pub tablet_stats: Vec<TabletStat>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTabletsCommand {
    pub tablet_ids: Vec<i64>,
    pub txn_id: Option<i64>,
    pub version: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub allow_partial_success: Option<bool>,
    pub encryption_meta: Option<Vec<u8>>,
    pub force_base_compaction: Option<bool>,
    pub skip_write_txnlog: Option<bool>,
    pub parallel_config: Option<CompactParallelConfig>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactParallelConfig {
    pub enable_parallel: Option<bool>,
    pub max_parallel_per_tablet: Option<i32>,
    pub max_bytes_per_subtask: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionStat {
    pub tablet_id: i64,
    pub total_compact_input_file_size: i64,
    pub read_segment_count: i64,
    pub write_segment_count: i64,
    pub write_segment_bytes: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactTabletsResult {
    pub failed_tablets: Vec<i64>,
    pub compact_stats: Vec<CompactionStat>,
    pub success_compaction_input_file_size: i64,
    pub txn_logs: Vec<StorageTransactionLog>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortCompactionCommand {
    pub txn_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumCommand {
    pub tablet_ids: Vec<i64>,
    pub tablet_min_versions: Vec<(i64, Option<i64>)>,
    pub min_retain_version: Option<i64>,
    pub grace_timestamp: Option<i64>,
    pub min_active_txn_id: Option<i64>,
    pub delete_txn_log: Option<bool>,
    pub partition_id: Option<i64>,
    pub enable_file_bundling: Option<bool>,
    pub retain_versions: Vec<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VacuumResult {
    pub vacuumed_files: i64,
    pub vacuumed_file_size: i64,
    pub vacuumed_version: i64,
    pub tablet_min_versions: Vec<(i64, i64)>,
    pub extra_file_size: i64,
}
