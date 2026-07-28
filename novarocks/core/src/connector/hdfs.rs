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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::common::ids::SlotId;
use crate::common::runtime_scan_predicate::RuntimeScanPredicateCounters;
use crate::connector::iceberg::delete_file::{IcebergDeleteFileSpec, IcebergFileContent};
use crate::connector::iceberg::file_pruning::{IcebergFileNullState, IcebergFilePruningCounters};
use crate::connector::iceberg::position_delete::load_position_deletes;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{
    BoundScanRanges, HdfsScanFileFormat, IncrementalScanRange, RuntimeFilterContext, ScanMorsel,
    ScanMorselPruneDecision, ScanMorsels, ScanOp, ScanSource,
};
use crate::formats::{FileFormatConfig, build_format_iter};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::runtime::profile::RuntimeProfile;
use crate::runtime_filter::exec::ordered_range_predicate::NativeOrderedRangePredicate;

fn delete_files_have_position_deletes(delete_files: &[IcebergDeleteFileSpec]) -> bool {
    delete_files
        .iter()
        .any(|file| file.file_content == IcebergFileContent::PositionDeletes)
}

fn apply_parquet_pruning_gate_for_delete_files(
    parquet_cfg: &mut crate::formats::parquet::ParquetScanConfig,
    delete_files: &[IcebergDeleteFileSpec],
) {
    if delete_files_have_position_deletes(delete_files) {
        parquet_cfg.enable_page_index = false;
        parquet_cfg.min_max_predicates.clear();
        parquet_cfg.runtime_min_max_filter_columns.clear();
        parquet_cfg.variant_path_predicates.clear();
    }
}

/// Opens one provider-owned file range without depending on the core `ScanOp`
/// contract. Both the transitional HDFS scan op and the SPI batch reader use
/// this seam so position-delete pruning behavior stays identical.
pub(crate) fn build_hdfs_range_iter(
    cfg: &HdfsScanConfig,
    range: FileScanRange,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    let external_datacache = range.external_datacache.clone();
    let scan = FileScanContext::build(
        vec![range],
        profile.clone(),
        cfg.object_store_config.as_ref(),
    )?;
    if let Some(profile) = profile.as_ref() {
        profile.add_info_string(
            "OriginalRangeCount",
            format!("{}", cfg.original_range_count),
        );
        profile.add_info_string("RangeCount", format!("{}", scan.ranges.len()));
    }
    let current_delete_files = scan
        .ranges
        .first()
        .map(|range| range.delete_files.as_slice())
        .unwrap_or(&[]);
    let Some(mut format) = cfg.format.clone() else {
        return Err("hdfs scan missing file format for non-empty morsel".to_string());
    };
    format = match format {
        FileFormatConfig::Parquet(mut parquet_cfg) => {
            parquet_cfg.datacache = parquet_cfg
                .datacache
                .with_external_range_options(external_datacache.as_ref())?;
            parquet_cfg.query_global_dicts = cfg.query_global_dicts.clone();
            apply_parquet_pruning_gate_for_delete_files(&mut parquet_cfg, current_delete_files);
            FileFormatConfig::Parquet(parquet_cfg)
        }
        FileFormatConfig::Orc(mut orc_cfg) => {
            orc_cfg.datacache = orc_cfg
                .datacache
                .with_external_range_options(external_datacache.as_ref())?;
            FileFormatConfig::Orc(orc_cfg)
        }
    };
    build_format_iter(scan, format, None, profile, runtime_filters)
}

fn exact_ordered_file_candidates(
    stats: &crate::connector::iceberg::scan_model::IcebergColumnStats,
    explicit_null_state: Option<IcebergFileNullState>,
    data_type: &DataType,
) -> Option<ArrayRef> {
    let has_null = match (stats.value_count, stats.null_count) {
        (Some(value_count), Some(null_count)) => {
            if value_count < 0 || null_count < 0 || null_count > value_count {
                return None;
            }
            if value_count == 0 {
                return Some(new_null_array(data_type, 0));
            }
            if value_count == null_count {
                return Some(new_null_array(data_type, 1));
            }
            null_count > 0
        }
        (None, None) => match explicit_null_state? {
            IcebergFileNullState::NoNulls => false,
            IcebergFileNullState::HasNulls => true,
            IcebergFileNullState::AllNull => return Some(new_null_array(data_type, 1)),
        },
        (Some(_), None) | (None, Some(_)) => return None,
    };
    let lower = stats.lower_bound.as_deref()?;
    let upper = stats.upper_bound.as_deref()?;
    macro_rules! candidates {
        ($lower:expr, $upper:expr, $array:ident $(, $finish:expr)?) => {{
            let lower = $lower;
            let upper = $upper;
            if lower > upper {
                return None;
            }
            let values = if has_null {
                vec![Some(lower), Some(upper), None]
            } else {
                vec![Some(lower), Some(upper)]
            };
            let array = $array::from(values);
            $(
                let array = $finish(array)?;
            )?
            Some(Arc::new(array) as ArrayRef)
        }};
    }
    match data_type {
        DataType::Boolean => {
            let decode = |bytes: &[u8]| match bytes {
                [0] => Some(false),
                [1] => Some(true),
                _ => None,
            };
            candidates!(decode(lower)?, decode(upper)?, BooleanArray)
        }
        DataType::Int32 => {
            candidates!(
                i32::from_le_bytes(lower.try_into().ok()?),
                i32::from_le_bytes(upper.try_into().ok()?),
                Int32Array
            )
        }
        DataType::Int64 => {
            candidates!(
                i64::from_le_bytes(lower.try_into().ok()?),
                i64::from_le_bytes(upper.try_into().ok()?),
                Int64Array
            )
        }
        DataType::Date32 => {
            candidates!(
                i32::from_le_bytes(lower.try_into().ok()?),
                i32::from_le_bytes(upper.try_into().ok()?),
                Date32Array
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let lower = i64::from_le_bytes(lower.try_into().ok()?);
            let upper = i64::from_le_bytes(upper.try_into().ok()?);
            if lower > upper {
                return None;
            }
            let values = if has_null {
                vec![Some(lower), Some(upper), None]
            } else {
                vec![Some(lower), Some(upper)]
            };
            Some(Arc::new(
                TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.clone()),
            ) as ArrayRef)
        }
        DataType::Decimal128(precision, scale) => {
            let decode = |bytes: &[u8]| {
                if bytes.is_empty() || bytes.len() > 16 {
                    return None;
                }
                let fill = if bytes[0] & 0x80 == 0 { 0 } else { u8::MAX };
                let mut decoded = [fill; 16];
                decoded[16 - bytes.len()..].copy_from_slice(bytes);
                Some(i128::from_be_bytes(decoded))
            };
            candidates!(
                decode(lower)?,
                decode(upper)?,
                Decimal128Array,
                |array: Decimal128Array| array.with_precision_and_scale(*precision, *scale).ok()
            )
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Default)]
pub struct HdfsIcebergRuntimePruningConfig {
    pub slot_to_column: HashMap<SlotId, String>,
    pub min_max_filter_columns: HashMap<i32, String>,
    pub discrete_set_max_values: usize,
}

#[derive(Clone, Debug)]
pub struct HdfsScanConfig {
    pub ranges: Vec<FileScanRange>,
    /// Original range count from FE `per_node_scan_ranges` before any local coalescing.
    /// This is useful for profiling/debugging when multiple splits point to the same file.
    pub original_range_count: usize,
    pub has_more: bool,
    pub limit: Option<usize>,
    pub profile_label: Option<String>,
    pub format: Option<FileFormatConfig>,
    /// OSS credentials supplied by FE via `THdfsScanNode.cloud_configuration`.
    /// Used as a fallback when the shard registry has no entry for the scanned path
    /// (typical for Iceberg external tables whose files are not tracked as lake tablets).
    pub object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    /// Cached Iceberg table locations keyed by `table_id`, used to resolve incremental
    /// scan ranges that only carry `relative_path`.
    pub iceberg_table_locations: HashMap<i64, String>,
    /// Per-slot global dictionary encode maps (string bytes -> dict id) for
    /// dict-encoded output columns. Empty for all non-dict scans. Injected into
    /// the parquet format config in `execute_iter`; the reader reads the dict
    /// column as Utf8 and encodes the strings to ids.
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
    pub iceberg_runtime_pruning: Option<HdfsIcebergRuntimePruningConfig>,
}

#[derive(Clone, Debug)]
pub struct HdfsScanOp {
    cfg: HdfsScanConfig,
    row_position_scan: bool,
    next_scan_range_id: Arc<AtomicI32>,
    iceberg_runtime_pruning_counters: Arc<HdfsIcebergRuntimePruningCounters>,
    iceberg_runtime_pruning_profile_flushed: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct HdfsIcebergRuntimePruningCounters {
    files_total: AtomicU64,
    files_selected: AtomicU64,
    files_pruned: AtomicU64,
    predicates: AtomicU64,
    unsupported: AtomicU64,
    unavailable: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HdfsIcebergRuntimePruningCounterSnapshot {
    pub(crate) files_total: u64,
    pub(crate) files_selected: u64,
    pub(crate) files_pruned: u64,
    pub(crate) predicates: u64,
    pub(crate) unsupported: u64,
    pub(crate) unavailable: u64,
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_i64_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn atomic_add_saturating(counter: &AtomicU64, delta: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(delta))
    });
}

impl HdfsIcebergRuntimePruningCounters {
    fn record_runtime_predicates(
        &self,
        predicates: usize,
        predicate_counters: &RuntimeScanPredicateCounters,
    ) {
        atomic_add_saturating(&self.predicates, predicates as u64);
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(predicate_counters.unsupported),
        );
    }

    fn record_file_counters(&self, file_counters: &IcebergFilePruningCounters) {
        atomic_add_saturating(
            &self.files_total,
            u128_to_u64_saturating(file_counters.files_total),
        );
        atomic_add_saturating(
            &self.files_selected,
            u128_to_u64_saturating(file_counters.files_selected),
        );
        atomic_add_saturating(
            &self.files_pruned,
            u128_to_u64_saturating(file_counters.files_pruned),
        );
        atomic_add_saturating(
            &self.unsupported,
            u128_to_u64_saturating(file_counters.unsupported),
        );
    }

    fn record_missing_metadata(&self, ranges: usize) {
        let ranges = ranges as u64;
        atomic_add_saturating(&self.files_total, ranges);
        atomic_add_saturating(&self.files_selected, ranges);
        atomic_add_saturating(&self.unsupported, ranges);
    }

    fn record_unavailable(&self) {
        atomic_add_saturating(&self.unavailable, 1);
    }

    fn snapshot(&self) -> HdfsIcebergRuntimePruningCounterSnapshot {
        HdfsIcebergRuntimePruningCounterSnapshot {
            files_total: self.files_total.load(Ordering::Acquire),
            files_selected: self.files_selected.load(Ordering::Acquire),
            files_pruned: self.files_pruned.load(Ordering::Acquire),
            predicates: self.predicates.load(Ordering::Acquire),
            unsupported: self.unsupported.load(Ordering::Acquire),
            unavailable: self.unavailable.load(Ordering::Acquire),
        }
    }
}

impl HdfsScanOp {
    pub fn new(cfg: HdfsScanConfig) -> Self {
        let row_position_scan = cfg
            .ranges
            .iter()
            .any(|r| r.scan_range_id >= 0 || r.first_row_id.is_some());
        let next_scan_range_id = cfg
            .ranges
            .iter()
            .filter_map(|r| (r.scan_range_id >= 0).then_some(r.scan_range_id))
            .max()
            .map(|v| v.saturating_add(1))
            .unwrap_or(0);
        Self {
            cfg,
            row_position_scan,
            next_scan_range_id: Arc::new(AtomicI32::new(next_scan_range_id)),
            iceberg_runtime_pruning_counters: Arc::new(HdfsIcebergRuntimePruningCounters::default()),
            iceberg_runtime_pruning_profile_flushed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn expected_hdfs_file_format(&self) -> Option<HdfsScanFileFormat> {
        match self.cfg.format.as_ref() {
            Some(FileFormatConfig::Parquet(_)) => Some(HdfsScanFileFormat::Parquet),
            Some(FileFormatConfig::Orc(_)) => Some(HdfsScanFileFormat::Orc),
            None => None,
        }
    }

    fn next_incremental_scan_range_id(&self) -> i32 {
        self.next_scan_range_id.fetch_add(1, Ordering::AcqRel)
    }

    fn lowered_delete_files_for_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<IcebergDeleteFileSpec>, String> {
        if let Some(range) =
            self.cfg.ranges.iter().find(|range| {
                range.path == path && range.offset == offset && range.length == length
            })
        {
            return Ok(range.delete_files.clone());
        }

        let same_path_delete_file_count = self
            .cfg
            .ranges
            .iter()
            .filter(|range| range.path == path && !range.delete_files.is_empty())
            .count();
        if same_path_delete_file_count > 0 {
            return Err(format!(
                "incremental HDFS range cannot safely reuse lowered Iceberg delete files for \
                 path={path} offset={offset} length={length}; found \
                 {same_path_delete_file_count} same-path lowered range(s) with delete files but \
                 no exact match"
            ));
        }

        Ok(Vec::new())
    }

    fn ordered_initial_ranges(&self) -> Vec<&FileScanRange> {
        let mut ranges = self.cfg.ranges.iter().collect::<Vec<_>>();
        if self.can_reorder_initial_ranges() {
            ranges.sort_by(|left, right| {
                right
                    .length
                    .cmp(&left.length)
                    .then_with(|| left.path.cmp(&right.path))
                    .then_with(|| left.offset.cmp(&right.offset))
            });
        }
        ranges
    }

    fn can_reorder_initial_ranges(&self) -> bool {
        !self.row_position_scan
            && self.cfg.ranges.iter().all(|range| {
                range.scan_range_id < 0
                    && range.first_row_id.is_none()
                    && range.data_sequence_number.is_none()
                    && range.ivm_change_op.is_none()
                    && range.delete_files.is_empty()
            })
    }

    fn has_iceberg_file_pruning_metadata(&self) -> bool {
        self.cfg
            .ranges
            .iter()
            .any(|range| range.iceberg_file_pruning.is_some())
    }

    fn has_iceberg_runtime_pruning_bindings(pruning_cfg: &HdfsIcebergRuntimePruningConfig) -> bool {
        !pruning_cfg.slot_to_column.is_empty() || !pruning_cfg.min_max_filter_columns.is_empty()
    }

    fn can_materialize_iceberg_runtime_file_pruning(&self) -> bool {
        self.cfg
            .iceberg_runtime_pruning
            .as_ref()
            .is_some_and(Self::has_iceberg_runtime_pruning_bindings)
    }

    fn build_morsels_from_ordered_ranges(
        &self,
        ranges: Vec<&FileScanRange>,
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::with_capacity(ranges.len());
        for r in ranges {
            morsels.push(ScanMorsel::FileRange {
                path: r.path.clone(),
                file_len: r.file_len,
                offset: r.offset,
                length: r.length,
                scan_range_id: r.scan_range_id,
                first_row_id: r.first_row_id,
                data_sequence_number: r.data_sequence_number,
                ivm_change_op: r.ivm_change_op,
                included_positions: r.included_positions.clone(),
                external_datacache: r.external_datacache.clone(),
                delete_files: r.delete_files.clone(),
                iceberg_file_pruning: r.iceberg_file_pruning.clone(),
            });
        }
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn flush_iceberg_runtime_pruning_profile(&self, profile: &RuntimeProfile) {
        if self
            .iceberg_runtime_pruning_profile_flushed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let snapshot = self.iceberg_runtime_pruning_counters.snapshot();
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesTotal",
            u64_to_i64_saturating(snapshot.files_total),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesSelected",
            u64_to_i64_saturating(snapshot.files_selected),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/FilesPruned",
            u64_to_i64_saturating(snapshot.files_pruned),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Predicates",
            u64_to_i64_saturating(snapshot.predicates),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unsupported",
            u64_to_i64_saturating(snapshot.unsupported),
        );
        profile.counter_set_unit(
            "IcebergRuntimeFilePruning/Unavailable",
            u64_to_i64_saturating(snapshot.unavailable),
        );
    }

    #[cfg(test)]
    fn iceberg_runtime_pruning_counter_snapshot_for_test(
        &self,
    ) -> HdfsIcebergRuntimePruningCounterSnapshot {
        self.iceberg_runtime_pruning_counters.snapshot()
    }
}

impl ScanOp for HdfsScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let Some(range) = morsel.file_range() else {
            return Err("hdfs scan received unexpected morsel".to_string());
        };
        build_hdfs_range_iter(&self.cfg, range, profile, runtime_filters)
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        self.build_morsels_from_ordered_ranges(self.ordered_initial_ranges())
    }

    fn flush_morsel_materialization_profile(&self, profile: &RuntimeProfile) {
        self.flush_iceberg_runtime_pruning_profile(profile);
    }

    fn late_prune_morsel_with_ordered_predicate(
        &self,
        morsel: &ScanMorsel,
        slot_id: SlotId,
        predicate: &NativeOrderedRangePredicate,
    ) -> Result<ScanMorselPruneDecision, String> {
        let ScanMorsel::FileRange {
            iceberg_file_pruning: Some(metadata),
            ..
        } = morsel
        else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(pruning) = self.cfg.iceberg_runtime_pruning.as_ref() else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(column) = pruning.slot_to_column.get(&slot_id) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(stats) = metadata.columns.get(column).or_else(|| {
            metadata
                .columns
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, stats)| stats)
        }) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Some(candidates) = exact_ordered_file_candidates(
            stats,
            metadata.null_state(column),
            predicate.data_type(),
        ) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        let Ok(mask) = predicate.evaluate(candidates.as_ref()) else {
            return Ok(ScanMorselPruneDecision::Keep);
        };
        if mask.iter().all(|value| value != Some(true)) {
            Ok(ScanMorselPruneDecision::Skip)
        } else {
            Ok(ScanMorselPruneDecision::Keep)
        }
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        true
    }

    fn build_incremental_morsels(
        &self,
        scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::new();
        let mut has_more = false;
        let expected_file_format = self.expected_hdfs_file_format();

        for scan_range in scan_ranges {
            if let Some(value) = scan_range.has_more() {
                has_more = value;
            }

            let IncrementalScanRange::Hdfs {
                range: hdfs_range, ..
            } = scan_range
            else {
                continue;
            };

            if let Some(expected) = expected_file_format {
                let file_format = hdfs_range.file_format.ok_or_else(|| {
                    "incremental hdfs scan range is missing file_format".to_string()
                })?;
                if file_format != expected {
                    return Err(format!(
                        "incremental hdfs scan range file_format mismatch: expected {:?}, got {:?}",
                        expected, file_format
                    ));
                }
            }

            let path = if let Some(path) = hdfs_range
                .full_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                path.to_string()
            } else if let Some(rel) = hdfs_range
                .relative_path
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                let table_id = hdfs_range.table_id.ok_or_else(|| {
                    "incremental hdfs scan range has relative_path but missing table_id".to_string()
                })?;
                let base = self
                    .cfg
                    .iceberg_table_locations
                    .get(&table_id)
                    .map(|s| s.trim_end_matches('/'))
                    .ok_or_else(|| {
                        format!(
                            "incremental hdfs scan range missing cached iceberg location for table_id={table_id}"
                        )
                    })?;
                let rel = rel.trim_start_matches('/');
                if rel.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}/{rel}")
                }
            } else {
                return Err(
                    "incremental hdfs scan range requires non-empty full_path or relative_path"
                        .to_string(),
                );
            };

            let file_len = hdfs_range.file_length;
            let file_len = if file_len > 0 { file_len as u64 } else { 0 };
            let offset = hdfs_range.offset;
            let offset = if offset >= 0 { offset as u64 } else { 0 };
            let length = hdfs_range.length;
            let mut length = if length > 0 { length as u64 } else { 0 };
            if length == 0 && file_len > offset {
                length = file_len - offset;
            }

            let (scan_range_id, first_row_id) = if self.row_position_scan {
                let first_row_id = hdfs_range.first_row_id.ok_or_else(|| {
                    "incremental hdfs scan range missing first_row_id for row position scan"
                        .to_string()
                })?;
                (self.next_incremental_scan_range_id(), Some(first_row_id))
            } else {
                (-1, None)
            };

            let delete_files = self.lowered_delete_files_for_range(&path, offset, length)?;
            let ivm_change_op = hdfs_range.ivm_change_op;
            // data_sequence_number is not carried by FE incremental ranges.
            // It is populated at initial lowering time from
            // the Iceberg manifest entry for V3 row-lineage tables.
            let data_sequence_number: Option<i64> = None;
            morsels.push(ScanMorsel::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                first_row_id,
                data_sequence_number,
                ivm_change_op,
                included_positions: None,
                external_datacache: hdfs_range.external_datacache.clone(),
                delete_files,
                iceberg_file_pruning: None,
            });
        }

        Ok(ScanMorsels::new(morsels, has_more))
    }

    fn profile_name(&self) -> Option<String> {
        let prefix = "HDFS_SCAN";
        if let Some(label) = self.cfg.profile_label.as_deref()
            && let Some(id) = label
                .strip_prefix("hdfs_scan_node_id=")
                .and_then(|s| s.parse::<i32>().ok())
        {
            return Some(format!("{prefix} (id={id})"));
        }
        Some(prefix.to_string())
    }

    fn load_iceberg_position_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<roaring::RoaringTreemap>, String> {
        let ScanMorsel::FileRange {
            path, delete_files, ..
        } = morsel
        else {
            return Ok(None);
        };
        if delete_files.is_empty() {
            return Ok(None);
        }
        // Build a one-off scan context across the data file and all its delete
        // files so a single OpenDAL operator resolves OSS / HDFS credentials
        // for the entire set. We reuse `FileScanContext::build` for scheme
        // classification and credential resolution, passing zero-length
        // ranges because we never read the data file through this context —
        // only the delete parquet files are read.
        let mut loader_ranges: Vec<crate::fs::scan_context::FileScanRange> =
            Vec::with_capacity(1 + delete_files.len());
        loader_ranges.push(crate::fs::scan_context::FileScanRange {
            path: path.clone(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        });
        for del in delete_files {
            loader_ranges.push(crate::fs::scan_context::FileScanRange {
                path: del.path.clone(),
                file_len: del.length.unwrap_or(0),
                offset: 0,
                length: del.length.unwrap_or(0),
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            });
        }
        let ctx = crate::fs::scan_context::FileScanContext::build(
            loader_ranges,
            None,
            self.cfg.object_store_config.as_ref(),
        )?;
        // After credential resolution `ctx.ranges` carries scheme-normalized
        // paths suitable for the OpenDAL operator, but the delete parquet
        // files record the data-file path exactly as the Iceberg writer saw
        // it (`oss://bucket/...`, `hdfs://ns/...`, or an absolute filesystem
        // path). Compare against the original morsel path so writer-recorded
        // rows match regardless of how OpenDAL normalized the prefix.
        let data_file_path = path.clone();
        let normalized_delete_specs: Vec<IcebergDeleteFileSpec> = ctx
            .ranges
            .iter()
            .skip(1)
            .zip(delete_files.iter())
            .map(|(resolved, original)| IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original.length,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
            .collect();
        let deleted =
            load_position_deletes(&normalized_delete_specs, &data_file_path, &ctx.factory)?;
        if deleted.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deleted))
        }
    }

    fn load_iceberg_equality_deletes(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<Vec<crate::connector::iceberg::equality_delete::EqualityDeleteSet>>, String>
    {
        let ScanMorsel::FileRange {
            path, delete_files, ..
        } = morsel
        else {
            return Ok(None);
        };
        if !delete_files
            .iter()
            .any(|file| file.file_content == IcebergFileContent::EqualityDeletes)
        {
            return Ok(None);
        }
        let mut loader_ranges: Vec<crate::fs::scan_context::FileScanRange> =
            Vec::with_capacity(1 + delete_files.len());
        loader_ranges.push(crate::fs::scan_context::FileScanRange {
            path: path.clone(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        });
        for del in delete_files {
            loader_ranges.push(crate::fs::scan_context::FileScanRange {
                path: del.path.clone(),
                file_len: del.length.unwrap_or(0),
                offset: 0,
                length: del.length.unwrap_or(0),
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            });
        }
        let ctx = crate::fs::scan_context::FileScanContext::build(
            loader_ranges,
            None,
            self.cfg.object_store_config.as_ref(),
        )?;
        let normalized_delete_specs: Vec<IcebergDeleteFileSpec> = ctx
            .ranges
            .iter()
            .skip(1)
            .zip(delete_files.iter())
            .map(|(resolved, original)| IcebergDeleteFileSpec {
                path: resolved.path.clone(),
                file_format: original.file_format,
                file_content: original.file_content,
                length: original.length,
                content_offset: original.content_offset,
                content_size_in_bytes: original.content_size_in_bytes,
            })
            .collect();
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &normalized_delete_specs,
            &ctx.factory,
        )?;
        if sets.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sets))
        }
    }
}

/// Static [`ScanSource`] for HDFS / Iceberg-data file scans.
///
/// Everything except the file ranges is fixed at decode time and stored here.
/// The per-instance `ranges` / `has_more` arrive via [`BoundScanRanges::File`]
/// at bind time. `original_range_count` is not carried on the source: every
/// decoder sets it to `ranges.len()` at construction (compat captures it before
/// `apply_path_rewrite`, which rewrites paths in place without changing the
/// count), so `bind` recomputes it from the bound ranges identically.
pub(crate) struct HdfsScanSource {
    limit: Option<usize>,
    profile_label: Option<String>,
    format: Option<FileFormatConfig>,
    object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
    iceberg_table_locations: HashMap<i64, String>,
    query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
    iceberg_runtime_pruning: Option<HdfsIcebergRuntimePruningConfig>,
}

impl HdfsScanSource {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        limit: Option<usize>,
        profile_label: Option<String>,
        format: Option<FileFormatConfig>,
        object_store_config: Option<crate::fs::object_store::ObjectStoreConfig>,
        iceberg_table_locations: HashMap<i64, String>,
        query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
        iceberg_runtime_pruning: Option<HdfsIcebergRuntimePruningConfig>,
    ) -> Self {
        Self {
            limit,
            profile_label,
            format,
            object_store_config,
            iceberg_table_locations,
            query_global_dicts,
            iceberg_runtime_pruning,
        }
    }
}

impl ScanSource for HdfsScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        let BoundScanRanges::File { ranges, has_more } = ranges else {
            return Err("hdfs scan source requires File scan ranges".to_string());
        };
        let cfg = HdfsScanConfig {
            original_range_count: ranges.len(),
            ranges,
            has_more,
            limit: self.limit,
            profile_label: self.profile_label.clone(),
            format: self.format.clone(),
            object_store_config: self.object_store_config.clone(),
            iceberg_table_locations: self.iceberg_table_locations.clone(),
            query_global_dicts: self.query_global_dicts.clone(),
            iceberg_runtime_pruning: self.iceberg_runtime_pruning.clone(),
        };
        Ok(Arc::new(HdfsScanOp::new(cfg)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::cache::{CacheOptions, DataCacheManager};
    use crate::common::ids::SlotId;
    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::connector::iceberg::delete_file::{
        IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
    };
    use crate::connector::iceberg::file_pruning::{
        IcebergFileNullState, IcebergFilePruningMetadata,
    };
    #[cfg(feature = "compat")]
    use crate::connector::iceberg::file_pruning_wire::{
        iceberg_file_pruning_metadata_from_thrift, iceberg_file_pruning_metadata_to_thrift,
    };
    use crate::connector::iceberg::scan_model::IcebergColumnStats;
    #[cfg(feature = "compat")]
    use crate::connector::iceberg::scan_model::IcebergDataFileInfo;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::node::scan::{
        BoundScanRanges, HdfsScanFileFormat, IncrementalHdfsScanRange, IncrementalScanRange,
        ScanMorsel, ScanMorselPruneDecision, ScanOp, ScanSource,
    };
    use crate::formats::parquet::{
        ParquetReadCachePolicy, ParquetScanConfig, ParquetSlotKind, VariantPathPruningPredicate,
    };
    use crate::fs::scan_context::FileScanRange;
    use crate::runtime_filter::exec::ordered_range_predicate::{
        NativeOrderedRangePredicate, OrderedRangePredicateContract,
    };
    use crate::runtime_filter::model::contract::{ChannelId, NullOrder, SortDirection};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::OrderedScalar;

    use super::{
        HdfsIcebergRuntimePruningConfig, HdfsScanConfig, HdfsScanOp, HdfsScanSource,
        apply_parquet_pruning_gate_for_delete_files,
    };

    fn plain_file_range(path: &str) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }
    }

    fn hdfs_scan_source_for_test() -> HdfsScanSource {
        HdfsScanSource::new(
            None,
            None,
            None,
            None,
            HashMap::new(),
            Default::default(),
            None,
        )
    }

    #[test]
    fn hdfs_scan_source_bind_file_ranges_yields_file_morsels() {
        let source = hdfs_scan_source_for_test();
        let op = source
            .bind(BoundScanRanges::File {
                ranges: vec![
                    plain_file_range("s3://bucket/path/a.parquet"),
                    plain_file_range("s3://bucket/path/b.parquet"),
                ],
                has_more: false,
            })
            .expect("hdfs scan source bind should succeed");

        let morsels = op.build_morsels().expect("build morsels");
        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 2);
        assert!(
            morsels
                .morsels
                .iter()
                .all(|morsel| matches!(morsel, ScanMorsel::FileRange { .. })),
            "expected all FileRange morsels, got {:?}",
            morsels.morsels
        );
    }

    #[test]
    fn hdfs_scan_source_bind_rejects_wrong_variant() {
        let source = hdfs_scan_source_for_test();
        let Err(err) = source.bind(BoundScanRanges::None) else {
            panic!("non-File scan ranges must be rejected");
        };
        assert!(
            err.contains("hdfs scan source requires File scan ranges"),
            "err={err}"
        );
    }

    fn make_hdfs_range(path: &str, first_row_id: Option<i64>) -> IncrementalScanRange {
        make_hdfs_range_with_change_op(path, first_row_id, None)
    }

    fn make_hdfs_range_with_change_op(
        path: &str,
        first_row_id: Option<i64>,
        ivm_change_op: Option<i8>,
    ) -> IncrementalScanRange {
        IncrementalScanRange::Hdfs {
            has_more: None,
            range: IncrementalHdfsScanRange {
                file_format: Some(HdfsScanFileFormat::Parquet),
                full_path: Some(path.to_string()),
                relative_path: None,
                table_id: None,
                file_length: 256,
                offset: 0,
                length: 100,
                first_row_id,
                ivm_change_op,
                external_datacache: None,
            },
        }
    }

    fn make_end_marker(has_more: bool) -> IncrementalScanRange {
        IncrementalScanRange::Empty {
            has_more: Some(has_more),
        }
    }

    fn test_datacache_context() -> crate::cache::DataCacheContext {
        let cache_options = CacheOptions::from_query_options(None).expect("cache options");
        DataCacheManager::instance().external_context(cache_options)
    }

    fn test_delete_file(file_content: IcebergFileContent) -> IcebergDeleteFileSpec {
        IcebergDeleteFileSpec {
            path: "delete.parquet".to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn test_iceberg_file_pruning_metadata() -> IcebergFilePruningMetadata {
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
            null_states: HashMap::new(),
        }
    }

    fn iceberg_file_pruning_metadata_for_i32_range(
        column: &str,
        lower: i32,
        upper: i32,
    ) -> IcebergFilePruningMetadata {
        IcebergFilePruningMetadata {
            columns: HashMap::from([(
                column.to_string(),
                IcebergColumnStats {
                    null_count: None,
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(lower.to_le_bytes().to_vec()),
                    upper_bound: Some(upper.to_le_bytes().to_vec()),
                },
            )]),
            null_states: HashMap::new(),
        }
    }

    fn exact_iceberg_file_pruning_metadata_for_i32_range(
        column: &str,
        lower: i32,
        upper: i32,
    ) -> IcebergFilePruningMetadata {
        let mut metadata = iceberg_file_pruning_metadata_for_i32_range(column, lower, upper);
        let stats = metadata
            .columns
            .get_mut(column)
            .expect("exact Iceberg column stats");
        stats.null_count = Some(0);
        stats.value_count = Some(2);
        metadata
    }

    fn ordered_i32_predicate(bound: i32) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int32,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Int32(bound)),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered predicate contract"),
        )
        .expect("ordered predicate")
    }

    #[cfg(feature = "compat")]
    fn ordered_i64_predicate(bound: i64) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Int64(bound)),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered predicate contract"),
        )
        .expect("ordered predicate")
    }

    fn ordered_utf8_predicate(bound: &str) -> NativeOrderedRangePredicate {
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Utf8,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let version = LogicalVersion::FIRST;
        let bundle = crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order.clone(),
            Some(OrderedScalar::Utf8(Arc::from(bound))),
            version,
        );
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, version)
                .expect("ordered UTF8 predicate contract"),
        )
        .expect("ordered UTF8 predicate")
    }

    fn iceberg_file_range_for_runtime_pruning_test(
        path: &str,
        stats: Option<IcebergFilePruningMetadata>,
    ) -> FileScanRange {
        FileScanRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: stats,
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/hit.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    #[test]
    fn native_scan_ordered_live_hdfs_skips_only_exact_file_range_evidence() {
        let mut cfg = hdfs_cfg_with_two_iceberg_files_for_test();
        cfg.ranges = Vec::new();
        let op = HdfsScanOp::new(cfg);
        let predicate = ordered_i32_predicate(50);

        let exact_miss = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/exact-miss.parquet",
            Some(exact_iceberg_file_pruning_metadata_for_i32_range(
                "k1", 90, 110,
            )),
        );
        let exact_miss = HdfsScanOp::new(HdfsScanConfig {
            ranges: vec![exact_miss],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("exact morsel")
        .morsels
        .pop()
        .expect("exact file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(&exact_miss, SlotId::new(3), &predicate,)
                .expect("exact late prune"),
            ScanMorselPruneDecision::Skip
        );

        let mut explicit_no_nulls = iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110);
        explicit_no_nulls
            .null_states
            .insert("k1".to_string(), IcebergFileNullState::NoNulls);
        let explicit_no_nulls = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/explicit-no-nulls.parquet",
            Some(explicit_no_nulls),
        );
        let explicit_no_nulls = HdfsScanOp::new(HdfsScanConfig {
            ranges: vec![explicit_no_nulls],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("explicit-null-state morsel")
        .morsels
        .pop()
        .expect("explicit-null-state file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &explicit_no_nulls,
                SlotId::new(3),
                &predicate,
            )
            .expect("explicit-null-state late prune"),
            ScanMorselPruneDecision::Skip
        );

        let missing_counts = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/missing-counts.parquet",
            Some(iceberg_file_pruning_metadata_for_i32_range("k1", 90, 110)),
        );
        let missing_counts = HdfsScanOp::new(HdfsScanConfig {
            ranges: vec![missing_counts],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("missing-counts morsel")
        .morsels
        .pop()
        .expect("missing-counts file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &missing_counts,
                SlotId::new(3),
                &predicate,
            )
            .expect("missing-counts late prune"),
            ScanMorselPruneDecision::Keep
        );

        let missing_metadata = HdfsScanOp::new(HdfsScanConfig {
            ranges: vec![iceberg_file_range_for_runtime_pruning_test(
                "s3://bucket/path/missing-metadata.parquet",
                None,
            )],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("missing-metadata morsel")
        .morsels
        .pop()
        .expect("missing-metadata file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &missing_metadata,
                SlotId::new(3),
                &predicate,
            )
            .expect("missing-metadata late prune"),
            ScanMorselPruneDecision::Keep
        );

        let unsupported_metadata = IcebergFilePruningMetadata {
            columns: HashMap::from([(
                "k1".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(2),
                    column_size: None,
                    lower_bound: Some(b"z".to_vec()),
                    upper_bound: Some(b"zz".to_vec()),
                },
            )]),
            null_states: HashMap::new(),
        };
        let unsupported = HdfsScanOp::new(HdfsScanConfig {
            ranges: vec![iceberg_file_range_for_runtime_pruning_test(
                "s3://bucket/path/unsupported-string-bounds.parquet",
                Some(unsupported_metadata),
            )],
            ..op.cfg.clone()
        })
        .build_morsels()
        .expect("unsupported morsel")
        .morsels
        .pop()
        .expect("unsupported file range");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &unsupported,
                SlotId::new(3),
                &ordered_utf8_predicate("m"),
            )
            .expect("unsupported late prune"),
            ScanMorselPruneDecision::Keep
        );
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &ScanMorsel::Schema {
                    table_name: "not-a-file-range".to_string(),
                },
                SlotId::new(3),
                &predicate,
            )
            .expect("non-file late prune"),
            ScanMorselPruneDecision::Keep
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn thrift_missing_null_count_keeps_ordered_late_pruning_conservative_after_wire_roundtrip() {
        let mut file =
            IcebergDataFileInfo::for_test("s3://bucket/path/missing-null-count.parquet", 1024, 2);
        file.column_stats = Some(HashMap::from([(
            "k1".to_string(),
            IcebergColumnStats {
                null_count: None,
                value_count: Some(2),
                column_size: None,
                lower_bound: Some(90_i64.to_le_bytes().to_vec()),
                upper_bound: Some(110_i64.to_le_bytes().to_vec()),
            },
        )]));
        let columns = vec![novarocks_catalog::schema::ColumnDef {
            name: "k1".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let encoded = iceberg_file_pruning_metadata_to_thrift(&file, &columns);
        let wire = crate::thrift::plan_nodes::THdfsScanRange {
            min_max_values: encoded,
            ..Default::default()
        };
        let decoded = iceberg_file_pruning_metadata_from_thrift(&wire, &["k1".to_string()]);
        let range = iceberg_file_range_for_runtime_pruning_test(
            "s3://bucket/path/missing-null-count.parquet",
            decoded,
        );
        let mut cfg = hdfs_cfg_with_two_iceberg_files_for_test();
        cfg.ranges = vec![range];
        let op = HdfsScanOp::new(cfg);
        let morsel = op
            .build_morsels()
            .expect("thrift roundtrip morsel")
            .morsels
            .pop()
            .expect("thrift file morsel");
        assert_eq!(
            op.late_prune_morsel_with_ordered_predicate(
                &morsel,
                SlotId::new(3),
                &ordered_i64_predicate(50),
            )
            .expect("thrift late prune"),
            ScanMorselPruneDecision::Keep,
            "missing null_count must not be encoded as explicit NoNulls"
        );
    }

    fn hdfs_cfg_with_all_pruned_iceberg_files_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-a.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 1, 10)),
                ),
                iceberg_file_range_for_runtime_pruning_test(
                    "s3://bucket/path/miss-b.parquet",
                    Some(iceberg_file_pruning_metadata_for_i32_range("k1", 20, 30)),
                ),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn hdfs_cfg_with_two_iceberg_files_without_metadata_for_test() -> HdfsScanConfig {
        HdfsScanConfig {
            ranges: vec![
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/hit.parquet", None),
                iceberg_file_range_for_runtime_pruning_test("s3://bucket/path/miss.parquet", None),
            ],
            original_range_count: 2,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: Some(HdfsIcebergRuntimePruningConfig {
                slot_to_column: HashMap::from([(SlotId::new(3), "k1".to_string())]),
                min_max_filter_columns: HashMap::new(),
                discrete_set_max_values: 256,
            }),
        }
    }

    fn test_prunable_parquet_config() -> ParquetScanConfig {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("__nr_var_payload_a", DataType::Int64, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![
                ParquetSlotKind::Regular,
                ParquetSlotKind::Regular,
                ParquetSlotKind::Variant,
            ],
            case_sensitive: true,
            enable_page_index: true,
            min_max_predicates: vec![MinMaxPredicate::Gt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            }],
            runtime_min_max_filter_columns: std::collections::HashMap::new(),
            variant_path_predicates: vec![VariantPathPruningPredicate {
                output_slot_id: SlotId::new(2),
                source_slot_id: SlotId::new(3),
                source_field_id: Some(10),
                canonical_path: "$.a".to_string(),
                requested_type: DataType::Int64,
                predicate: MinMaxPredicate::Gt {
                    column: "__nr_var_payload_a".to_string(),
                    value: MinMaxPredicateValue::Int64(7),
                },
            }],
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]))),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        }
    }

    #[test]
    fn hdfs_scan_position_delete_morsel_strips_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();
        parquet_cfg
            .runtime_min_max_filter_columns
            .insert(11, "id".to_string());

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::PositionDeletes)],
        );

        assert!(!parquet_cfg.enable_page_index);
        assert!(parquet_cfg.min_max_predicates.is_empty());
        assert!(parquet_cfg.runtime_min_max_filter_columns.is_empty());
        assert!(parquet_cfg.variant_path_predicates.is_empty());
    }

    #[test]
    fn hdfs_scan_equality_delete_morsel_keeps_parquet_pruning() {
        let mut parquet_cfg = test_prunable_parquet_config();

        apply_parquet_pruning_gate_for_delete_files(
            &mut parquet_cfg,
            &[test_delete_file(IcebergFileContent::EqualityDeletes)],
        );

        assert!(parquet_cfg.enable_page_index);
        assert_eq!(parquet_cfg.min_max_predicates.len(), 1);
        assert_eq!(parquet_cfg.variant_path_predicates.len(), 1);
    }

    #[test]
    fn incremental_hdfs_ranges_parse_data_and_end_marker() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                path,
                scan_range_id,
                ..
            } => {
                assert_eq!(path, "s3://bucket/path/file.parquet");
                assert_eq!(*scan_range_id, -1);
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_assign_row_position_scan_range_id_contiguously() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/seed.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: 7,
                first_row_id: Some(10),
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/a.parquet", Some(1000)),
                make_hdfs_range("s3://bucket/path/b.parquet", Some(2000)),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        assert!(!morsels.has_more);
        assert_eq!(morsels.morsels.len(), 2);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 8);
                assert_eq!(*first_row_id, Some(1000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
        match &morsels.morsels[1] {
            ScanMorsel::FileRange {
                scan_range_id,
                first_row_id,
                ..
            } => {
                assert_eq!(*scan_range_id, 9);
                assert_eq!(*first_row_id, Some(2000));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reuse_lowered_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[
                make_hdfs_range("s3://bucket/path/file.parquet", None),
                make_end_marker(false),
            ])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert_eq!(delete_files.len(), 1);
                assert_eq!(
                    delete_files[0].file_content,
                    IcebergFileContent::PositionDeletes
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_reject_same_path_delete_files_without_exact_match() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 100,
                offset: 64,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let err = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect_err("same-path delete files without exact lowered range must fail closed");

        assert!(err.contains("cannot safely reuse lowered Iceberg delete files"));
        assert!(err.contains("s3://bucket/path/file.parquet"));
        assert!(err.contains("offset=0"));
        assert!(err.contains("length=100"));
    }

    #[test]
    fn incremental_hdfs_ranges_allow_empty_delete_files_without_same_path_delete_files() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/other.parquet".to_string(),
                file_len: 100,
                offset: 0,
                length: 100,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: vec![test_delete_file(IcebergFileContent::PositionDeletes)],
                iceberg_file_pruning: None,
            }],
            original_range_count: 1,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range("s3://bucket/path/file.parquet", None)])
            .expect("build incremental morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange { delete_files, .. } => {
                assert!(delete_files.is_empty());
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn incremental_hdfs_ranges_propagate_change_op_extended_column() {
        let cfg = HdfsScanConfig {
            ranges: vec![],
            original_range_count: 0,
            has_more: true,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op
            .build_incremental_morsels(&[make_hdfs_range_with_change_op(
                "s3://bucket/path/file.parquet",
                None,
                Some(-1),
            )])
            .expect("build incremental morsels");

        assert_eq!(morsels.morsels.len(), 1);
        match &morsels.morsels[0] {
            ScanMorsel::FileRange { ivm_change_op, .. } => {
                assert_eq!(*ivm_change_op, Some(-1));
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }

    #[test]
    fn build_morsels_prioritizes_large_plain_ranges() {
        let cfg = HdfsScanConfig {
            ranges: vec![
                FileScanRange {
                    path: "s3://bucket/path/small-a.parquet".to_string(),
                    file_len: 1024,
                    offset: 0,
                    length: 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/large.parquet".to_string(),
                    file_len: 128 * 1024 * 1024,
                    offset: 0,
                    length: 128 * 1024 * 1024,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
                FileScanRange {
                    path: "s3://bucket/path/small-b.parquet".to_string(),
                    file_len: 2048,
                    offset: 0,
                    length: 2048,
                    scan_range_id: -1,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                },
            ],
            original_range_count: 3,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        let paths = morsels
            .morsels
            .iter()
            .map(|morsel| match morsel {
                ScanMorsel::FileRange { path, .. } => path.as_str(),
                other => panic!("unexpected morsel: {:?}", other),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "s3://bucket/path/large.parquet",
                "s3://bucket/path/small-b.parquet",
                "s3://bucket/path/small-a.parquet",
            ]
        );
    }

    #[test]
    fn build_morsels_preserves_iceberg_file_pruning_metadata() {
        let cfg = HdfsScanConfig {
            ranges: vec![FileScanRange {
                path: "s3://bucket/path/file.parquet".to_string(),
                file_len: 1024,
                offset: 0,
                length: 1024,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: Some(test_iceberg_file_pruning_metadata()),
            }],
            original_range_count: 1,
            has_more: false,
            limit: None,
            profile_label: None,
            format: None,
            object_store_config: None,
            iceberg_table_locations: std::collections::HashMap::new(),
            query_global_dicts: Default::default(),
            iceberg_runtime_pruning: None,
        };
        let op = HdfsScanOp::new(cfg);

        let morsels = op.build_morsels().expect("build morsels");

        match &morsels.morsels[0] {
            ScanMorsel::FileRange {
                iceberg_file_pruning,
                ..
            } => {
                let metadata = iceberg_file_pruning
                    .as_ref()
                    .expect("iceberg pruning metadata");
                assert_eq!(
                    metadata.columns["id"].upper_bound,
                    Some(20_i64.to_le_bytes().to_vec())
                );
            }
            other => panic!("unexpected morsel: {:?}", other),
        }
    }
}
