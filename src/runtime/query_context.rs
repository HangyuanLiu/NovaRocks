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
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::cache::{CacheOptions, ExternalDataCacheRangeOptions};
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::node::scan::HdfsScanFileFormat;
use crate::exec::node::scan::IncrementalHdfsScanRange;
use crate::exec::node::scan::IncrementalScanRange;
#[cfg(feature = "compat")]
use crate::exec::node::scan::LakeGlmScanInfo;
use crate::exec::node::scan::RowPositionScanConfig;
use crate::exec::node::scan::ScanNode;
use crate::exec::operators::scan::dispatch::ScanDispatchState;
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::global_driver_executor::FragmentCompletion;
use crate::exec::row_position::RowPositionDescriptor;
use crate::fs::scan_context::FileScanRange;
use crate::runtime::descriptor_snapshot::DescriptorSnapshot;
#[cfg(feature = "compat")]
use crate::runtime::descriptor_snapshot_thrift::descriptor_snapshot_from_thrift;
use crate::runtime::lookup::GlobalLateMaterializationContext;
use crate::runtime::mem_tracker::{self, MemTracker};
pub(crate) use crate::runtime::query_options::query_expire_durations;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::runtime::runtime_filter_worker::{RuntimeFilterWorker, RuntimeFilterWorkerParams};
#[cfg(feature = "compat")]
use crate::thrift::descriptors;
#[cfg(feature = "compat")]
use crate::thrift::internal_service;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct QueryId {
    pub(crate) hi: i64,
    pub(crate) lo: i64,
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let uuid = crate::common::types::format_uuid(self.hi, self.lo);
        f.write_str(&uuid)
    }
}

pub(crate) struct QueryContext {
    #[allow(dead_code)]
    pub(crate) query_id: QueryId,
    pub(crate) cache_options: Option<CacheOptions>,
    #[cfg(feature = "compat")]
    pub(crate) desc_tbl: Option<descriptors::TDescriptorTable>,
    pub(crate) desc_snapshot: Option<Arc<DescriptorSnapshot>>,
    pub(crate) num_fragments: usize,
    pub(crate) num_active_fragments: usize,
    pub(crate) total_fragments: Option<usize>,
    pub(crate) cancelled_by_fe: bool,
    pub(crate) delivery_expire: Duration,
    pub(crate) delivery_deadline: Instant,
    #[allow(dead_code)]
    pub(crate) query_expire: Duration,
    #[allow(dead_code)]
    pub(crate) query_deadline: Instant,
    pub(crate) exchange_senders: HashMap<i32, usize>,
    pub(crate) runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    pub(crate) runtime_filter_params: Option<RuntimeFilterParams>,
    pub(crate) runtime_filter_worker_params: Option<RuntimeFilterWorkerParams>,
    pub(crate) runtime_filter_worker: Option<Arc<RuntimeFilterWorker>>,
    pub(crate) pending_runtime_filters: Vec<PendingRuntimeFilter>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) glm_contexts: HashMap<SlotId, GlobalLateMaterializationContext>,
    #[cfg(feature = "compat")]
    pub(crate) lake_glm_contexts: HashMap<SlotId, LakeGlmScanInfo>,
    pub(crate) lake_tablet_paths: HashMap<String, HashMap<i64, String>>,
    pub(crate) mem_tracker: Arc<MemTracker>,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRuntimeFilter {
    pub(crate) filter_id: i32,
    pub(crate) build_be_number: i32,
    pub(crate) data: Vec<u8>,
    pub(crate) build_data_type: Option<DataType>,
}

impl QueryContext {
    pub(crate) fn new(
        query_id: QueryId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Self {
        let now = Instant::now();
        let process = mem_tracker::process_mem_tracker();
        let query_label = format!("query_{:x}_{:x}", query_id.hi, query_id.lo);
        let mem_tracker = MemTracker::new_child(query_label, &process);
        Self {
            query_id,
            cache_options: None,
            #[cfg(feature = "compat")]
            desc_tbl: None,
            desc_snapshot: None,
            num_fragments: 0,
            num_active_fragments: 0,
            total_fragments: None,
            cancelled_by_fe: false,
            delivery_expire,
            delivery_deadline: now + delivery_expire,
            query_expire,
            query_deadline: now + query_expire,
            exchange_senders: HashMap::new(),
            runtime_filter_hub: None,
            runtime_filter_params: None,
            runtime_filter_worker_params: None,
            runtime_filter_worker: None,
            pending_runtime_filters: Vec::new(),
            row_pos_descs: HashMap::new(),
            glm_contexts: HashMap::new(),
            #[cfg(feature = "compat")]
            lake_glm_contexts: HashMap::new(),
            lake_tablet_paths: HashMap::new(),
            mem_tracker,
        }
    }

    pub(crate) fn increment_num_fragments(&mut self) {
        self.num_fragments += 1;
        self.num_active_fragments += 1;
    }

    #[allow(dead_code)]
    pub(crate) fn rollback_inc_fragments(&mut self) {
        self.num_fragments = self.num_fragments.saturating_sub(1);
        self.num_active_fragments = self.num_active_fragments.saturating_sub(1);
    }

    pub(crate) fn count_down_fragments(&mut self) -> bool {
        if self.num_active_fragments > 0 {
            self.num_active_fragments -= 1;
        }
        self.num_active_fragments == 0
    }

    pub(crate) fn has_no_active_instances(&self) -> bool {
        self.num_active_fragments == 0
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.num_active_fragments == 0
            && (self.cancelled_by_fe
                || self
                    .total_fragments
                    .map(|t| self.num_fragments >= t)
                    .unwrap_or(false))
    }

    pub(crate) fn is_delivery_expired(&self) -> bool {
        Instant::now() >= self.delivery_deadline
    }

    #[allow(dead_code)]
    pub(crate) fn is_query_expired(&self) -> bool {
        Instant::now() >= self.query_deadline
    }

    pub(crate) fn extend_delivery_lifetime(&mut self) {
        self.delivery_deadline = Instant::now() + self.delivery_expire;
    }

    pub(crate) fn update_exchange_senders(&mut self, counts: HashMap<i32, usize>) {
        for (node_id, count) in counts {
            let entry = self.exchange_senders.entry(node_id).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
    }

    pub(crate) fn exchange_sender_count(&self, node_id: i32) -> Option<usize> {
        self.exchange_senders.get(&node_id).copied()
    }

    pub(crate) fn set_runtime_filter_hub(&mut self, hub: Arc<RuntimeFilterHub>) {
        self.runtime_filter_hub = Some(hub);
    }

    pub(crate) fn runtime_filter_hub(&self) -> Option<Arc<RuntimeFilterHub>> {
        self.runtime_filter_hub.clone()
    }

    pub(crate) fn set_runtime_filter_params(&mut self, params: RuntimeFilterParams) {
        if self.runtime_filter_params.is_none() {
            self.runtime_filter_worker_params = Some(params.to_worker_params());
            self.runtime_filter_params = Some(params);
        }
    }

    pub(crate) fn runtime_filter_params(&self) -> Option<RuntimeFilterParams> {
        self.runtime_filter_params.clone()
    }

    pub(crate) fn runtime_filter_worker_params(&self) -> Option<RuntimeFilterWorkerParams> {
        self.runtime_filter_worker_params.clone()
    }

    pub(crate) fn set_runtime_filter_worker(&mut self, worker: Arc<RuntimeFilterWorker>) {
        self.runtime_filter_worker = Some(worker);
    }

    pub(crate) fn runtime_filter_worker(&self) -> Option<Arc<RuntimeFilterWorker>> {
        self.runtime_filter_worker.clone()
    }

    pub(crate) fn set_row_pos_descs(&mut self, descs: HashMap<i32, RowPositionDescriptor>) {
        self.row_pos_descs = descs;
    }

    pub(crate) fn row_pos_desc(&self, tuple_id: i32) -> Option<RowPositionDescriptor> {
        self.row_pos_descs.get(&tuple_id).cloned()
    }

    pub(crate) fn register_glm_scan_ranges(
        &mut self,
        row_source_slot: SlotId,
        scan_cfg: RowPositionScanConfig,
        ranges: Vec<FileScanRange>,
    ) {
        let ctx = self
            .glm_contexts
            .entry(row_source_slot)
            .or_insert_with(|| GlobalLateMaterializationContext::new(row_source_slot, scan_cfg));
        ctx.register_ranges(ranges);
    }

    pub(crate) fn glm_scan_range(
        &self,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<FileScanRange> {
        self.glm_contexts
            .get(&row_source_slot)
            .and_then(|ctx| ctx.get_scan_range(scan_range_id).cloned())
    }

    pub(crate) fn glm_scan_config(&self, row_source_slot: SlotId) -> Option<RowPositionScanConfig> {
        self.glm_contexts
            .get(&row_source_slot)
            .map(|ctx| ctx.scan_config.clone())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn register_lake_glm(&mut self, row_source_slot: SlotId, info: LakeGlmScanInfo) {
        self.lake_glm_contexts.insert(row_source_slot, info);
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_glm_info(&self, row_source_slot: SlotId) -> Option<&LakeGlmScanInfo> {
        self.lake_glm_contexts.get(&row_source_slot)
    }

    pub(crate) fn push_pending_runtime_filter(
        &mut self,
        filter_id: i32,
        build_be_number: i32,
        data: Vec<u8>,
        build_data_type: Option<DataType>,
    ) {
        self.pending_runtime_filters.push(PendingRuntimeFilter {
            filter_id,
            build_be_number,
            data,
            build_data_type,
        });
    }

    pub(crate) fn drain_pending_runtime_filters(&mut self) -> Vec<PendingRuntimeFilter> {
        std::mem::take(&mut self.pending_runtime_filters)
    }

    pub(crate) fn mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.mem_tracker)
    }

    pub(crate) fn set_cache_options(&mut self, options: CacheOptions) -> Result<(), String> {
        if let Some(existing) = self.cache_options.as_ref() {
            if existing != &options {
                return Err("cache options mismatch for query".to_string());
            }
            return Ok(());
        }
        self.cache_options = Some(options);
        Ok(())
    }

    pub(crate) fn cache_options(&self) -> Option<CacheOptions> {
        self.cache_options.clone()
    }

    pub(crate) fn set_lake_tablet_paths(&mut self, cache_key: String, paths: HashMap<i64, String>) {
        self.lake_tablet_paths.insert(cache_key, paths);
    }

    pub(crate) fn lake_tablet_paths(&self, cache_key: &str) -> Option<HashMap<i64, String>> {
        self.lake_tablet_paths.get(cache_key).cloned()
    }
}

#[cfg(feature = "compat")]
struct IncrementalScanNodeHandle {
    scan: ScanNode,
    dispatch: Arc<ScanDispatchState>,
    update_mu: Mutex<()>,
}

#[cfg(feature = "compat")]
impl IncrementalScanNodeHandle {
    fn new(scan: ScanNode, dispatch: Arc<ScanDispatchState>) -> Self {
        Self {
            scan,
            dispatch,
            update_mu: Mutex::new(()),
        }
    }

    fn append_scan_ranges(
        &self,
        scan_ranges: &[internal_service::TScanRangeParams],
    ) -> Result<(), String> {
        let _guard = self.update_mu.lock().expect("incremental scan handle lock");
        let ranges = incremental_scan_ranges_from_thrift(scan_ranges)?;
        let morsels = self.scan.build_incremental_morsels(&ranges)?;
        self.dispatch
            .append_morsels(morsels.morsels, morsels.has_more)
    }
}

#[cfg(feature = "compat")]
fn incremental_scan_ranges_from_thrift(
    scan_ranges: &[internal_service::TScanRangeParams],
) -> Result<Vec<IncrementalScanRange>, String> {
    scan_ranges
        .iter()
        .map(incremental_scan_range_from_thrift)
        .collect()
}

#[cfg(feature = "compat")]
fn incremental_scan_range_from_thrift(
    params: &internal_service::TScanRangeParams,
) -> Result<IncrementalScanRange, String> {
    if params.empty.unwrap_or(false) {
        return Ok(IncrementalScanRange::Empty {
            has_more: params.has_more,
        });
    }
    let Some(hdfs_range) = params.scan_range.hdfs_scan_range.as_ref() else {
        return Ok(IncrementalScanRange::Other {
            has_more: params.has_more,
        });
    };
    Ok(IncrementalScanRange::Hdfs {
        has_more: params.has_more,
        range: IncrementalHdfsScanRange {
            file_format: hdfs_range
                .file_format
                .as_ref()
                .map(hdfs_file_format_from_thrift),
            full_path: hdfs_range.full_path.clone(),
            relative_path: hdfs_range.relative_path.clone(),
            table_id: hdfs_range.table_id,
            file_length: hdfs_range.file_length.unwrap_or(0),
            offset: hdfs_range.offset.unwrap_or(0),
            length: hdfs_range.length.unwrap_or(0),
            first_row_id: hdfs_range.first_row_id,
            ivm_change_op: incremental_change_op_from_thrift(hdfs_range)?,
            external_datacache: external_datacache_options_from_thrift(hdfs_range),
        },
    })
}

#[cfg(feature = "compat")]
fn hdfs_file_format_from_thrift(format: &descriptors::THdfsFileFormat) -> HdfsScanFileFormat {
    match *format {
        descriptors::THdfsFileFormat::PARQUET => HdfsScanFileFormat::Parquet,
        descriptors::THdfsFileFormat::ORC => HdfsScanFileFormat::Orc,
        _ => HdfsScanFileFormat::Other,
    }
}

#[cfg(feature = "compat")]
fn incremental_change_op_from_thrift(
    hdfs_range: &crate::thrift::plan_nodes::THdfsScanRange,
) -> Result<Option<i8>, String> {
    let Some(extended_columns) = hdfs_range.extended_columns.as_ref() else {
        return Ok(None);
    };
    if extended_columns.is_empty() {
        return Ok(None);
    }
    if extended_columns.len() != 1 {
        return Err(format!(
            "incremental hdfs scan range expects exactly one __change_op extended column, got {}",
            extended_columns.len()
        ));
    }
    let slot_id = *extended_columns
        .keys()
        .next()
        .expect("non-empty extended_columns has a first key");
    let slot = SlotId::try_from(slot_id).map_err(|e| {
        format!("incremental hdfs scan range has invalid __change_op slot_id={slot_id}: {e}")
    })?;
    crate::runtime::change_op::extract_change_op_from_hdfs_range_extended_columns(
        -1,
        hdfs_range,
        Some(slot),
    )
}

#[cfg(feature = "compat")]
fn external_datacache_options_from_thrift(
    hdfs_range: &crate::thrift::plan_nodes::THdfsScanRange,
) -> Option<ExternalDataCacheRangeOptions> {
    let candidate_node = hdfs_range
        .candidate_node
        .as_ref()
        .map(|node| node.trim())
        .filter(|node| !node.is_empty())
        .map(|node| node.to_string());
    let options = ExternalDataCacheRangeOptions {
        modification_time: hdfs_range.modification_time,
        enable_populate_datacache: hdfs_range
            .datacache_options
            .as_ref()
            .and_then(|opts| opts.enable_populate_datacache),
        datacache_priority: hdfs_range
            .datacache_options
            .as_ref()
            .and_then(|opts| opts.priority),
        candidate_node,
    };
    if options.modification_time.is_some()
        || options.enable_populate_datacache.is_some()
        || options.datacache_priority.is_some()
        || options.candidate_node.is_some()
    {
        Some(options)
    } else {
        None
    }
}

#[derive(Default)]
struct QueryContextManagerInner {
    active: HashMap<QueryId, QueryContext>,
    second_chance: HashMap<QueryId, QueryContext>,
    finst_to_query: HashMap<UniqueId, QueryId>,
    fragment_completions: HashMap<UniqueId, Weak<FragmentCompletion>>,
    #[cfg(feature = "compat")]
    incremental_scan_nodes: HashMap<UniqueId, HashMap<i32, Arc<IncrementalScanNodeHandle>>>,
    #[cfg(feature = "compat")]
    pending_incremental_scan_ranges:
        HashMap<UniqueId, HashMap<i32, Vec<internal_service::TScanRangeParams>>>,
}

pub(crate) struct QueryContextManager {
    inner: Mutex<QueryContextManagerInner>,
    stopped: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FragmentFinishReportDecision {
    pub(crate) include_runtime_filter_profile: bool,
    remove_runtime_filter_lifecycle_after_report: bool,
}

impl QueryContextManager {
    fn new() -> Arc<Self> {
        let manager = Arc::new(Self {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        });
        let mgr = Arc::clone(&manager);
        thread::spawn(move || mgr.clean_loop());
        manager
    }

    fn clean_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Relaxed) {
            self.clean_expired();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn clean_expired(&self) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let mut to_remove = Vec::new();
        for (qid, ctx) in &guard.second_chance {
            if ctx.has_no_active_instances() && ctx.is_delivery_expired() {
                to_remove.push(*qid);
            }
        }
        for qid in to_remove {
            guard.second_chance.remove(&qid);
            remove_runtime_filter_lifecycle(qid);
        }
    }

    pub(crate) fn get_or_register(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
        )
    }

    pub(crate) fn ensure_context(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
        )
    }

    fn get_or_register_internal(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
        increment: bool,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            if increment {
                ctx.increment_num_fragments();
            }
            return Ok(());
        }
        if let Some(mut ctx) = guard.second_chance.remove(&query_id) {
            if increment {
                ctx.increment_num_fragments();
            }
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let mut ctx = QueryContext::new(query_id, delivery_expire, query_expire);
        if increment {
            ctx.increment_num_fragments();
        }
        guard.active.insert(query_id, ctx);
        Ok(())
    }

    pub(crate) fn with_context_mut<T, F>(&self, query_id: QueryId, f: F) -> Result<T, String>
    where
        F: FnOnce(&mut QueryContext) -> Result<T, String>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = guard
            .active
            .get_mut(&query_id)
            .ok_or_else(|| "QueryContext not found".to_string())?;
        f(ctx)
    }

    pub(crate) fn set_cache_options(
        &self,
        query_id: QueryId,
        options: CacheOptions,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| ctx.set_cache_options(options))
    }

    pub(crate) fn cache_options(&self, query_id: QueryId) -> Option<CacheOptions> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .and_then(|ctx| ctx.cache_options())
            .or_else(|| {
                guard
                    .second_chance
                    .get(&query_id)
                    .and_then(|ctx| ctx.cache_options())
            })
    }

    pub(crate) fn set_lake_tablet_paths(
        &self,
        query_id: QueryId,
        cache_key: String,
        paths: HashMap<i64, String>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.set_lake_tablet_paths(cache_key, paths);
            Ok(())
        })
    }

    pub(crate) fn lake_tablet_paths(
        &self,
        query_id: QueryId,
        cache_key: &str,
    ) -> Option<HashMap<i64, String>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.lake_tablet_paths(cache_key))
    }

    pub(crate) fn update_exchange_sender_counts(
        &self,
        query_id: QueryId,
        counts: HashMap<i32, usize>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.update_exchange_senders(counts);
            Ok(())
        })
    }

    pub(crate) fn register_row_pos_descs(
        &self,
        query_id: QueryId,
        descs: HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.set_row_pos_descs(descs);
            Ok(())
        })
    }

    pub(crate) fn register_glm_scan_ranges(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        scan_cfg: RowPositionScanConfig,
        ranges: Vec<FileScanRange>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_glm_scan_ranges(row_source_slot, scan_cfg, ranges);
            Ok(())
        })
    }

    pub(crate) fn row_pos_desc(
        &self,
        query_id: QueryId,
        tuple_id: i32,
    ) -> Option<RowPositionDescriptor> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.row_pos_desc(tuple_id))
    }

    pub(crate) fn glm_scan_range(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        scan_range_id: i32,
    ) -> Option<FileScanRange> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.glm_scan_range(row_source_slot, scan_range_id))
    }

    pub(crate) fn glm_scan_config(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
    ) -> Option<RowPositionScanConfig> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.glm_scan_config(row_source_slot))
    }

    #[cfg(feature = "compat")]
    pub(crate) fn register_lake_glm(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        info: LakeGlmScanInfo,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_lake_glm(row_source_slot, info);
            Ok(())
        })
    }

    #[cfg(feature = "compat")]
    pub(crate) fn lake_glm_info(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
    ) -> Option<LakeGlmScanInfo> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.lake_glm_info(row_source_slot))
            .cloned()
    }

    pub(crate) fn exchange_sender_count(&self, query_id: QueryId, node_id: i32) -> Option<usize> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.exchange_sender_count(node_id))
    }

    pub(crate) fn query_mem_tracker(&self, query_id: QueryId) -> Option<Arc<MemTracker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.mem_tracker())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn desc_tbl(&self, query_id: QueryId) -> Option<descriptors::TDescriptorTable> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.desc_tbl.clone())
    }

    pub(crate) fn descriptor_snapshot(&self, query_id: QueryId) -> Option<Arc<DescriptorSnapshot>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.desc_snapshot.clone())
    }

    pub(crate) fn set_runtime_filter_hub(
        &self,
        query_id: QueryId,
        hub: Arc<RuntimeFilterHub>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.set_runtime_filter_hub(hub);
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_hub(hub);
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    pub(crate) fn get_runtime_filter_hub(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterHub>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_hub())
    }

    pub(crate) fn set_runtime_filter_params(
        &self,
        query_id: QueryId,
        params: RuntimeFilterParams,
    ) -> Result<(), String> {
        let pending = self.with_context_mut(query_id, |ctx| {
            ctx.set_runtime_filter_params(params);
            Ok(ctx.drain_pending_runtime_filters())
        })?;
        if let Some(worker) = self.get_or_create_runtime_filter_worker(query_id) {
            for item in pending {
                let _ = worker.receive_partial(
                    item.filter_id,
                    &item.data,
                    item.build_be_number,
                    item.build_data_type,
                );
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn get_runtime_filter_params(
        &self,
        query_id: QueryId,
    ) -> Option<RuntimeFilterParams> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_params())
    }

    #[allow(dead_code)]
    pub(crate) fn set_runtime_filter_worker(
        &self,
        query_id: QueryId,
        worker: Arc<RuntimeFilterWorker>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker);
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker);
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    #[allow(dead_code)]
    pub(crate) fn get_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterWorker>> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .and_then(|ctx| ctx.runtime_filter_worker())
    }

    pub(crate) fn get_or_create_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Option<Arc<RuntimeFilterWorker>> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx
        } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx
        } else {
            return None;
        };
        if let Some(worker) = ctx.runtime_filter_worker() {
            return Some(worker);
        }
        let params = ctx.runtime_filter_worker_params()?;
        let hub = if let Some(hub) = ctx.runtime_filter_hub() {
            hub
        } else {
            let hub = Arc::new(RuntimeFilterHub::new_for_query(
                DependencyManager::new(),
                query_id,
            ));
            ctx.set_runtime_filter_hub(Arc::clone(&hub));
            hub
        };
        let worker = Arc::new(RuntimeFilterWorker::new(query_id, params, hub));
        ctx.set_runtime_filter_worker(Arc::clone(&worker));
        Some(worker)
    }

    pub(crate) fn enqueue_pending_runtime_filter(
        &self,
        query_id: QueryId,
        filter_id: i32,
        build_be_number: i32,
        data: Vec<u8>,
        build_data_type: Option<DataType>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.push_pending_runtime_filter(filter_id, build_be_number, data, build_data_type);
            Ok(())
        })
    }

    #[cfg(feature = "compat")]
    pub(crate) fn register_incremental_scan_node(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        scan: ScanNode,
        dispatch: Arc<ScanDispatchState>,
    ) -> Result<(), String> {
        let handle = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if !guard.finst_to_query.contains_key(&finst_id) {
                return Ok(());
            }
            let node_map = guard.incremental_scan_nodes.entry(finst_id).or_default();
            if let Some(existing) = node_map.get(&node_id) {
                Arc::clone(existing)
            } else {
                let handle = Arc::new(IncrementalScanNodeHandle::new(scan, dispatch));
                node_map.insert(node_id, Arc::clone(&handle));
                handle
            }
        };

        let pending = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            guard
                .pending_incremental_scan_ranges
                .get_mut(&finst_id)
                .and_then(|node_map| node_map.remove(&node_id))
        };
        if let Some(scan_ranges) = pending {
            handle.append_scan_ranges(&scan_ranges)?;
        }
        Ok(())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn append_incremental_scan_ranges(
        &self,
        finst_id: UniqueId,
        node_id: i32,
        mut scan_ranges: Vec<internal_service::TScanRangeParams>,
    ) -> Result<(), String> {
        if scan_ranges.is_empty() {
            return Ok(());
        }
        let handle = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(handle) = guard
                .incremental_scan_nodes
                .get(&finst_id)
                .and_then(|node_map| node_map.get(&node_id))
            {
                Some(Arc::clone(handle))
            } else if guard.finst_to_query.contains_key(&finst_id) {
                guard
                    .pending_incremental_scan_ranges
                    .entry(finst_id)
                    .or_default()
                    .entry(node_id)
                    .or_default()
                    .append(&mut scan_ranges);
                None
            } else {
                None
            }
        };
        if let Some(handle) = handle {
            handle.append_scan_ranges(&scan_ranges)?;
        }
        Ok(())
    }

    pub(crate) fn register_finst(&self, finst_id: UniqueId, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.insert(finst_id, query_id);
    }

    pub(crate) fn query_id_by_finst(&self, finst_id: UniqueId) -> Option<QueryId> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.get(&finst_id).copied()
    }

    pub(crate) fn unregister_finst(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.finst_to_query.remove(&finst_id);
        guard.fragment_completions.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.incremental_scan_nodes.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.pending_incremental_scan_ranges.remove(&finst_id);
    }

    pub(crate) fn register_fragment_completion(
        &self,
        finst_id: UniqueId,
        completion: Arc<FragmentCompletion>,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .fragment_completions
            .insert(finst_id, Arc::downgrade(&completion));
    }

    pub(crate) fn unregister_fragment_completion(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.fragment_completions.remove(&finst_id);
    }

    pub(crate) fn get_query_timeout_by_finst(&self, finst_id: UniqueId) -> Option<Duration> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let query_id = guard.finst_to_query.get(&finst_id).copied()?;
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|ctx| ctx.query_expire)
    }

    pub(crate) fn is_query_canceled(&self, query_id: QueryId) -> bool {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .map(|ctx| ctx.cancelled_by_fe)
            .or_else(|| {
                guard
                    .second_chance
                    .get(&query_id)
                    .map(|ctx| ctx.cancelled_by_fe)
            })
            .unwrap_or(false)
    }

    #[allow(dead_code)]
    pub(crate) fn abort_query(&self, query_id: QueryId) -> Vec<UniqueId> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.cancelled_by_fe = true;
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.cancelled_by_fe = true;
        }
        guard
            .finst_to_query
            .iter()
            .filter_map(|(finst_id, qid)| (*qid == query_id).then_some(*finst_id))
            .collect()
    }

    pub(crate) fn cancel_query(&self, query_id: QueryId, err: String) -> Vec<UniqueId> {
        let (finsts, completions) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
            }
            if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
            }

            let mut finsts = Vec::new();
            let mut completions = Vec::new();
            let mut stale = Vec::new();
            for (finst_id, qid) in guard.finst_to_query.iter() {
                if *qid != query_id {
                    continue;
                }
                finsts.push(*finst_id);
                if let Some(weak) = guard.fragment_completions.get(finst_id) {
                    if let Some(completion) = weak.upgrade() {
                        completions.push(completion);
                    } else {
                        stale.push(*finst_id);
                    }
                }
            }
            for finst_id in stale {
                guard.fragment_completions.remove(&finst_id);
            }
            (finsts, completions)
        };

        for completion in completions {
            completion.abort_from_query(err.clone());
        }
        finsts
    }

    /// A sender's exchange RPC failed. Map the finst to its query and cancel
    /// the whole query so blocked receivers abort instead of timing out.
    pub(crate) fn propagate_sender_error(&self, finst_id: UniqueId, err: String) -> Vec<UniqueId> {
        match self.query_id_by_finst(finst_id) {
            Some(qid) => {
                let finsts = self.cancel_query(qid, format!("exchange send failed: {err}"));
                for id in &finsts {
                    crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
                }
                finsts
            }
            None => {
                crate::runtime::exchange::cancel_fragment(finst_id.hi, finst_id.lo);
                vec![finst_id]
            }
        }
    }

    pub(crate) fn finish_fragment(&self, query_id: QueryId) {
        let decision = self.finish_fragment_internal(query_id);
        if decision.remove_runtime_filter_lifecycle_after_report {
            remove_runtime_filter_lifecycle(query_id);
        }
    }

    pub(crate) fn finish_fragment_for_report(
        &self,
        query_id: QueryId,
    ) -> FragmentFinishReportDecision {
        self.finish_fragment_internal(query_id)
    }

    pub(crate) fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: FragmentFinishReportDecision,
    ) {
        if decision.remove_runtime_filter_lifecycle_after_report {
            remove_runtime_filter_lifecycle(query_id);
        }
    }

    fn finish_fragment_internal(&self, query_id: QueryId) -> FragmentFinishReportDecision {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(mut ctx) = guard.active.remove(&query_id) else {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: false,
            };
        };
        let no_active_fragments = ctx.count_down_fragments();
        if !no_active_fragments {
            guard.active.insert(query_id, ctx);
            return FragmentFinishReportDecision::default();
        }
        if ctx.is_dead() {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: true,
            };
        }
        ctx.extend_delivery_lifetime();
        guard.second_chance.insert(query_id, ctx);
        FragmentFinishReportDecision {
            include_runtime_filter_profile: true,
            remove_runtime_filter_lifecycle_after_report: false,
        }
    }
}

fn remove_runtime_filter_lifecycle(query_id: QueryId) {
    RuntimeFilterLifecycleRegistry::global()
        .remove_query(QueryKey::from_hi_lo(query_id.hi, query_id.lo));
}

static QUERY_CONTEXT_MANAGER: OnceLock<Arc<QueryContextManager>> = OnceLock::new();

pub(crate) fn query_context_manager() -> Arc<QueryContextManager> {
    QUERY_CONTEXT_MANAGER
        .get_or_init(QueryContextManager::new)
        .clone()
}

#[cfg(test)]
mod sender_error_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use crate::common::types::UniqueId;
    use crate::runtime::exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state};

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn mapped_finst_cancels_all_query_finsts_and_receivers() {
        let mgr = test_manager();
        let qid = QueryId { hi: 11, lo: 22 };
        let finst_a = UniqueId { hi: 101, lo: 201 };
        let finst_b = UniqueId { hi: 102, lo: 202 };
        let key_a = ExchangeKey {
            finst_id_hi: finst_a.hi,
            finst_id_lo: finst_a.lo,
            node_id: 301,
        };
        let key_b = ExchangeKey {
            finst_id_hi: finst_b.hi,
            finst_id_lo: finst_b.lo,
            node_id: 302,
        };

        mgr.get_or_register(qid, false, Duration::from_secs(1), Duration::from_secs(5))
            .expect("query context must be created");
        mgr.register_finst(finst_a, qid);
        mgr.register_finst(finst_b, qid);
        set_expected_senders(key_a, 1);
        set_expected_senders(key_b, 1);

        assert!(snapshot_receiver_state(key_a).is_some());
        assert!(snapshot_receiver_state(key_b).is_some());

        let mut finsts = mgr.propagate_sender_error(finst_a, "connection refused".into());
        finsts.sort_by_key(|id| (id.hi, id.lo));

        assert_eq!(finsts, vec![finst_a, finst_b]);
        assert!(mgr.is_query_canceled(qid));
        assert!(snapshot_receiver_state(key_a).is_none());
        assert!(snapshot_receiver_state(key_b).is_none());
    }

    #[test]
    fn unmapped_finst_cancels_its_own_receiver_only() {
        let mgr = test_manager();
        let finst = UniqueId { hi: 201, lo: 202 };
        let key = ExchangeKey {
            finst_id_hi: finst.hi,
            finst_id_lo: finst.lo,
            node_id: 401,
        };

        set_expected_senders(key, 1);
        assert!(snapshot_receiver_state(key).is_some());

        let finsts = mgr.propagate_sender_error(finst, "broken pipe".into());

        assert_eq!(finsts, vec![finst]);
        assert!(snapshot_receiver_state(key).is_none());
    }
}

#[cfg(test)]
mod runtime_filter_lifecycle_cleanup_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use super::{
        FragmentFinishReportDecision, QueryContext, QueryContextManager, QueryContextManagerInner,
        QueryId,
    };
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn finish_fragment_removes_runtime_filter_lifecycle_when_query_is_dead() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_101,
            lo: 4_102,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("query context must be created");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(1);
        }

        mgr.finish_fragment(query_id);

        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn finish_fragment_for_report_claims_runtime_filter_export_once_before_cleanup() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_151,
            lo: 4_152,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("first query context fragment must be created");
        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("second query context fragment must be created");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(2);
        }

        let first = mgr.finish_fragment_for_report(query_id);
        assert_eq!(first, FragmentFinishReportDecision::default());
        mgr.cleanup_after_fragment_report(query_id, first);
        assert!(registry.snapshot(query_key).is_some());

        let second = mgr.finish_fragment_for_report(query_id);
        assert!(second.include_runtime_filter_profile);
        assert!(second.remove_runtime_filter_lifecycle_after_report);
        assert!(registry.snapshot(query_key).is_some());

        mgr.cleanup_after_fragment_report(query_id, second);

        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn clean_expired_removes_runtime_filter_lifecycle_for_second_chance_query() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_201,
            lo: 4_202,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        registry.recorder(query_key).planned(7);

        let mut ctx = QueryContext::new(query_id, Duration::from_millis(1), Duration::from_secs(5));
        ctx.delivery_deadline = Instant::now() - Duration::from_millis(1);
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard.second_chance.insert(query_id, ctx);
        }

        mgr.clean_expired();

        assert!(registry.snapshot(query_key).is_none());
    }
}

#[cfg(feature = "compat")]
pub(crate) fn observe_total_fragments(
    ctx: &mut QueryContext,
    exec_params: &internal_service::TPlanFragmentExecParams,
) {
    if let Some(n) = exec_params.instances_number {
        let n = n.max(0) as usize;
        ctx.total_fragments = Some(ctx.total_fragments.map_or(n, |cur| cur.max(n)));
    }
}

#[cfg(feature = "compat")]
pub(crate) fn desc_tbl_is_cached(desc: &descriptors::TDescriptorTable) -> bool {
    desc.is_cached.unwrap_or(false)
}

#[cfg(feature = "compat")]
pub(crate) fn is_desc_tbl_effectively_empty(desc: &descriptors::TDescriptorTable) -> bool {
    let has_tuple = !desc.tuple_descriptors.is_empty();
    let has_table = desc
        .table_descriptors
        .as_ref()
        .is_some_and(|v| !v.is_empty());
    let has_slot = desc
        .slot_descriptors
        .as_ref()
        .is_some_and(|v| !v.is_empty());
    !(has_tuple || has_table || has_slot)
}

#[cfg(feature = "compat")]
pub(crate) fn resolve_desc_tbl_for_instance(
    mgr: &QueryContextManager,
    query_id: QueryId,
    incoming: Option<&descriptors::TDescriptorTable>,
    fallback: Option<&descriptors::TDescriptorTable>,
) -> Result<Option<descriptors::TDescriptorTable>, String> {
    mgr.with_context_mut(query_id, |ctx| {
        if let Some(desc) = incoming {
            if desc_tbl_is_cached(desc) {
                if ctx.desc_snapshot.is_none() {
                    let existing = ctx.desc_tbl.as_ref().ok_or_else(|| {
                        "Query terminates prematurely (missing desc_tbl)".to_string()
                    })?;
                    ctx.desc_snapshot = Some(Arc::new(descriptor_snapshot_from_thrift(existing)?));
                }
                return ctx
                    .desc_tbl
                    .clone()
                    .ok_or_else(|| "Query terminates prematurely (missing desc_tbl)".to_string())
                    .map(Some);
            }
            if !is_desc_tbl_effectively_empty(desc) {
                let snapshot = descriptor_snapshot_from_thrift(desc)?;
                ctx.desc_tbl = Some(desc.clone());
                ctx.desc_snapshot = Some(Arc::new(snapshot));
                return Ok(Some(desc.clone()));
            }
        }
        if let Some(desc) = fallback
            && !is_desc_tbl_effectively_empty(desc)
        {
            let snapshot = descriptor_snapshot_from_thrift(desc)?;
            ctx.desc_tbl = Some(desc.clone());
            ctx.desc_snapshot = Some(Arc::new(snapshot));
            return Ok(Some(desc.clone()));
        }
        Ok(ctx.desc_tbl.clone())
    })
}

#[cfg(all(test, feature = "compat"))]
mod descriptor_snapshot_tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{
        QueryContextManager, QueryContextManagerInner, QueryId, is_desc_tbl_effectively_empty,
        resolve_desc_tbl_for_instance,
    };
    use crate::common::ids::SlotId;
    use crate::thrift::descriptors;
    use crate::thrift::types::TPrimitiveType;

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    fn int_slot_desc_tbl(tuple_id: i32, slot_id: i32) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(vec![descriptors::TSlotDescriptor {
                id: Some(slot_id),
                parent: Some(tuple_id),
                slot_type: Some(crate::types::arrow_thrift::thrift_type_desc_from_primitive(
                    TPrimitiveType::INT,
                )),
                column_pos: None,
                byte_offset: None,
                null_indicator_byte: None,
                null_indicator_bit: None,
                col_name: Some("c1".to_string()),
                slot_idx: None,
                is_materialized: Some(true),
                is_output_column: Some(true),
                is_nullable: Some(true),
                col_unique_id: Some(2001),
                col_physical_name: None,
                is_virtual_column: None,
            }]),
            vec![descriptors::TTupleDescriptor::new(
                Some(tuple_id),
                None,
                None,
                Some(100),
                None,
            )],
            None::<Vec<descriptors::TTableDescriptor>>,
            None,
        )
    }

    fn cached_desc_tbl() -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            None::<Vec<descriptors::TSlotDescriptor>>,
            vec![],
            None::<Vec<descriptors::TTableDescriptor>>,
            Some(true),
        )
    }

    #[test]
    fn resolve_desc_tbl_caches_descriptor_snapshot() {
        let mgr = test_manager();
        let query_id = QueryId { hi: 11, lo: 22 };
        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("query context must be created");
        let desc = int_slot_desc_tbl(1, 7);

        let resolved = resolve_desc_tbl_for_instance(&mgr, query_id, Some(&desc), None)
            .expect("resolve")
            .expect("desc");

        assert!(!is_desc_tbl_effectively_empty(&resolved));
        let snapshot = mgr.descriptor_snapshot(query_id).expect("snapshot");
        assert!(snapshot.slot(1, SlotId::new(7)).is_some());
        assert_eq!(snapshot.table_id_for_tuple(1), Some(100));
    }

    #[test]
    fn cached_descriptor_rebuilds_missing_snapshot_from_existing_desc_tbl() {
        let mgr = test_manager();
        let query_id = QueryId { hi: 33, lo: 44 };
        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("query context must be created");

        let desc = int_slot_desc_tbl(2, 9);
        mgr.with_context_mut(query_id, |ctx| {
            ctx.desc_tbl = Some(desc);
            ctx.desc_snapshot = None;
            Ok(())
        })
        .expect("seed descriptor without snapshot");

        resolve_desc_tbl_for_instance(&mgr, query_id, Some(&cached_desc_tbl()), None)
            .expect("resolve cached descriptor")
            .expect("cached desc");

        let snapshot = mgr.descriptor_snapshot(query_id).expect("snapshot");
        assert!(snapshot.slot(2, SlotId::new(9)).is_some());
    }
}

#[cfg(all(test, feature = "compat"))]
mod incremental_scan_range_wire_tests {
    use std::collections::BTreeMap;

    use super::incremental_scan_ranges_from_thrift;
    use crate::exec::node::scan::{HdfsScanFileFormat, IncrementalScanRange};
    use crate::thrift::descriptors;
    use crate::thrift::internal_service;
    use crate::thrift::plan_nodes;
    use crate::thrift::{exprs, types};

    fn make_hdfs_range(
        extended_columns: Option<
            BTreeMap<crate::thrift::types::TSlotId, crate::thrift::exprs::TExpr>,
        >,
    ) -> internal_service::TScanRangeParams {
        let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
            None::<String>,
            Some(0_i64),
            Some(100_i64),
            None::<i64>,
            Some(256_i64),
            Some(descriptors::THdfsFileFormat::PARQUET),
            None::<descriptors::TTextFileDesc>,
            Some("s3://bucket/path/file.parquet".to_string()),
            None::<Vec<String>>,
            None::<bool>,
            None::<Vec<plan_nodes::TIcebergDeleteFile>>,
            None::<i64>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<crate::thrift::data_cache::TDataCacheOptions>,
            None::<Vec<crate::thrift::types::TSlotId>>,
            None::<bool>,
            None::<std::collections::BTreeMap<String, String>>,
            None::<Vec<crate::thrift::types::TSlotId>>,
            None::<bool>,
            None::<String>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<plan_nodes::TPaimonDeletionFile>,
            extended_columns,
            None::<descriptors::THdfsPartition>,
            None::<crate::thrift::types::TTableId>,
            None::<plan_nodes::TDeletionVectorDescriptor>,
            None::<String>,
            None::<i64>,
            None::<bool>,
            None::<std::collections::BTreeMap<i32, crate::thrift::exprs::TExprMinMaxValue>>,
            None::<i32>,
            None::<i64>,
            None::<i64>,
            None::<Vec<i64>>,
        );
        internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                None::<plan_nodes::TInternalScanRange>,
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                Some(hdfs_scan_range),
                None::<plan_nodes::TBinlogScanRange>,
                None::<plan_nodes::TBenchmarkScanRange>,
            ),
            None::<i32>,
            Some(false),
            None::<bool>,
        )
    }

    fn int_expr(value: i64) -> exprs::TExpr {
        exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::INT_LITERAL,
            type_: crate::types::arrow_thrift::thrift_type_desc_from_primitive(
                types::TPrimitiveType::BIGINT,
            ),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: Some(exprs::TIntLiteral { value }),
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }])
    }

    #[test]
    fn incremental_hdfs_range_wire_decodes_domain_payload() {
        let ranges = incremental_scan_ranges_from_thrift(&[make_hdfs_range(Some(BTreeMap::from(
            [(9, int_expr(-1))],
        )))])
        .expect("incremental range");

        let IncrementalScanRange::Hdfs { range, .. } = &ranges[0] else {
            panic!("expected hdfs range");
        };
        assert_eq!(range.file_format, Some(HdfsScanFileFormat::Parquet));
        assert_eq!(
            range.full_path.as_deref(),
            Some("s3://bucket/path/file.parquet")
        );
        assert_eq!(range.file_length, 256);
        assert_eq!(range.offset, 0);
        assert_eq!(range.length, 100);
        assert_eq!(range.ivm_change_op, Some(-1));
    }

    #[test]
    fn incremental_hdfs_range_wire_rejects_bad_change_op() {
        let err =
            incremental_scan_ranges_from_thrift(&[make_hdfs_range(Some(BTreeMap::from([(
                9,
                int_expr(0),
            )])))])
            .expect_err("invalid change op must fail at wire adapter");

        assert!(err.contains("__change_op"));
        assert!(err.contains("invalid value"));
    }
}
