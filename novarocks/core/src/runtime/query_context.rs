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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::cache::CacheOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
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
use crate::runtime::lookup::GlobalLateMaterializationContext;
use crate::runtime::mem_tracker::{self, MemTracker};
pub(crate) use crate::runtime::query_options::query_expire_durations;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_filter_observability::{
    QueryKey, RegistryRuntimeFilterEventSink, RuntimeFilterLifecycleRegistry,
};
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::runtime::runtime_filter_worker::{RuntimeFilterWorker, RuntimeFilterWorkerParams};
use crate::runtime_filter::service::RuntimeFilterService;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct QueryId {
    pub(crate) hi: i64,
    pub(crate) lo: i64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct StarRocksQueryGeneration(NonZeroU64);

impl StarRocksQueryGeneration {
    pub(crate) fn new(generation: u64) -> Result<Self, String> {
        NonZeroU64::new(generation)
            .map(Self)
            .ok_or_else(|| "StarRocks query generation must be non-zero".to_string())
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum QueryExecutionGeneration {
    Native,
    StarRocks(StarRocksQueryGeneration),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct QueryExecutionKey {
    query_id: QueryId,
    generation: QueryExecutionGeneration,
}

impl QueryExecutionKey {
    pub(crate) const fn native(query_id: QueryId) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::Native,
        }
    }

    pub(crate) const fn starrocks(query_id: QueryId, generation: StarRocksQueryGeneration) -> Self {
        Self {
            query_id,
            generation: QueryExecutionGeneration::StarRocks(generation),
        }
    }

    pub(crate) const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn starrocks_generation(self) -> Option<StarRocksQueryGeneration> {
        match self.generation {
            QueryExecutionGeneration::Native => None,
            QueryExecutionGeneration::StarRocks(generation) => Some(generation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryContextGeneration {
    Native,
    StarRocksUnbound,
    StarRocks(StarRocksQueryGeneration),
}

pub(crate) struct QueryCleanupLease {
    release: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl QueryCleanupLease {
    pub(crate) fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self {
            release: Some(Box::new(release)),
        }
    }

    pub(crate) fn release(mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

impl Drop for QueryCleanupLease {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
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
    execution_generation: QueryContextGeneration,
    pub(crate) cache_options: Option<CacheOptions>,
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
    legacy_runtime_filter_execution: LegacyRuntimeFilterExecutionClaim,
    runtime_filter_hub: Option<Arc<RuntimeFilterHub>>,
    runtime_filter_params: Option<RuntimeFilterParams>,
    runtime_filter_worker_params: Option<RuntimeFilterWorkerParams>,
    runtime_filter_worker: Option<Arc<RuntimeFilterWorker>>,
    pending_runtime_filters: Vec<PendingRuntimeFilter>,
    runtime_filter_service: Arc<RuntimeFilterService>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
    pub(crate) glm_contexts: HashMap<SlotId, GlobalLateMaterializationContext>,
    #[cfg(feature = "compat")]
    pub(crate) lake_glm_contexts: HashMap<SlotId, LakeGlmScanInfo>,
    pub(crate) lake_tablet_paths: HashMap<String, HashMap<i64, String>>,
    pub(crate) mem_tracker: Arc<MemTracker>,
    cleanup_leases: Vec<QueryCleanupLease>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LegacyRuntimeFilterExecutionClaim {
    #[default]
    Unclaimed,
    NativeDisabled,
    #[cfg(feature = "compat")]
    Compat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LookupFetcherLifecycle {
    Exact(usize),
    Unknown,
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
        Self::new_with_generation(
            query_id,
            QueryContextGeneration::Native,
            delivery_expire,
            query_expire,
        )
    }

    fn new_with_generation(
        query_id: QueryId,
        execution_generation: QueryContextGeneration,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Self {
        let now = Instant::now();
        let process = mem_tracker::process_mem_tracker();
        let query_label = format!("query_{:x}_{:x}", query_id.hi, query_id.lo);
        let mem_tracker = MemTracker::new_child(query_label, &process);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let event_sink = Arc::new(RegistryRuntimeFilterEventSink::new(
            RuntimeFilterLifecycleRegistry::global(),
            query_key,
        ));
        let runtime_filter_service = Arc::new(RuntimeFilterService::new_for_query(
            UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            },
            event_sink,
            &mem_tracker,
        ));
        Self {
            query_id,
            execution_generation,
            cache_options: None,
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
            legacy_runtime_filter_execution: LegacyRuntimeFilterExecutionClaim::Unclaimed,
            runtime_filter_hub: None,
            runtime_filter_params: None,
            runtime_filter_worker_params: None,
            runtime_filter_worker: None,
            pending_runtime_filters: Vec::new(),
            runtime_filter_service,
            row_pos_descs: HashMap::new(),
            lookup_fetchers: HashMap::new(),
            glm_contexts: HashMap::new(),
            #[cfg(feature = "compat")]
            lake_glm_contexts: HashMap::new(),
            lake_tablet_paths: HashMap::new(),
            mem_tracker,
            cleanup_leases: Vec::new(),
        }
    }

    fn matches_execution(&self, key: QueryExecutionKey) -> bool {
        self.query_id == key.query_id
            && matches!(
                (self.execution_generation, key.generation),
                (
                    QueryContextGeneration::Native,
                    QueryExecutionGeneration::Native
                ) | (
                    QueryContextGeneration::StarRocks(_),
                    QueryExecutionGeneration::StarRocks(_)
                )
            )
            && match (self.execution_generation, key.generation) {
                (
                    QueryContextGeneration::StarRocks(current),
                    QueryExecutionGeneration::StarRocks(requested),
                ) => current == requested,
                (QueryContextGeneration::Native, QueryExecutionGeneration::Native) => true,
                _ => false,
            }
    }

    #[cfg(all(test, feature = "compat"))]
    fn bind_starrocks_generation(
        &mut self,
        generation: StarRocksQueryGeneration,
    ) -> Result<(), String> {
        match self.execution_generation {
            QueryContextGeneration::StarRocksUnbound if self.num_fragments == 0 => {
                self.execution_generation = QueryContextGeneration::StarRocks(generation);
                Ok(())
            }
            QueryContextGeneration::StarRocks(current) if current == generation => Ok(()),
            QueryContextGeneration::StarRocksUnbound => {
                Err("cannot bind StarRocks generation after fragment registration".to_string())
            }
            QueryContextGeneration::StarRocks(current) => Err(format!(
                "StarRocks query generation mismatch: current={} requested={}",
                current.get(),
                generation.get()
            )),
            QueryContextGeneration::Native => {
                Err("cannot bind StarRocks generation to native query context".to_string())
            }
        }
    }

    pub(crate) fn increment_num_fragments(&mut self) {
        self.num_fragments += 1;
        self.num_active_fragments += 1;
    }

    pub(crate) fn attach_cleanup_lease(&mut self, lease: QueryCleanupLease) {
        self.cleanup_leases.push(lease);
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
            && self
                .lookup_fetchers
                .values()
                .all(|lifecycle| matches!(lifecycle, LookupFetcherLifecycle::Exact(0)))
            && (self.cancelled_by_fe
                || self
                    .total_fragments
                    .map(|t| self.num_fragments >= t)
                    .unwrap_or(false))
    }

    pub(crate) fn is_delivery_expired(&self) -> bool {
        Instant::now() >= self.delivery_deadline
    }

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

    fn claim_legacy_runtime_filter_execution(
        &mut self,
        claim: LegacyRuntimeFilterExecutionClaim,
    ) -> Result<(), String> {
        self.validate_legacy_runtime_filter_execution_claim(claim)?;
        match (self.legacy_runtime_filter_execution, claim) {
            (_, LegacyRuntimeFilterExecutionClaim::Unclaimed) => Ok(()),
            (current, requested) if current == requested => Ok(()),
            (
                LegacyRuntimeFilterExecutionClaim::Unclaimed,
                LegacyRuntimeFilterExecutionClaim::NativeDisabled,
            ) => {
                if self.runtime_filter_params.is_some()
                    || self.runtime_filter_worker_params.is_some()
                    || self.runtime_filter_hub.is_some()
                    || self.runtime_filter_worker.is_some()
                    || !self.pending_runtime_filters.is_empty()
                {
                    return Err(
                        "cannot claim NativeDisabled with unexpected legacy runtime-filter state"
                            .to_string(),
                    );
                }
                self.legacy_runtime_filter_execution = claim;
                Ok(())
            }
            #[cfg(feature = "compat")]
            (
                LegacyRuntimeFilterExecutionClaim::Unclaimed,
                LegacyRuntimeFilterExecutionClaim::Compat,
            ) => {
                self.legacy_runtime_filter_execution = claim;
                Ok(())
            }
            (current, requested) => Err(format!(
                "legacy runtime-filter execution claim conflict: current={current:?} requested={requested:?}"
            )),
        }
    }

    fn validate_legacy_runtime_filter_execution_claim(
        &self,
        claim: LegacyRuntimeFilterExecutionClaim,
    ) -> Result<(), String> {
        match (self.legacy_runtime_filter_execution, claim) {
            (_, LegacyRuntimeFilterExecutionClaim::Unclaimed) => Ok(()),
            (current, requested) if current == requested => Ok(()),
            (
                LegacyRuntimeFilterExecutionClaim::Unclaimed,
                LegacyRuntimeFilterExecutionClaim::NativeDisabled,
            ) => {
                if self.runtime_filter_params.is_some()
                    || self.runtime_filter_worker_params.is_some()
                    || self.runtime_filter_hub.is_some()
                    || self.runtime_filter_worker.is_some()
                    || !self.pending_runtime_filters.is_empty()
                {
                    return Err(
                        "cannot claim NativeDisabled with unexpected legacy runtime-filter state"
                            .to_string(),
                    );
                }
                Ok(())
            }
            #[cfg(feature = "compat")]
            (
                LegacyRuntimeFilterExecutionClaim::Unclaimed,
                LegacyRuntimeFilterExecutionClaim::Compat,
            ) => Ok(()),
            (current, requested) => Err(format!(
                "legacy runtime-filter execution claim conflict: current={current:?} requested={requested:?}"
            )),
        }
    }

    fn ensure_legacy_runtime_filter_enabled(&self) -> Result<(), String> {
        match self.legacy_runtime_filter_execution {
            LegacyRuntimeFilterExecutionClaim::NativeDisabled => Err(
                "legacy runtime-filter state is unavailable for NativeDisabled query".to_string(),
            ),
            LegacyRuntimeFilterExecutionClaim::Unclaimed => {
                Err("legacy runtime-filter execution mode is unclaimed".to_string())
            }
            #[cfg(feature = "compat")]
            LegacyRuntimeFilterExecutionClaim::Compat => Ok(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn legacy_runtime_filter_execution_claim(
        &self,
    ) -> LegacyRuntimeFilterExecutionClaim {
        self.legacy_runtime_filter_execution
    }

    pub(crate) fn set_runtime_filter_hub(
        &mut self,
        hub: Arc<RuntimeFilterHub>,
    ) -> Result<(), String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        self.runtime_filter_hub = Some(hub);
        Ok(())
    }

    pub(crate) fn runtime_filter_hub(&self) -> Result<Option<Arc<RuntimeFilterHub>>, String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        Ok(self.runtime_filter_hub.clone())
    }

    pub(crate) fn set_runtime_filter_params(
        &mut self,
        params: RuntimeFilterParams,
    ) -> Result<(), String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        if self.runtime_filter_params.is_none() {
            self.runtime_filter_worker_params = Some(params.to_worker_params());
            self.runtime_filter_params = Some(params);
        }
        Ok(())
    }

    pub(crate) fn runtime_filter_params(&self) -> Result<Option<RuntimeFilterParams>, String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        Ok(self.runtime_filter_params.clone())
    }

    pub(crate) fn runtime_filter_worker_params(
        &self,
    ) -> Result<Option<RuntimeFilterWorkerParams>, String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        Ok(self.runtime_filter_worker_params.clone())
    }

    pub(crate) fn set_runtime_filter_worker(
        &mut self,
        worker: Arc<RuntimeFilterWorker>,
    ) -> Result<(), String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        self.runtime_filter_worker = Some(worker);
        Ok(())
    }

    pub(crate) fn runtime_filter_worker(&self) -> Result<Option<Arc<RuntimeFilterWorker>>, String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        Ok(self.runtime_filter_worker.clone())
    }

    pub(crate) fn runtime_filter_service(&self) -> Arc<RuntimeFilterService> {
        self.runtime_filter_service.clone()
    }

    pub(crate) fn merge_row_pos_descs(
        &mut self,
        descs: HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        self.validate_row_pos_descs(&descs)?;
        for (tuple_id, incoming) in descs {
            self.row_pos_descs.entry(tuple_id).or_insert(incoming);
        }
        Ok(())
    }

    fn validate_row_pos_descs(
        &self,
        descs: &HashMap<i32, RowPositionDescriptor>,
    ) -> Result<(), String> {
        for (tuple_id, incoming) in descs {
            if let Some(existing) = self.row_pos_descs.get(tuple_id) {
                if existing.row_position_type != incoming.row_position_type
                    || existing.row_source_slot != incoming.row_source_slot
                    || existing.fetch_ref_slots != incoming.fetch_ref_slots
                    || existing.lookup_ref_slots != incoming.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={tuple_id}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn row_pos_desc(&self, tuple_id: i32) -> Option<RowPositionDescriptor> {
        self.row_pos_descs.get(&tuple_id).cloned()
    }

    pub(crate) fn register_lookup_fetchers(
        &mut self,
        lifecycles: &HashMap<i32, LookupFetcherLifecycle>,
    ) {
        for (node_id, incoming) in lifecycles {
            self.lookup_fetchers
                .entry(*node_id)
                .and_modify(|existing| {
                    *existing = match (*existing, *incoming) {
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Exact(new),
                        ) => LookupFetcherLifecycle::Exact(current.max(new)),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Exact(new)) => {
                            LookupFetcherLifecycle::Exact(new)
                        }
                        (
                            LookupFetcherLifecycle::Exact(current),
                            LookupFetcherLifecycle::Unknown,
                        ) => LookupFetcherLifecycle::Exact(current),
                        (LookupFetcherLifecycle::Unknown, LookupFetcherLifecycle::Unknown) => {
                            LookupFetcherLifecycle::Unknown
                        }
                    };
                })
                .or_insert(*incoming);
        }
    }

    pub(crate) fn complete_lookup_fetcher(&mut self, node_id: i32) -> Result<(), String> {
        let lifecycle = self
            .lookup_fetchers
            .get_mut(&node_id)
            .ok_or_else(|| format!("lookup node {node_id} is not registered"))?;
        let LookupFetcherLifecycle::Exact(count) = lifecycle else {
            // Without the FE-provided peer-fragment count, a close cannot prove that
            // it is the last fetch fragment. Keep the dispatcher until bounded expiry.
            return Ok(());
        };
        if *count == 0 {
            return Ok(());
        }
        *count -= 1;
        Ok(())
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
    ) -> Result<(), String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        self.pending_runtime_filters.push(PendingRuntimeFilter {
            filter_id,
            build_be_number,
            data,
            build_data_type,
        });
        Ok(())
    }

    pub(crate) fn drain_pending_runtime_filters(
        &mut self,
    ) -> Result<Vec<PendingRuntimeFilter>, String> {
        self.ensure_legacy_runtime_filter_enabled()?;
        Ok(std::mem::take(&mut self.pending_runtime_filters))
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

impl Drop for QueryContext {
    fn drop(&mut self) {
        self.runtime_filter_service.shutdown();
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

    fn append_scan_ranges(&self, scan_ranges: &[IncrementalScanRange]) -> Result<(), String> {
        let _guard = self.update_mu.lock().expect("incremental scan handle lock");
        let morsels = self.scan.build_incremental_morsels(scan_ranges)?;
        self.dispatch
            .append_morsels(morsels.morsels, morsels.has_more)
    }
}

#[derive(Default)]
struct QueryContextManagerInner {
    active: HashMap<QueryId, QueryContext>,
    second_chance: HashMap<QueryId, QueryContext>,
    finst_to_query: HashMap<UniqueId, QueryExecutionKey>,
    fragment_completions: HashMap<UniqueId, FragmentCompletionEntry>,
    #[cfg(feature = "compat")]
    incremental_scan_nodes: HashMap<UniqueId, HashMap<i32, Arc<IncrementalScanNodeHandle>>>,
    #[cfg(feature = "compat")]
    pending_incremental_scan_ranges: HashMap<UniqueId, HashMap<i32, Vec<IncrementalScanRange>>>,
    #[cfg(feature = "compat")]
    incremental_change_op_slots: HashMap<UniqueId, HashMap<i32, Option<SlotId>>>,
}

struct FragmentCompletionEntry {
    execution: QueryExecutionKey,
    completion: Weak<FragmentCompletion>,
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

pub(crate) struct FinstCancelResult {
    pub(crate) query_id: Option<QueryId>,
    pub(crate) finsts: Vec<UniqueId>,
}

#[cfg(feature = "compat")]
pub(crate) struct StarRocksQueryHandoff {
    pub(crate) execution: QueryExecutionKey,
    pub(crate) delivery_expire: Duration,
    pub(crate) query_expire: Duration,
    pub(crate) fragment_count: usize,
    pub(crate) cache_options: CacheOptions,
    pub(crate) exchange_senders: HashMap<i32, usize>,
    pub(crate) descriptor_snapshot: Option<Arc<DescriptorSnapshot>>,
    pub(crate) total_fragments: Option<usize>,
    pub(crate) row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    pub(crate) lookup_fetchers: HashMap<i32, LookupFetcherLifecycle>,
    pub(crate) runtime_filter_params: RuntimeFilterParams,
    pub(crate) instances: Vec<(UniqueId, HashMap<i32, Option<SlotId>>)>,
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
        let expired = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let expired_second_chance = guard
                .second_chance
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_delivery_expired()).then_some(*qid)
                })
                .collect::<Vec<_>>();
            let expired_active = guard
                .active
                .iter()
                .filter_map(|(qid, ctx)| {
                    (ctx.has_no_active_instances() && ctx.is_query_expired()).then_some(*qid)
                })
                .collect::<Vec<_>>();
            let mut expired = Vec::with_capacity(
                expired_second_chance
                    .len()
                    .saturating_add(expired_active.len()),
            );
            expired.extend(
                expired_second_chance
                    .into_iter()
                    .filter_map(|qid| guard.second_chance.remove(&qid).map(|ctx| (qid, ctx))),
            );
            expired.extend(
                expired_active
                    .into_iter()
                    .filter_map(|qid| guard.active.remove(&qid).map(|ctx| (qid, ctx))),
            );
            expired
        };
        for (qid, ctx) in expired {
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            self.remove_runtime_filter_lifecycle_if_context_absent(qid);
        }
    }

    #[cfg(test)]
    pub(crate) fn clean_expired_for_test(&self) {
        self.clean_expired();
    }

    #[cfg(test)]
    pub(crate) fn expire_delivery_for_test(&self, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = if guard.active.contains_key(&query_id) {
            guard.active.get_mut(&query_id).expect("checked active")
        } else {
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("query context must exist")
        };
        context.delivery_deadline = Instant::now() - Duration::from_millis(1);
    }

    #[cfg(test)]
    pub(crate) fn fragment_counts_for_test(&self, query_id: QueryId) -> Option<(usize, usize)> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .map(|context| (context.num_fragments, context.num_active_fragments))
    }

    fn remove_runtime_filter_lifecycle_if_context_absent(&self, query_id: QueryId) {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        if !guard.active.contains_key(&query_id) && !guard.second_chance.contains_key(&query_id) {
            remove_runtime_filter_lifecycle(query_id);
        }
    }

    fn get_or_register(
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

    pub(crate) fn get_or_register_native(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_claim(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
            LegacyRuntimeFilterExecutionClaim::NativeDisabled,
        )
    }

    pub(crate) fn ensure_native_context(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_claim(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
            LegacyRuntimeFilterExecutionClaim::NativeDisabled,
        )
    }

    #[cfg(feature = "compat")]
    pub(crate) fn get_or_register_compat(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_claim(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            true,
            LegacyRuntimeFilterExecutionClaim::Compat,
        )
    }

    #[cfg(feature = "compat")]
    pub(crate) fn ensure_compat_context(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        self.get_or_register_internal_with_claim(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            false,
            LegacyRuntimeFilterExecutionClaim::Compat,
        )
    }

    fn ensure_context(
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
        self.get_or_register_internal_with_claim(
            query_id,
            return_error_if_not_exist,
            delivery_expire,
            query_expire,
            increment,
            LegacyRuntimeFilterExecutionClaim::Unclaimed,
        )
    }

    fn get_or_register_internal_with_claim(
        &self,
        query_id: QueryId,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
        increment: bool,
        claim: LegacyRuntimeFilterExecutionClaim,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            if claim != LegacyRuntimeFilterExecutionClaim::Unclaimed {
                ctx.claim_legacy_runtime_filter_execution(claim)?;
            }
            if increment {
                ctx.increment_num_fragments();
            }
            return Ok(());
        }
        if guard.second_chance.contains_key(&query_id) {
            if claim != LegacyRuntimeFilterExecutionClaim::Unclaimed {
                guard
                    .second_chance
                    .get_mut(&query_id)
                    .expect("checked")
                    .claim_legacy_runtime_filter_execution(claim)?;
            }
            let mut ctx = guard.second_chance.remove(&query_id).expect("checked");
            if increment {
                ctx.increment_num_fragments();
            }
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let generation = match claim {
            #[cfg(feature = "compat")]
            LegacyRuntimeFilterExecutionClaim::Compat => QueryContextGeneration::StarRocksUnbound,
            _ => QueryContextGeneration::Native,
        };
        let mut ctx =
            QueryContext::new_with_generation(query_id, generation, delivery_expire, query_expire);
        if claim != LegacyRuntimeFilterExecutionClaim::Unclaimed {
            ctx.claim_legacy_runtime_filter_execution(claim)?;
        }
        if increment {
            ctx.increment_num_fragments();
        }
        guard.active.insert(query_id, ctx);
        Ok(())
    }

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn get_or_register_compat_generation(
        &self,
        execution: QueryExecutionKey,
        return_error_if_not_exist: bool,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<(), String> {
        let generation = execution
            .starrocks_generation()
            .ok_or_else(|| "compat query registration requires StarRocks generation".to_string())?;
        let query_id = execution.query_id();
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .finst_to_query
            .values()
            .any(|existing| existing.query_id() == query_id && *existing != execution)
        {
            return Err(format!(
                "previous StarRocks query generation still has fragment routing: query_id={query_id}"
            ));
        }
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.bind_starrocks_generation(generation)?;
            ctx.claim_legacy_runtime_filter_execution(LegacyRuntimeFilterExecutionClaim::Compat)?;
            ctx.increment_num_fragments();
            return Ok(());
        }
        if guard.second_chance.contains_key(&query_id) {
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("checked")
                .bind_starrocks_generation(generation)?;
            guard
                .second_chance
                .get_mut(&query_id)
                .expect("checked")
                .claim_legacy_runtime_filter_execution(LegacyRuntimeFilterExecutionClaim::Compat)?;
            let mut ctx = guard.second_chance.remove(&query_id).expect("checked");
            ctx.increment_num_fragments();
            guard.active.insert(query_id, ctx);
            return Ok(());
        }
        if return_error_if_not_exist {
            return Err("Query terminates prematurely (missing QueryContext)".to_string());
        }
        let mut ctx = QueryContext::new_with_generation(
            query_id,
            QueryContextGeneration::StarRocks(generation),
            delivery_expire,
            query_expire,
        );
        ctx.claim_legacy_runtime_filter_execution(LegacyRuntimeFilterExecutionClaim::Compat)?;
        ctx.increment_num_fragments();
        guard.active.insert(query_id, ctx);
        Ok(())
    }

    #[cfg(feature = "compat")]
    pub(crate) fn commit_starrocks_handoff<F>(
        &self,
        handoff: StarRocksQueryHandoff,
        make_cleanup_lease: F,
    ) -> Result<Arc<MemTracker>, String>
    where
        F: FnOnce() -> Option<QueryCleanupLease>,
    {
        let generation = handoff
            .execution
            .starrocks_generation()
            .ok_or_else(|| "StarRocks handoff requires query generation".to_string())?;
        if handoff.fragment_count == 0 || handoff.fragment_count != handoff.instances.len() {
            return Err("StarRocks handoff fragment count does not match instances".to_string());
        }

        let query_id = handoff.execution.query_id();
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .finst_to_query
            .values()
            .any(|existing| existing.query_id() == query_id && *existing != handoff.execution)
        {
            return Err(format!(
                "previous StarRocks query generation still has fragment routing: query_id={query_id}"
            ));
        }
        let mut incoming_finsts = HashSet::with_capacity(handoff.instances.len());
        for (finst_id, _) in &handoff.instances {
            if !incoming_finsts.insert(*finst_id) {
                return Err(format!(
                    "duplicate fragment instance in StarRocks handoff: finst_id={finst_id}"
                ));
            }
            if let Some(existing) = guard.finst_to_query.get(finst_id) {
                return Err(format!(
                    "fragment instance is already registered: finst_id={finst_id} execution={existing:?}"
                ));
            }
        }

        let existing = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id));
        if let Some(context) = existing {
            match context.execution_generation {
                QueryContextGeneration::StarRocksUnbound if context.num_fragments == 0 => {}
                QueryContextGeneration::StarRocks(current) if current == generation => {}
                QueryContextGeneration::StarRocksUnbound => {
                    return Err(
                        "cannot bind StarRocks generation after fragment registration".to_string(),
                    );
                }
                QueryContextGeneration::StarRocks(current) => {
                    return Err(format!(
                        "StarRocks query generation mismatch: current={} requested={}",
                        current.get(),
                        generation.get()
                    ));
                }
                QueryContextGeneration::Native => {
                    return Err(
                        "cannot bind StarRocks generation to native query context".to_string()
                    );
                }
            }
            context.validate_legacy_runtime_filter_execution_claim(
                LegacyRuntimeFilterExecutionClaim::Compat,
            )?;
            if context
                .cache_options
                .as_ref()
                .is_some_and(|current| current != &handoff.cache_options)
            {
                return Err("cache options mismatch for query".to_string());
            }
            context.validate_row_pos_descs(&handoff.row_pos_descs)?;
        }

        // All fallible checks finish before the descriptor lease is created or any Q state is
        // published. The caller may therefore keep the descriptor-cache lock across this call
        // without risking a cleanup-lease drop while that lock is held.
        let cleanup_lease = make_cleanup_lease();
        let mut context = if let Some(context) = guard.active.remove(&query_id) {
            context
        } else if let Some(context) = guard.second_chance.remove(&query_id) {
            context
        } else {
            QueryContext::new_with_generation(
                query_id,
                QueryContextGeneration::StarRocks(generation),
                handoff.delivery_expire,
                handoff.query_expire,
            )
        };
        if context.execution_generation == QueryContextGeneration::StarRocksUnbound {
            context.execution_generation = QueryContextGeneration::StarRocks(generation);
        }
        if context.legacy_runtime_filter_execution == LegacyRuntimeFilterExecutionClaim::Unclaimed {
            context.legacy_runtime_filter_execution = LegacyRuntimeFilterExecutionClaim::Compat;
        }
        if context.cache_options.is_none() {
            context.cache_options = Some(handoff.cache_options);
        }
        context.update_exchange_senders(handoff.exchange_senders);
        if let Some(snapshot) = handoff.descriptor_snapshot {
            context.desc_snapshot = Some(snapshot);
        }
        if let Some(total_fragments) = handoff.total_fragments {
            context.total_fragments = Some(
                context
                    .total_fragments
                    .map_or(total_fragments, |current| current.max(total_fragments)),
            );
        }
        for (tuple_id, descriptor) in handoff.row_pos_descs {
            context.row_pos_descs.entry(tuple_id).or_insert(descriptor);
        }
        context.register_lookup_fetchers(&handoff.lookup_fetchers);
        if context.runtime_filter_params.is_none() {
            context.runtime_filter_worker_params =
                Some(handoff.runtime_filter_params.to_worker_params());
            context.runtime_filter_params = Some(handoff.runtime_filter_params);
        }
        context.num_fragments += handoff.fragment_count;
        context.num_active_fragments += handoff.fragment_count;
        if let Some(lease) = cleanup_lease {
            context.attach_cleanup_lease(lease);
        }
        let mem_tracker = context.mem_tracker();
        guard.active.insert(query_id, context);
        for (finst_id, contracts) in handoff.instances {
            guard.finst_to_query.insert(finst_id, handoff.execution);
            guard
                .incremental_change_op_slots
                .insert(finst_id, contracts);
        }
        Ok(mem_tracker)
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

    pub(crate) fn attach_cleanup_lease(
        &self,
        query_id: QueryId,
        lease: QueryCleanupLease,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.attach_cleanup_lease(lease);
            Ok(())
        })
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
        self.with_context_mut(query_id, |ctx| ctx.merge_row_pos_descs(descs))
    }

    pub(crate) fn register_lookup_fetchers(
        &self,
        query_id: QueryId,
        lifecycles: HashMap<i32, LookupFetcherLifecycle>,
    ) -> Result<(), String> {
        self.with_context_mut(query_id, |ctx| {
            ctx.register_lookup_fetchers(&lifecycles);
            Ok(())
        })
    }

    pub(crate) fn complete_lookup_fetcher(
        &self,
        query_id: QueryId,
        node_id: i32,
    ) -> Result<(), String> {
        let removed = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.complete_lookup_fetcher(node_id)?;
                None
            } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.complete_lookup_fetcher(node_id)?;
                if ctx.is_dead() {
                    guard.second_chance.remove(&query_id)
                } else {
                    None
                }
            } else {
                return Err(format!("QueryContext not found: query_id={query_id}"));
            }
        };
        if let Some(ctx) = removed {
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
        Ok(())
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
            ctx.set_runtime_filter_hub(hub)?;
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_hub(hub)?;
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    pub(crate) fn get_runtime_filter_hub(
        &self,
        query_id: QueryId,
    ) -> Result<Option<Arc<RuntimeFilterHub>>, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(ctx) = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
        else {
            return Ok(None);
        };
        ctx.runtime_filter_hub()
    }

    pub(crate) fn set_runtime_filter_params(
        &self,
        query_id: QueryId,
        params: RuntimeFilterParams,
    ) -> Result<(), String> {
        let pending = self.with_context_mut(query_id, |ctx| {
            ctx.set_runtime_filter_params(params)?;
            ctx.drain_pending_runtime_filters()
        })?;
        if let Some(worker) = self.get_or_create_runtime_filter_worker(query_id)? {
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
    ) -> Result<Option<RuntimeFilterParams>, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(ctx) = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
        else {
            return Ok(None);
        };
        ctx.runtime_filter_params()
    }

    #[allow(dead_code)]
    pub(crate) fn set_runtime_filter_worker(
        &self,
        query_id: QueryId,
        worker: Arc<RuntimeFilterWorker>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker)?;
            return Ok(());
        }
        if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx.set_runtime_filter_worker(worker)?;
            return Ok(());
        }
        Err("QueryContext not found".to_string())
    }

    #[allow(dead_code)]
    pub(crate) fn get_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Result<Option<Arc<RuntimeFilterWorker>>, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let Some(ctx) = guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
        else {
            return Ok(None);
        };
        ctx.runtime_filter_worker()
    }

    pub(crate) fn get_or_create_runtime_filter_worker(
        &self,
        query_id: QueryId,
    ) -> Result<Option<Arc<RuntimeFilterWorker>>, String> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let ctx = if let Some(ctx) = guard.active.get_mut(&query_id) {
            ctx
        } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
            ctx
        } else {
            return Ok(None);
        };
        let existing_worker = ctx.runtime_filter_worker()?;
        if let Some(worker) = existing_worker {
            return Ok(Some(worker));
        }
        let Some(params) = ctx.runtime_filter_worker_params()? else {
            return Ok(None);
        };
        let hub = if let Some(hub) = ctx.runtime_filter_hub()? {
            hub
        } else {
            let hub = Arc::new(RuntimeFilterHub::new_for_query(
                DependencyManager::new(),
                query_id,
            ));
            ctx.set_runtime_filter_hub(Arc::clone(&hub))?;
            hub
        };
        let worker = Arc::new(RuntimeFilterWorker::new(query_id, params, hub));
        ctx.set_runtime_filter_worker(Arc::clone(&worker))?;
        Ok(Some(worker))
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
            ctx.push_pending_runtime_filter(filter_id, build_be_number, data, build_data_type)
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
        mut scan_ranges: Vec<IncrementalScanRange>,
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

    #[cfg(feature = "compat")]
    pub(crate) fn incremental_change_op_slot(
        &self,
        finst_id: UniqueId,
        node_id: i32,
    ) -> Result<Option<SlotId>, String> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .incremental_change_op_slots
            .get(&finst_id)
            .and_then(|contracts| contracts.get(&node_id))
            .copied()
            .ok_or_else(|| {
                format!(
                    "incremental scan range has no registered scan contract for finst_id={finst_id} node_id={node_id}"
                )
            })
    }

    #[cfg(all(test, feature = "compat"))]
    fn pending_incremental_scan_ranges_for_test(
        &self,
        finst_id: UniqueId,
        node_id: i32,
    ) -> Vec<IncrementalScanRange> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .pending_incremental_scan_ranges
            .get(&finst_id)
            .and_then(|nodes| nodes.get(&node_id))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn register_finst(&self, finst_id: UniqueId, query_id: QueryId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .finst_to_query
            .insert(finst_id, QueryExecutionKey::native(query_id));
    }

    pub(crate) fn register_finsts<I>(&self, finst_ids: I, query_id: QueryId)
    where
        I: IntoIterator<Item = UniqueId>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        for finst_id in finst_ids {
            guard
                .finst_to_query
                .insert(finst_id, QueryExecutionKey::native(query_id));
        }
    }

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn register_starrocks_finsts<I>(
        &self,
        finst_ids: I,
        execution: QueryExecutionKey,
    ) -> Result<(), String>
    where
        I: IntoIterator<Item = UniqueId>,
    {
        if execution.starrocks_generation().is_none() {
            return Err("StarRocks finst registration requires generation".to_string());
        }
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let context = guard
            .active
            .get(&execution.query_id())
            .ok_or_else(|| "QueryContext not found".to_string())?;
        if !context.matches_execution(execution) {
            return Err("StarRocks query generation is not active".to_string());
        }
        for finst_id in finst_ids {
            guard.finst_to_query.insert(finst_id, execution);
        }
        Ok(())
    }

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn register_finsts_with_incremental_contracts<I>(
        &self,
        instances: I,
        query_id: QueryId,
    ) where
        I: IntoIterator<Item = (UniqueId, HashMap<i32, Option<SlotId>>)>,
    {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        for (finst_id, contracts) in instances {
            guard
                .finst_to_query
                .insert(finst_id, QueryExecutionKey::native(query_id));
            guard
                .incremental_change_op_slots
                .insert(finst_id, contracts);
        }
    }

    pub(crate) fn query_id_by_finst(&self, finst_id: UniqueId) -> Option<QueryId> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        guard
            .finst_to_query
            .get(&finst_id)
            .map(|execution| execution.query_id())
    }

    #[cfg(all(test, feature = "compat"))]
    pub(crate) fn query_execution_by_finst(&self, finst_id: UniqueId) -> Option<QueryExecutionKey> {
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
        #[cfg(feature = "compat")]
        guard.incremental_change_op_slots.remove(&finst_id);
    }

    pub(crate) fn unregister_finst_execution(
        &self,
        finst_id: UniqueId,
        execution: QueryExecutionKey,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard.finst_to_query.get(&finst_id) != Some(&execution) {
            return;
        }
        guard.finst_to_query.remove(&finst_id);
        if guard
            .fragment_completions
            .get(&finst_id)
            .is_some_and(|entry| entry.execution == execution)
        {
            guard.fragment_completions.remove(&finst_id);
        }
        #[cfg(feature = "compat")]
        guard.incremental_scan_nodes.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.pending_incremental_scan_ranges.remove(&finst_id);
        #[cfg(feature = "compat")]
        guard.incremental_change_op_slots.remove(&finst_id);
    }

    pub(crate) fn register_fragment_completion(
        &self,
        finst_id: UniqueId,
        completion: Arc<FragmentCompletion>,
    ) -> Option<QueryExecutionKey> {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        let execution = guard.finst_to_query.get(&finst_id).copied()?;
        guard.fragment_completions.insert(
            finst_id,
            FragmentCompletionEntry {
                execution,
                completion: Arc::downgrade(&completion),
            },
        );
        Some(execution)
    }

    pub(crate) fn unregister_fragment_completion(&self, finst_id: UniqueId) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        guard.fragment_completions.remove(&finst_id);
    }

    pub(crate) fn unregister_fragment_completion_execution(
        &self,
        finst_id: UniqueId,
        execution: QueryExecutionKey,
    ) {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if guard
            .fragment_completions
            .get(&finst_id)
            .is_some_and(|entry| entry.execution == execution)
        {
            guard.fragment_completions.remove(&finst_id);
        }
    }

    pub(crate) fn get_query_timeout_by_finst(&self, finst_id: UniqueId) -> Option<Duration> {
        let guard = self.inner.lock().expect("query_ctx_manager lock");
        let query_id = guard.finst_to_query.get(&finst_id)?.query_id();
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
        let (service, finsts) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let service = if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
                Some(ctx.runtime_filter_service())
            } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
                Some(ctx.runtime_filter_service())
            } else {
                None
            };
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, execution)| {
                    (execution.query_id() == query_id).then_some(*finst_id)
                })
                .collect();
            (service, finsts)
        };
        if let Some(service) = service {
            service.cancel();
        }
        finsts
    }

    pub(crate) fn cancel_query(&self, query_id: QueryId, err: String) -> Vec<UniqueId> {
        let (service, finsts, completions) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let service = if let Some(ctx) = guard.active.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
                Some(ctx.runtime_filter_service())
            } else if let Some(ctx) = guard.second_chance.get_mut(&query_id) {
                ctx.cancelled_by_fe = true;
                Some(ctx.runtime_filter_service())
            } else {
                None
            };

            let mut finsts = Vec::new();
            let mut completions = Vec::new();
            let mut stale = Vec::new();
            for (finst_id, execution) in guard.finst_to_query.iter() {
                if execution.query_id() != query_id {
                    continue;
                }
                finsts.push(*finst_id);
                if let Some(entry) = guard.fragment_completions.get(finst_id) {
                    if let Some(completion) = entry.completion.upgrade() {
                        completions.push(completion);
                    } else {
                        stale.push(*finst_id);
                    }
                }
            }
            for finst_id in stale {
                guard.fragment_completions.remove(&finst_id);
            }
            (service, finsts, completions)
        };

        if let Some(service) = service {
            service.cancel();
        }
        for completion in completions {
            completion.abort_from_query(err.clone());
        }
        finsts
    }

    pub(crate) fn cancel_query_execution(
        &self,
        execution: QueryExecutionKey,
        err: String,
    ) -> Vec<UniqueId> {
        let (service, finsts, completions) = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let query_id = execution.query_id();
            let context = if guard.active.contains_key(&query_id) {
                guard.active.get_mut(&query_id)
            } else {
                guard.second_chance.get_mut(&query_id)
            };
            let service = context
                .filter(|ctx| ctx.matches_execution(execution))
                .map(|ctx| {
                    ctx.cancelled_by_fe = true;
                    ctx.runtime_filter_service()
                });
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, current)| (*current == execution).then_some(*finst_id))
                .collect::<Vec<_>>();
            let completions = finsts
                .iter()
                .filter_map(|finst_id| guard.fragment_completions.get(finst_id))
                .filter(|entry| entry.execution == execution)
                .filter_map(|entry| entry.completion.upgrade())
                .collect::<Vec<_>>();
            (service, finsts, completions)
        };
        if let Some(service) = service {
            service.cancel();
        }
        for completion in completions {
            completion.abort_from_query(err.clone());
        }
        finsts
    }

    pub(crate) fn cancel_finst(&self, finst_id: UniqueId, err: String) -> FinstCancelResult {
        self.cancel_finst_internal(finst_id, err, || {})
    }

    fn cancel_finst_internal<F>(
        &self,
        finst_id: UniqueId,
        err: String,
        binding_observer: F,
    ) -> FinstCancelResult
    where
        F: FnOnce(),
    {
        let collected = {
            let mut guard = self.inner.lock().expect("query_ctx_manager lock");
            let Some(execution) = guard.finst_to_query.get(&finst_id).copied() else {
                return FinstCancelResult {
                    query_id: None,
                    finsts: Vec::new(),
                };
            };
            binding_observer();
            let query_id = execution.query_id();
            let context = if guard.active.contains_key(&query_id) {
                guard.active.get_mut(&query_id)
            } else {
                guard.second_chance.get_mut(&query_id)
            };
            let service = context
                .filter(|ctx| ctx.matches_execution(execution))
                .map(|ctx| {
                    ctx.cancelled_by_fe = true;
                    ctx.runtime_filter_service()
                });
            let finsts = guard
                .finst_to_query
                .iter()
                .filter_map(|(finst_id, current)| (*current == execution).then_some(*finst_id))
                .collect::<Vec<_>>();
            let completions = finsts
                .iter()
                .filter_map(|finst_id| guard.fragment_completions.get(finst_id))
                .filter(|entry| entry.execution == execution)
                .filter_map(|entry| entry.completion.upgrade())
                .collect::<Vec<_>>();
            (query_id, service, finsts, completions)
        };
        let (query_id, service, finsts, completions) = collected;
        if let Some(service) = service {
            service.cancel();
        }
        for completion in completions {
            completion.abort_from_query(err.clone());
        }
        if finsts.is_empty() {
            return FinstCancelResult {
                query_id: Some(query_id),
                finsts,
            };
        }
        FinstCancelResult {
            query_id: Some(query_id),
            finsts,
        }
    }

    #[cfg(all(test, feature = "compat"))]
    fn cancel_finst_with_binding_observer<F>(
        &self,
        finst_id: UniqueId,
        err: String,
        binding_observer: F,
    ) -> FinstCancelResult
    where
        F: FnOnce(),
    {
        self.cancel_finst_internal(finst_id, err, binding_observer)
    }

    /// A sender's exchange RPC failed. Map the finst to its query and cancel
    /// the whole query so blocked receivers abort instead of timing out.
    pub(crate) fn propagate_sender_error(&self, finst_id: UniqueId, err: String) -> Vec<UniqueId> {
        let result = self.cancel_finst(finst_id, format!("exchange send failed: {err}"));
        match result.query_id {
            Some(_) => {
                let finsts = result.finsts;
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
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
    }

    pub(crate) fn finish_fragment_for_report(
        &self,
        query_id: QueryId,
    ) -> FragmentFinishReportDecision {
        self.finish_fragment_internal(query_id)
    }

    pub(crate) fn finish_fragment_execution(&self, execution: QueryExecutionKey) {
        let decision =
            self.finish_fragment_internal_execution(execution.query_id(), Some(execution));
        if decision.remove_runtime_filter_lifecycle_after_report {
            self.remove_runtime_filter_lifecycle_if_context_absent(execution.query_id());
        }
    }

    pub(crate) fn finish_fragment_for_report_execution(
        &self,
        execution: QueryExecutionKey,
    ) -> FragmentFinishReportDecision {
        self.finish_fragment_internal_execution(execution.query_id(), Some(execution))
    }

    pub(crate) fn cleanup_after_fragment_report(
        &self,
        query_id: QueryId,
        decision: FragmentFinishReportDecision,
    ) {
        if decision.remove_runtime_filter_lifecycle_after_report {
            self.remove_runtime_filter_lifecycle_if_context_absent(query_id);
        }
    }

    fn finish_fragment_internal(&self, query_id: QueryId) -> FragmentFinishReportDecision {
        self.finish_fragment_internal_execution(query_id, None)
    }

    fn finish_fragment_internal_execution(
        &self,
        query_id: QueryId,
        execution: Option<QueryExecutionKey>,
    ) -> FragmentFinishReportDecision {
        let mut guard = self.inner.lock().expect("query_ctx_manager lock");
        if execution.is_some_and(|execution| {
            !guard
                .active
                .get(&query_id)
                .is_some_and(|ctx| ctx.matches_execution(execution))
        }) {
            return FragmentFinishReportDecision {
                include_runtime_filter_profile: false,
                remove_runtime_filter_lifecycle_after_report: false,
            };
        }
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
            let decision = FragmentFinishReportDecision {
                include_runtime_filter_profile: true,
                remove_runtime_filter_lifecycle_after_report: true,
            };
            drop(guard);
            ctx.runtime_filter_service().shutdown();
            drop(ctx);
            return decision;
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

#[cfg(all(test, feature = "compat"))]
mod generation_race_tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    use crate::common::types::UniqueId;

    use super::{
        QueryContextManager, QueryContextManagerInner, QueryExecutionKey, QueryId,
        StarRocksQueryGeneration,
    };

    fn test_manager() -> Arc<QueryContextManager> {
        Arc::new(QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        })
    }

    #[test]
    fn cancel_resolution_is_atomic_with_old_generation_drain_and_reuse() {
        let manager = test_manager();
        let query_id = QueryId {
            hi: 86_001,
            lo: 86_002,
        };
        let old_finst = UniqueId {
            hi: 86_003,
            lo: 86_004,
        };
        let new_finst = UniqueId {
            hi: 86_005,
            lo: 86_006,
        };
        let old_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(1).expect("old generation"),
        );
        let new_execution = QueryExecutionKey::starrocks(
            query_id,
            StarRocksQueryGeneration::new(2).expect("new generation"),
        );
        manager
            .get_or_register_compat_generation(
                old_execution,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("old context");
        manager
            .register_starrocks_finsts([old_finst], old_execution)
            .expect("old routing");

        let (resolved_tx, resolved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let cancel_manager = Arc::clone(&manager);
        let cancel = std::thread::spawn(move || {
            cancel_manager.cancel_finst_with_binding_observer(
                old_finst,
                "old generation cancel".to_string(),
                || {
                    resolved_tx.send(()).expect("signal resolved binding");
                    release_rx.recv().expect("release cancel lock");
                },
            )
        });
        resolved_rx.recv().expect("cancel resolved old binding");

        let (reuse_started_tx, reuse_started_rx) = mpsc::channel();
        let reuse_manager = Arc::clone(&manager);
        let reuse = std::thread::spawn(move || {
            reuse_started_tx.send(()).expect("signal reuse attempt");
            reuse_manager.unregister_finst_execution(old_finst, old_execution);
            reuse_manager.finish_fragment_execution(old_execution);
            reuse_manager
                .get_or_register_compat_generation(
                    new_execution,
                    false,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                )
                .expect("new context after old routing drain");
            reuse_manager
                .register_starrocks_finsts([new_finst], new_execution)
                .expect("new routing");
        });
        reuse_started_rx.recv().expect("reuse thread started");
        release_tx.send(()).expect("release cancel lock");

        let cancelled = cancel.join().expect("cancel thread");
        reuse.join().expect("reuse thread");
        assert_eq!(cancelled.query_id, Some(query_id));
        assert_eq!(cancelled.finsts, vec![old_finst]);
        assert_eq!(
            manager.query_execution_by_finst(new_finst),
            Some(new_execution)
        );
        assert!(
            !manager.is_query_canceled(query_id),
            "old cancellation must only touch the service captured for its generation"
        );

        manager.unregister_finst_execution(new_finst, new_execution);
        manager.cancel_query_execution(new_execution, "test cleanup".to_string());
        manager.finish_fragment_execution(new_execution);
    }
}

#[cfg(test)]
mod lookup_lifecycle_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::{LookupFetcherLifecycle, QueryContextManager, QueryContextManagerInner, QueryId};

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn lookup_context_survives_all_fragments_until_last_fetcher_closes() {
        let manager = test_manager();
        let query_id = QueryId { hi: 901, lo: 902 };
        for _ in 0..2 {
            manager
                .get_or_register(
                    query_id,
                    false,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("fragment context");
        }
        {
            let mut guard = manager.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(2);
        }
        manager
            .register_lookup_fetchers(
                query_id,
                HashMap::from([(3, LookupFetcherLifecycle::Exact(1))]),
            )
            .expect("lookup lifecycle");

        manager.finish_fragment(query_id);
        manager.finish_fragment(query_id);

        {
            let guard = manager.inner.lock().expect("query ctx manager lock");
            assert!(!guard.active.contains_key(&query_id));
            assert!(guard.second_chance.contains_key(&query_id));
        }

        manager
            .complete_lookup_fetcher(query_id, 3)
            .expect("last fetcher close");

        let guard = manager.inner.lock().expect("query ctx manager lock");
        assert!(!guard.active.contains_key(&query_id));
        assert!(!guard.second_chance.contains_key(&query_id));
    }

    #[test]
    fn duplicate_fragment_registration_does_not_double_lookup_fetchers() {
        let manager = test_manager();
        let query_id = QueryId { hi: 911, lo: 912 };
        manager
            .get_or_register(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("query context");

        for _ in 0..2 {
            manager
                .register_lookup_fetchers(
                    query_id,
                    HashMap::from([(7, LookupFetcherLifecycle::Exact(2))]),
                )
                .expect("idempotent registration");
        }

        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("first close");
        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("second close");
        manager
            .complete_lookup_fetcher(query_id, 7)
            .expect("duplicate close is idempotent");
    }

    #[test]
    fn unknown_lookup_fetcher_count_keeps_context_until_bounded_expiry() {
        let manager = test_manager();
        let query_id = QueryId { hi: 921, lo: 922 };
        manager
            .get_or_register(query_id, false, Duration::ZERO, Duration::from_secs(5))
            .expect("query context");
        manager
            .register_lookup_fetchers(
                query_id,
                HashMap::from([(8, LookupFetcherLifecycle::Unknown)]),
            )
            .expect("unknown lookup lifecycle");

        manager.finish_fragment(query_id);
        manager
            .complete_lookup_fetcher(query_id, 8)
            .expect("unknown close is acknowledged conservatively");

        {
            let guard = manager.inner.lock().expect("query ctx manager lock");
            assert!(guard.second_chance.contains_key(&query_id));
        }
        manager.clean_expired();
        let guard = manager.inner.lock().expect("query ctx manager lock");
        assert!(!guard.second_chance.contains_key(&query_id));
    }
}

static QUERY_CONTEXT_MANAGER: OnceLock<Arc<QueryContextManager>> = OnceLock::new();

pub(crate) fn query_context_manager() -> Arc<QueryContextManager> {
    QUERY_CONTEXT_MANAGER
        .get_or_init(QueryContextManager::new)
        .clone()
}

#[cfg(test)]
mod legacy_runtime_filter_execution_claim_tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{
        LegacyRuntimeFilterExecutionClaim, QueryContextManager, QueryContextManagerInner, QueryId,
    };
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    fn test_manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn disabled_query_context_rejects_legacy_params_without_partial_state() {
        let mgr = test_manager();
        let query_id = QueryId { hi: 501, lo: 502 };
        mgr.get_or_register_native(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("native claim");

        let error = mgr
            .set_runtime_filter_params(
                query_id,
                RuntimeFilterParams::new(BTreeMap::new(), BTreeMap::new(), None),
            )
            .expect_err("disabled context must reject legacy params");
        assert!(error.contains("NativeDisabled"), "{error}");
        let guard = mgr.inner.lock().expect("query ctx manager lock");
        let context = guard.active.get(&query_id).expect("query context");
        assert_eq!(
            context.legacy_runtime_filter_execution_claim(),
            LegacyRuntimeFilterExecutionClaim::NativeDisabled
        );
        assert!(context.runtime_filter_params.is_none());
        assert_eq!(context.num_fragments, 1);
    }

    #[test]
    fn disabled_query_context_rejects_all_legacy_state_access() {
        let mgr = test_manager();
        let query_id = QueryId { hi: 505, lo: 506 };
        mgr.get_or_register_native(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("native claim");

        let hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        assert!(mgr.set_runtime_filter_hub(query_id, hub).is_err());
        assert!(mgr.get_runtime_filter_hub(query_id).is_err());
        assert!(mgr.get_runtime_filter_worker(query_id).is_err());
        assert!(mgr.get_or_create_runtime_filter_worker(query_id).is_err());
        assert!(
            mgr.enqueue_pending_runtime_filter(query_id, 7, 0, vec![1], None)
                .is_err()
        );
        assert!(
            mgr.with_context_mut(query_id, |ctx| ctx.drain_pending_runtime_filters())
                .is_err()
        );
    }
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
    fn report_cleanup_preserves_lifecycle_for_recreated_context() {
        let mgr = test_manager();
        let query_id = QueryId {
            hi: 4_181,
            lo: 4_182,
        };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);

        mgr.get_or_register(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("old query context");
        {
            let mut guard = mgr.inner.lock().expect("query ctx manager lock");
            guard
                .active
                .get_mut(&query_id)
                .expect("active query")
                .total_fragments = Some(1);
        }

        let decision = mgr.finish_fragment_for_report(query_id);
        assert!(decision.remove_runtime_filter_lifecycle_after_report);
        mgr.ensure_context(
            query_id,
            false,
            Duration::from_secs(1),
            Duration::from_secs(5),
        )
        .expect("replacement query context");

        mgr.cleanup_after_fragment_report(query_id, decision);

        assert!(
            registry.snapshot(query_key).is_some(),
            "old report cleanup must preserve the replacement context lifecycle"
        );
        registry.remove_query(query_key);
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

#[cfg(all(test, feature = "compat"))]
mod incremental_scan_domain_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    use super::{QueryContextManager, QueryContextManagerInner, QueryId};
    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::node::scan::IncrementalScanRange;

    fn manager() -> QueryContextManager {
        QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        }
    }

    #[test]
    fn pending_incremental_ranges_store_domain_values_and_registered_slot_contract() {
        let manager = manager();
        let finst_id = UniqueId { hi: 91, lo: 92 };
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, Some(SlotId::new(7)))]))],
            QueryId { hi: 81, lo: 82 },
        );

        assert_eq!(
            manager
                .incremental_change_op_slot(finst_id, 41)
                .expect("registered contract"),
            Some(SlotId::new(7))
        );
        manager
            .append_incremental_scan_ranges(
                finst_id,
                41,
                vec![IncrementalScanRange::Empty {
                    has_more: Some(true),
                }],
            )
            .expect("queue domain range");
        let pending = manager.pending_incremental_scan_ranges_for_test(finst_id, 41);
        assert!(matches!(
            pending.as_slice(),
            [IncrementalScanRange::Empty {
                has_more: Some(true)
            }]
        ));
    }

    #[test]
    fn incremental_slot_lookup_rejects_unknown_node_without_pending_side_effect() {
        let manager = manager();
        let finst_id = UniqueId { hi: 93, lo: 94 };
        manager.register_finsts_with_incremental_contracts(
            [(finst_id, HashMap::from([(41, None)]))],
            QueryId { hi: 83, lo: 84 },
        );

        let error = manager
            .incremental_change_op_slot(finst_id, 42)
            .expect_err("unknown node must fail before append");
        assert!(error.contains("no registered scan contract"), "{error}");
        assert!(
            manager
                .pending_incremental_scan_ranges_for_test(finst_id, 42)
                .is_empty()
        );
    }
}

#[cfg(test)]
mod row_position_descriptor_tests {
    use std::collections::HashMap;

    use super::{QueryContext, QueryId};
    use crate::common::ids::SlotId;
    use crate::exec::row_position::{RowPositionDescriptor, RowPositionType};

    fn descriptor(row_source_slot: u32) -> RowPositionDescriptor {
        RowPositionDescriptor {
            row_position_type: RowPositionType::Lake,
            row_source_slot: SlotId::new(row_source_slot),
            fetch_ref_slots: vec![SlotId::new(12), SlotId::new(13), SlotId::new(14)],
            lookup_ref_slots: vec![SlotId::new(15)],
        }
    }

    #[test]
    fn row_position_registration_is_idempotent_and_merges_distinct_tuples() {
        let mut context = QueryContext::new(
            QueryId { hi: 71, lo: 72 },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11))]))
            .expect("register lookup descriptor");
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11)), (4, descriptor(21))]))
            .expect("idempotent registration and distinct tuple merge");

        assert_eq!(
            context.row_pos_desc(3).unwrap().row_source_slot,
            SlotId::new(11)
        );
        assert_eq!(
            context.row_pos_desc(4).unwrap().row_source_slot,
            SlotId::new(21)
        );
    }

    #[test]
    fn row_position_registration_rejects_conflicting_tuple_metadata() {
        let mut context = QueryContext::new(
            QueryId { hi: 73, lo: 74 },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
        );
        context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(11))]))
            .expect("register lookup descriptor");

        let error = context
            .merge_row_pos_descs(HashMap::from([(3, descriptor(21)), (4, descriptor(31))]))
            .expect_err("conflicting metadata must fail");
        assert_eq!(error, "conflicting row position descriptor for tuple_id=3");
        assert!(
            context.row_pos_desc(4).is_none(),
            "a rejected registration must not leave partial metadata"
        );
    }
}

#[cfg(test)]
mod runtime_filter_service_lifecycle_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, Weak, mpsc};
    use std::time::{Duration, Instant};

    use arrow::datatypes::DataType;

    use super::{QueryContext, QueryContextManager, QueryContextManagerInner, QueryId};
    use crate::common::types::UniqueId;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, ReductionRequirement,
        RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    };
    use crate::runtime_filter::model::coverage::Coverage;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::install::{
        ConsumerDeployment, ProducerDeployment, RuntimeFilterChannelDeployment,
        RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
        local_participant_install_for_test,
    };
    use crate::runtime_filter::port::producer::{InstallContractErrorKind, InstallOutcome};
    use crate::runtime_filter::service::RuntimeFilterService;

    fn test_manager() -> Arc<QueryContextManager> {
        Arc::new(QueryContextManager {
            inner: Mutex::new(QueryContextManagerInner::default()),
            stopped: AtomicBool::new(false),
        })
    }

    fn query_id(lo: i64) -> QueryId {
        QueryId { hi: 70, lo }
    }

    fn uid(lo: i64) -> UniqueId {
        UniqueId { hi: 70, lo }
    }

    fn participant_install() -> RuntimeFilterParticipantInstall {
        let channel_id = ChannelId::new(1);
        let witness_id = CoverageWitnessId::new(2);
        let deployment = RuntimeFilterChannelDeployment::new(
            channel_id,
            RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int64,
                null_semantics: NullSemantics::NeverMatches,
            },
            RuntimeFilterLifecycle::CompleteOnce,
            Coverage::Leaf(witness_id),
            Coverage::Leaf(witness_id),
            ReductionRequirement::SetUnion,
            BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            CompletionRequirement::ProducerClosed,
            RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 1024,
                deadline_ms: 100,
                max_retries: 0,
            },
            RuntimeFilterCoreBudget::new(8192),
            crate::runtime_filter::port::install::MaterializationPolicy::for_test(),
            BTreeMap::from([(
                BindingId::new(3),
                ProducerDeployment::new(witness_id, BTreeSet::from([uid(30)])),
            )]),
            BTreeMap::from([(
                BindingId::new(4),
                ConsumerDeployment::new(
                    ConsumerActivation::BlockingSnapshot,
                    BTreeSet::from([ArtifactCapability::Membership]),
                    RouteEdgeId::new(5),
                    BTreeSet::from([uid(40)]),
                ),
            )]),
        );
        local_participant_install_for_test(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(6),
            RuntimeFilterParticipantId::new(7),
            BTreeMap::from([(channel_id, deployment)]),
        ))
    }

    fn register(manager: &QueryContextManager, query_id: QueryId) {
        manager
            .get_or_register(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("query context");
    }

    fn service(manager: &QueryContextManager, query_id: QueryId) -> Arc<RuntimeFilterService> {
        let guard = manager.inner.lock().expect("query manager");
        guard
            .active
            .get(&query_id)
            .or_else(|| guard.second_chance.get(&query_id))
            .expect("query context")
            .runtime_filter_service()
    }

    struct LockProbeSink {
        manager: Weak<QueryContextManager>,
        terminal_probe: Mutex<Option<mpsc::SyncSender<bool>>>,
    }

    struct BlockingShutdownSink {
        entered: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl RuntimeFilterEventSink for BlockingShutdownSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }) {
                return;
            }
            self.entered.send(()).expect("shutdown entered");
            self.release
                .lock()
                .expect("shutdown release")
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown released");
        }
    }

    impl RuntimeFilterEventSink for LockProbeSink {
        fn record(&self, event: RuntimeFilterEvent) {
            if !matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }) {
                return;
            }
            let lock_was_free = self
                .manager
                .upgrade()
                .is_some_and(|manager| manager.inner.try_lock().is_ok());
            if let Some(sender) = self.terminal_probe.lock().expect("probe sender").take() {
                let _ = sender.send(lock_was_free);
            }
        }
    }

    fn install_probed_service(
        manager: &Arc<QueryContextManager>,
        query_id: QueryId,
    ) -> (Arc<RuntimeFilterService>, mpsc::Receiver<bool>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let sink = Arc::new(LockProbeSink {
            manager: Arc::downgrade(manager),
            terminal_probe: Mutex::new(Some(sender)),
        });
        let service = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            let service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = service.clone();
            service
        };
        assert_eq!(
            service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );
        (service, receiver)
    }

    fn assert_terminal_probe(receiver: mpsc::Receiver<bool>) {
        assert!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown event"),
            "runtime filter shutdown must run after releasing the manager lock"
        );
    }

    #[test]
    fn query_context_constructs_exactly_one_runtime_filter_service() {
        let context = QueryContext::new(query_id(1), Duration::ZERO, Duration::ZERO);
        let first = context.runtime_filter_service();
        let second = context.runtime_filter_service();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn second_chance_round_trip_preserves_runtime_filter_service() {
        let manager = test_manager();
        let query_id = query_id(2);
        register(&manager, query_id);
        let before = service(&manager, query_id);

        manager.finish_fragment(query_id);
        manager
            .get_or_register(
                query_id,
                true,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("second-chance query context");

        assert!(Arc::ptr_eq(&before, &service(&manager, query_id)));
        assert_eq!(
            before
                .install(participant_install())
                .expect("service remains open"),
            InstallOutcome::Installed
        );
    }

    #[test]
    fn empty_query_service_creates_no_channel_or_event() {
        let query_id = query_id(3);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let context = QueryContext::new(query_id, Duration::ZERO, Duration::ZERO);
        let service = context.runtime_filter_service();
        assert_eq!(
            service
                .install(local_participant_install_for_test(
                    RuntimeFilterInstallView::new(
                        DeploymentEpoch::new(1),
                        RuntimeFilterParticipantId::new(0),
                        BTreeMap::new(),
                    ),
                ))
                .expect("empty view"),
            InstallOutcome::IgnoredEmpty
        );
        let snapshot = registry.snapshot(query_key).expect("query lifecycle entry");
        assert!(snapshot.filters.is_empty());
        assert!(snapshot.channel_events.is_empty());
        registry.remove_query(query_key);
    }

    #[test]
    fn cancel_query_cancels_service_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(4);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);

        manager.cancel_query(query_id, "cancelled".to_string());

        assert_terminal_probe(receiver);
    }

    #[test]
    fn abort_query_cancels_service_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(5);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);

        manager.abort_query(query_id);

        assert_terminal_probe(receiver);
    }

    #[test]
    fn dead_finish_shuts_down_and_drops_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(6);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("active query")
            .total_fragments = Some(1);

        manager.finish_fragment(query_id);

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .active
                .contains_key(&query_id)
        );
    }

    #[test]
    fn clean_expired_shuts_down_second_chance_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(7);
        register(&manager, query_id);
        let (_service, receiver) = install_probed_service(&manager, query_id);
        {
            let mut guard = manager.inner.lock().expect("query manager");
            let mut context = guard.active.remove(&query_id).expect("active query");
            context.num_active_fragments = 0;
            context.delivery_deadline = Instant::now() - Duration::from_millis(1);
            guard.second_chance.insert(query_id, context);
        }

        manager.clean_expired();

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .second_chance
                .contains_key(&query_id)
        );
    }

    #[test]
    fn clean_expired_shuts_down_expired_claim_only_active_context_after_releasing_manager_lock() {
        let manager = test_manager();
        let query_id = query_id(72);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("runtime-filter RPC claim-only context");
        let (service, receiver) = install_probed_service(&manager, query_id);
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("claim-only active query")
            .query_deadline = Instant::now() - Duration::from_millis(1);

        manager.clean_expired();

        assert_terminal_probe(receiver);
        assert!(
            !manager
                .inner
                .lock()
                .expect("query manager")
                .active
                .contains_key(&query_id)
        );
        assert!(registry.snapshot(query_key).is_none());
        let error = service
            .install(participant_install())
            .expect_err("expired claim-only service must remain closed");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }

    #[test]
    fn clean_expired_preserves_unexpired_claim_only_active_context() {
        let manager = test_manager();
        let query_id = query_id(73);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("runtime-filter RPC claim-only context");
        let before = service(&manager, query_id);

        manager.clean_expired();

        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("claim remains active");
        assert_eq!(context.num_active_fragments, 0);
        assert_eq!(
            context.legacy_runtime_filter_execution_claim(),
            super::LegacyRuntimeFilterExecutionClaim::NativeDisabled
        );
        assert!(Arc::ptr_eq(&before, &context.runtime_filter_service()));
    }

    #[test]
    fn clean_expired_preserves_active_fragment_past_query_deadline() {
        let manager = test_manager();
        let query_id = query_id(74);
        manager
            .get_or_register_native(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("active Native fragment");
        manager
            .inner
            .lock()
            .expect("query manager")
            .active
            .get_mut(&query_id)
            .expect("active query")
            .query_deadline = Instant::now() - Duration::from_millis(1);

        manager.clean_expired();

        let guard = manager.inner.lock().expect("query manager");
        let context = guard
            .active
            .get(&query_id)
            .expect("active fragment retained");
        assert_eq!(context.num_active_fragments, 1);
    }

    #[test]
    fn concurrent_native_fragment_recreates_context_while_expired_claim_shuts_down() {
        let manager = test_manager();
        let query_id = query_id(75);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("claim-only Native context");

        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(BlockingShutdownSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let old_service = {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("claim-only query");
            context.query_deadline = Instant::now() - Duration::from_millis(1);
            let service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = Arc::clone(&service);
            service
        };
        assert_eq!(
            old_service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );

        let cleaner = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || manager.clean_expired())
        };
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expired claim shutdown must start");

        manager
            .get_or_register_native(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("legal concurrent Native fragment");
        release_tx.send(()).expect("release old shutdown");
        cleaner.join().expect("cleaner");

        let guard = manager.inner.lock().expect("query manager");
        let context = guard.active.get(&query_id).expect("replacement context");
        assert_eq!(context.num_active_fragments, 1);
        assert_eq!(
            context.legacy_runtime_filter_execution_claim(),
            super::LegacyRuntimeFilterExecutionClaim::NativeDisabled
        );
        assert!(registry.snapshot(query_key).is_some());
        registry.remove_query(query_key);
    }

    #[test]
    fn clean_expired_preserves_lifecycle_recreated_while_old_context_shuts_down() {
        let manager = test_manager();
        let query_id = query_id(71);
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        register(&manager, query_id);

        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let sink = Arc::new(BlockingShutdownSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        {
            let mut guard = manager.inner.lock().expect("query manager");
            let context = guard.active.get_mut(&query_id).expect("active query");
            let old_service = Arc::new(RuntimeFilterService::new_for_query(
                uid(query_id.lo),
                sink,
                &context.mem_tracker,
            ));
            context.runtime_filter_service = old_service.clone();
            assert_eq!(
                old_service
                    .install(participant_install())
                    .expect("valid install"),
                InstallOutcome::Installed
            );

            let mut context = guard.active.remove(&query_id).expect("active query");
            context.num_active_fragments = 0;
            context.delivery_deadline = Instant::now() - Duration::from_millis(1);
            guard.second_chance.insert(query_id, context);
        }

        let cleaner = {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || manager.clean_expired())
        };
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("old shutdown must block in the sink");

        manager
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("replacement query context");
        release_tx.send(()).expect("release old shutdown");
        cleaner.join().expect("cleaner");

        assert!(
            registry.snapshot(query_key).is_some(),
            "old cleanup must not remove the replacement context lifecycle"
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn late_service_handle_cannot_recreate_deployment_after_shutdown() {
        let manager = test_manager();
        let query_id = query_id(8);
        register(&manager, query_id);
        let (service, receiver) = install_probed_service(&manager, query_id);
        manager.cancel_query(query_id, "cancelled".to_string());
        assert_terminal_probe(receiver);

        let error = service
            .install(participant_install())
            .expect_err("closed service");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }

    #[test]
    fn query_context_drop_shuts_down_service_retained_by_external_handle() {
        let context = QueryContext::new(query_id(9), Duration::ZERO, Duration::ZERO);
        let service = context.runtime_filter_service();
        assert_eq!(
            service
                .install(participant_install())
                .expect("valid install"),
            InstallOutcome::Installed
        );

        drop(context);

        let error = service
            .install(participant_install())
            .expect_err("context drop must close retained service");
        assert_eq!(error.kind(), InstallContractErrorKind::ServiceClosed);
    }
}
