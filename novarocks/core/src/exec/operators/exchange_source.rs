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
//! Exchange source for receiving distributed upstream data.
//!
//! Responsibilities:
//! - Fetches remote stream pages from exchange service and reconstructs chunks for local pipeline processing.
//! - Handles end-of-stream coordination, sender completion tracking, and error propagation.
//!
//! Key exported interfaces:
//! - Types: `ExchangeSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::operators::runtime_filter::NativeRuntimeFilterConsumerSet;
use crate::exec::pipeline::binding::ExchangeBinding;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, filter_chunk_by_in_filters_with_exprs,
    filter_chunk_by_membership_filters_with_exprs,
};
use crate::novarocks_logging::debug;
use crate::runtime::exchange;
use crate::runtime::profile::ProfileUnit;
use crate::runtime::runtime_filter_hub::{
    AcquireProgress, AcquiredRuntimeFilters, RuntimeFilterHub, RuntimeFilterProbe,
    RuntimeFilterSnapshot,
};
use crate::runtime::runtime_filter_observability::{
    QueryKey, RfLifecycleHandle, RuntimeFilterLifecycleRegistry,
};
use crate::runtime::runtime_state::RuntimeState;

static EXCHANGE_SOURCE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_source_ready() -> bool {
    let count = EXCHANGE_SOURCE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    count.is_multiple_of(1024)
}

const JOIN_RUNTIME_FILTER_TIME: &str = "JoinRuntimeFilterTime";
const JOIN_RUNTIME_FILTER_HASH_TIME: &str = "JoinRuntimeFilterHashTime";
const JOIN_RUNTIME_FILTER_INPUT_ROWS: &str = "JoinRuntimeFilterInputRows";
const JOIN_RUNTIME_FILTER_OUTPUT_ROWS: &str = "JoinRuntimeFilterOutputRows";
const JOIN_RUNTIME_FILTER_EVALUATE: &str = "JoinRuntimeFilterEvaluate";
const RUNTIME_FILTER_NUM: &str = "RuntimeFilterNum";
const RUNTIME_IN_FILTER_NUM: &str = "RuntimeInFilterNum";
const RUNTIME_FILTER_PLANNED: &str = "RuntimeFilterPlanned";
const RUNTIME_FILTER_COMPLETE: &str = "RuntimeFilterComplete";
const RUNTIME_FILTER_UNAVAILABLE: &str = "RuntimeFilterUnavailable";

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    runtime_filter_execution: ExchangeSourceRuntimeFilterExecution,
    arena: Arc<ExprArena>,
}

enum ExchangeSourceRuntimeFilterExecution {
    Native {
        consumers: NativeRuntimeFilterConsumerSet,
    },
    #[cfg(feature = "compat")]
    Compat {
        runtime_filter_specs: Vec<crate::exec::node::RuntimeFilterProbeSpec>,
        runtime_filter_exprs: HashMap<i32, ExprId>,
        runtime_filters_expected: usize,
        local_rf_waiting_set: Vec<i32>,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
    },
}

impl ExchangeSourceFactory {
    pub(crate) fn new_native(
        node: ExchangeSourceNode,
        binding: ExchangeBinding,
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let name = node.profile_name();
        exchange::register_expected_chunk_schema(
            binding.key,
            binding.expected_senders,
            node.expected_chunk_schema(),
        )?;
        let consumers = NativeRuntimeFilterConsumerSet::from_plan(
            node.native_runtime_filter_specs(),
            Arc::clone(&arena),
        )?;
        Ok(Self {
            name,
            node,
            binding,
            runtime_filter_execution: ExchangeSourceRuntimeFilterExecution::Native { consumers },
            arena,
        })
    }

    #[cfg(test)]
    fn new_native_with_consumers_for_test(
        node: ExchangeSourceNode,
        binding: ExchangeBinding,
        arena: Arc<ExprArena>,
        consumers: NativeRuntimeFilterConsumerSet,
    ) -> Result<Self, String> {
        let name = node.profile_name();
        exchange::register_expected_chunk_schema(
            binding.key,
            binding.expected_senders,
            node.expected_chunk_schema(),
        )?;
        Ok(Self {
            name,
            node,
            binding,
            runtime_filter_execution: ExchangeSourceRuntimeFilterExecution::Native { consumers },
            arena,
        })
    }

    #[cfg(feature = "compat")]
    pub(crate) fn new_compat(
        node: ExchangeSourceNode,
        binding: ExchangeBinding,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let name = node.profile_name();
        exchange::register_expected_chunk_schema(
            binding.key,
            binding.expected_senders,
            node.expected_chunk_schema(),
        )?;
        let runtime_filter_specs = node.runtime_filter_specs().to_vec();
        let runtime_filter_exprs = runtime_filter_specs
            .iter()
            .map(|spec| (spec.filter_id, spec.expr_id))
            .collect();
        let runtime_filters_expected = runtime_filter_specs.len();
        if runtime_filters_expected > 0 {
            runtime_filter_hub.register_probe_specs(node.node_id, &runtime_filter_specs);
        }
        let local_rf_waiting_set = node.local_rf_waiting_set().to_vec();
        Ok(Self {
            name,
            node,
            binding,
            runtime_filter_execution: ExchangeSourceRuntimeFilterExecution::Compat {
                runtime_filter_specs,
                runtime_filter_exprs,
                runtime_filters_expected,
                local_rf_waiting_set,
                runtime_filter_hub,
            },
            arena,
        })
    }

    #[cfg(feature = "compat")]
    fn local_rf_deps(&self) -> Vec<crate::exec::pipeline::dependency::DependencyHandle> {
        let ExchangeSourceRuntimeFilterExecution::Compat {
            local_rf_waiting_set,
            runtime_filter_hub,
            ..
        } = &self.runtime_filter_execution
        else {
            return Vec::new();
        };
        if local_rf_waiting_set.is_empty() {
            return Vec::new();
        }
        let mut deps = Vec::new();
        let mut seen = HashSet::new();
        for node_id in local_rf_waiting_set {
            if seen.insert(*node_id) {
                if let Some(dep) = runtime_filter_hub.local_dependency_if_exists(*node_id) {
                    deps.push(dep);
                } else {
                    debug!(
                        "exchange skip unknown local RF dependency: node_id={} waiting_build_node_id={}",
                        self.node.node_id, node_id
                    );
                }
            }
        }
        deps
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let (runtime_filter_probe, local_rf_deps, runtime_filter_exprs, runtime_filters_expected) =
            match &self.runtime_filter_execution {
                ExchangeSourceRuntimeFilterExecution::Native { .. } => {
                    (None, Vec::new(), HashMap::new(), 0)
                }
                #[cfg(feature = "compat")]
                ExchangeSourceRuntimeFilterExecution::Compat {
                    runtime_filter_exprs,
                    runtime_filters_expected,
                    local_rf_waiting_set,
                    runtime_filter_hub,
                    ..
                } => {
                    if !local_rf_waiting_set.is_empty() {
                        debug!(
                            "ExchangeSource local RF wait: finst={} node_id={} driver_id={} waiting_set={:?}",
                            self.binding.key.finst_uuid(),
                            self.node.node_id,
                            driver_id,
                            local_rf_waiting_set
                        );
                    }
                    let probe =
                        (*runtime_filters_expected > 0).then(|| ExchangeRuntimeFilterProbe {
                            probe: runtime_filter_hub.register_probe(self.node.node_id),
                        });
                    (
                        probe,
                        self.local_rf_deps(),
                        runtime_filter_exprs.clone(),
                        *runtime_filters_expected,
                    )
                }
            };
        Box::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node.clone(),
            binding: self.binding,
            driver_id,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            runtime_filter_probe,
            local_rf_deps,
            runtime_filter_exprs,
            runtime_filters_expected,
            runtime_filter_lifecycle_handles: HashMap::new(),
            acquired: None,
            runtime_filters_loaded: false,
            arena: Arc::clone(&self.arena),
            native_runtime_filter_consumers: match &self.runtime_filter_execution {
                ExchangeSourceRuntimeFilterExecution::Native { consumers } => {
                    Some(consumers.clone())
                }
                #[cfg(feature = "compat")]
                ExchangeSourceRuntimeFilterExecution::Compat { .. } => None,
            },
            profiles: None,
            receiver_mem_tracker_ready: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceNode,
    binding: ExchangeBinding,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    start: Option<Instant>,
    finished: bool,
    logged_first_pull: bool,
    logged_first_none: bool,
    runtime_filter_probe: Option<ExchangeRuntimeFilterProbe>,
    local_rf_deps: Vec<crate::exec::pipeline::dependency::DependencyHandle>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    runtime_filter_lifecycle_handles: HashMap<i32, RfLifecycleHandle>,
    acquired: Option<AcquiredRuntimeFilters>,
    runtime_filters_loaded: bool,
    arena: Arc<ExprArena>,
    native_runtime_filter_consumers: Option<NativeRuntimeFilterConsumerSet>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    receiver_mem_tracker_ready: bool,
}

struct ExchangeRuntimeFilterProbe {
    probe: RuntimeFilterProbe,
}

impl ExchangeRuntimeFilterProbe {
    fn dependency_or_timeout(&self) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        match self.poll_acquire() {
            AcquireProgress::Pending(dep) => Some(dep),
            AcquireProgress::Resolved(_) => None,
        }
    }

    fn poll_acquire(&self) -> crate::runtime::runtime_filter_hub::AcquireProgress {
        self.probe.poll_acquire(false)
    }

    fn mark_ready(&self) -> Option<Duration> {
        self.probe.mark_ready()
    }
}

impl ExchangeSourceOperator {
    fn local_rf_dependency(&self) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        for dep in &self.local_rf_deps {
            if !dep.is_ready() {
                return Some(dep.clone());
            }
        }
        None
    }

    fn bind_runtime_filter_lifecycle(&mut self, state: &RuntimeState) {
        let Some(query_id) = state.query_id() else {
            self.runtime_filter_lifecycle_handles.clear();
            return;
        };
        let recorder = RuntimeFilterLifecycleRegistry::global()
            .recorder(QueryKey::from_hi_lo(query_id.hi, query_id.lo));
        self.runtime_filter_lifecycle_handles = self
            .runtime_filter_exprs
            .keys()
            .map(|filter_id| (*filter_id, recorder.filter(*filter_id)))
            .collect();
    }

    fn record_runtime_filter_acquired(&self, outcome: &str, latency_ns: i64) {
        for handle in self.runtime_filter_lifecycle_handles.values() {
            handle.acquired(outcome, latency_ns);
        }
    }

    fn record_runtime_filter_applied(&self, filter_id: i32, input_rows: usize, output_rows: usize) {
        if let Some(handle) = self.runtime_filter_lifecycle_handles.get(&filter_id) {
            handle.applied(input_rows as i64, output_rows as i64, 1);
        }
    }

    fn apply_complete_runtime_filters(
        &self,
        snapshot: &RuntimeFilterSnapshot,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        let mut current = Some(chunk);
        for filter in snapshot.membership_filters() {
            let Some(chunk) = current else {
                return Ok(None);
            };
            let input_rows = chunk.len();
            let filter_id = filter.filter_id();
            current = filter_chunk_by_membership_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                std::slice::from_ref(filter),
                chunk,
            )?;
            let output_rows = current.as_ref().map(|chunk| chunk.len()).unwrap_or(0);
            self.record_runtime_filter_applied(filter_id, input_rows, output_rows);
        }
        for filter in snapshot.in_filters() {
            let Some(chunk) = current else {
                return Ok(None);
            };
            let input_rows = chunk.len();
            let filter_id = filter.filter_id();
            current = filter_chunk_by_in_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                std::slice::from_ref(filter),
                chunk,
            )?;
            let output_rows = current.as_ref().map(|chunk| chunk.len()).unwrap_or(0);
            self.record_runtime_filter_applied(filter_id, input_rows, output_rows);
        }
        Ok(current)
    }
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver =
            exchange::get_receiver_handle(self.binding.key, self.binding.expected_senders)?;
        self.receiver = Some(receiver);
        debug!(
            "ExchangeSource prepared: finst={} node_id={} expected_senders={} timeout={:?}",
            self.binding.key.finst_uuid(),
            self.node.node_id,
            self.binding.expected_senders,
            self.node.timeout
        );
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.bind_runtime_filter_lifecycle(state);
        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
            consumers.bind(state)?;
        }
        Ok(())
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for ExchangeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if let Some(start) = self.start
            && start.elapsed() >= self.node.timeout
        {
            if should_log_exchange_source_ready() {
                debug!(
                    "ExchangeSource has_output due to timeout: finst={} node_id={} elapsed={:?} timeout={:?}",
                    self.binding.key.finst_uuid(),
                    self.node.node_id,
                    start.elapsed(),
                    self.node.timeout
                );
            }
            return true;
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return false;
        };
        let ready = receiver.has_output_or_finished(self.binding.expected_senders);
        if ready && should_log_exchange_source_ready() {
            debug!(
                "ExchangeSource has_output due to receiver: finst={} node_id={} expected_senders={}",
                self.binding.key.finst_uuid(),
                self.node.node_id,
                self.binding.expected_senders
            );
        }
        ready
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("exchange source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        if self.receiver.is_none() {
            return Err("exchange source operator not prepared".to_string());
        }

        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = state.mem_tracker() {
                let _ = exchange::ensure_receiver_mem_tracker(self.binding.key, &root)?;
            }
        }

        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.node_id, self.driver_id
            );
        }

        self.load_runtime_filters_if_ready();

        let start = self.start.get_or_insert_with(Instant::now);
        if start.elapsed() >= self.node.timeout {
            debug!(
                "ExchangeSource timeout waiting for senders: finst_id={} node_id={} elapsed={:?} timeout={:?}",
                self.binding.key.finst_uuid(),
                self.node.node_id,
                start.elapsed(),
                self.node.timeout
            );
            return Err(format!(
                "exchange timeout waiting for senders: finst_id={} node_id={}",
                self.binding.key.finst_uuid(),
                self.node.node_id
            ));
        }

        loop {
            let out = {
                let receiver = self.receiver.as_ref().expect("receiver");
                receiver
                    .try_pop_next_with_stats(self.binding.expected_senders)
                    .map_err(|e| e.to_string())?
            };

            match out {
                Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                    let input_rows = chunk.len();
                    let chunk =
                        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
                            consumers.acquire_configured()?;
                            let Some(chunk) =
                                consumers.apply_chunk_profiled(chunk, self.profiles.as_ref())?
                            else {
                                continue;
                            };
                            chunk
                        } else {
                            chunk
                        };
                    if let Some(filtered) = self.apply_runtime_filters(chunk)? {
                        if filtered.is_empty() {
                            debug!(
                                "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                                self.node.node_id, self.driver_id, input_rows
                            );
                            continue;
                        }
                        debug!(
                            "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                            self.node.node_id,
                            self.driver_id,
                            input_rows,
                            filtered.len()
                        );
                        return Ok(Some(filtered));
                    } else {
                        debug!(
                            "ExchangeSource filtered to None: node_id={} driver_id={} input_rows={}",
                            self.node.node_id, self.driver_id, input_rows
                        );
                    }
                    continue;
                }
                Some(exchange::ExchangePopResult::Finished(stats)) => {
                    debug!(
                        "ExchangeSource finished: finst={} node_id={} driver_id={} request_received={} bytes_received={} deserialize_ns={} chunks_received={} rows_received={}",
                        self.binding.key.finst_uuid(),
                        self.node.node_id,
                        self.driver_id,
                        stats.request_received,
                        stats.bytes_received,
                        stats.deserialize_ns,
                        stats.chunks_received,
                        stats.rows_received
                    );
                    self.finished = true;
                    return Ok(None);
                }
                None => {
                    if !self.logged_first_none {
                        self.logged_first_none = true;
                        debug!(
                            "ExchangeSource no output yet: node_id={} driver_id={}",
                            self.node.node_id, self.driver_id
                        );
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        if let Some(dep) = self.local_rf_dependency() {
            return Some(dep);
        }
        if let Some(rf) = self.runtime_filter_probe.as_ref()
            && let Some(dep) = rf.dependency_or_timeout()
            && !dep.is_ready()
        {
            if self.finished {
                return None;
            }
            if let Some(receiver) = self.receiver.as_ref() {
                // If data is already available, let the driver consume it instead of
                // waiting on runtime filters.
                if receiver.has_output_or_finished(self.binding.expected_senders) {
                    return None;
                }
            }
            return Some(dep);
        }
        None
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        self.receiver.as_ref().map(|r| r.observable())
    }
}

impl ExchangeSourceOperator {
    fn load_runtime_filters_if_ready(&mut self) {
        let Some(rf) = self.runtime_filter_probe.as_ref() else {
            self.acquired = None;
            self.runtime_filters_loaded = true;
            return;
        };
        if self.runtime_filters_loaded {
            return;
        }
        if self.runtime_filters_expected == 0 {
            self.acquired = None;
            self.runtime_filters_loaded = true;
            return;
        }
        let acquired = match rf.poll_acquire() {
            AcquireProgress::Pending(_) => return,
            AcquireProgress::Resolved(acquired) => acquired,
        };
        if let Some(profile) = self.profiles.as_ref() {
            profile.common.add_timer(JOIN_RUNTIME_FILTER_TIME);
            profile.common.add_timer(JOIN_RUNTIME_FILTER_HASH_TIME);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_INPUT_ROWS, ProfileUnit::Unit);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_OUTPUT_ROWS, ProfileUnit::Unit);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_EVALUATE, ProfileUnit::Unit);
            profile
                .common
                .add_counter(RUNTIME_FILTER_NUM, ProfileUnit::Unit);
            profile
                .common
                .add_counter(RUNTIME_IN_FILTER_NUM, ProfileUnit::Unit);
        }
        let (filter_num, in_filter_num) = match &acquired {
            AcquiredRuntimeFilters::Complete(snapshot) => {
                let ready_latency_ns = rf
                    .mark_ready()
                    .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as i64);
                self.record_runtime_filter_acquired("complete", ready_latency_ns.unwrap_or(0));
                if let Some(latency_ns) = ready_latency_ns
                    && let Some(profile) = self.profiles.as_ref()
                {
                    for filter in snapshot.in_filters() {
                        let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                        profile
                            .common
                            .counter_set(&name, ProfileUnit::TimeNs, latency_ns);
                    }
                    for filter in snapshot.membership_filters() {
                        let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                        profile
                            .common
                            .counter_set(&name, ProfileUnit::TimeNs, latency_ns);
                    }
                }
                self.log_runtime_filters_loaded(
                    snapshot.in_filters(),
                    snapshot.membership_filters(),
                );
                (
                    snapshot.membership_filters().len(),
                    snapshot.in_filters().len(),
                )
            }
            AcquiredRuntimeFilters::Unavailable(reason) => {
                let outcome = format!("unavailable:{reason:?}");
                self.record_runtime_filter_acquired(&outcome, 0);
                debug!(
                    "exchange runtime filters unavailable: node_id={} expected={} reason={:?}",
                    self.node.node_id, self.runtime_filters_expected, reason
                );
                (0, 0)
            }
        };
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .counter_set(RUNTIME_FILTER_NUM, ProfileUnit::Unit, filter_num as i64);
            profile.common.counter_set(
                RUNTIME_IN_FILTER_NUM,
                ProfileUnit::Unit,
                in_filter_num as i64,
            );
            profile
                .common
                .counter_set_unit(RUNTIME_FILTER_PLANNED, self.runtime_filters_expected as i64);
            match &acquired {
                AcquiredRuntimeFilters::Complete(snapshot) => {
                    let complete =
                        snapshot.in_filters().len() + snapshot.membership_filters().len();
                    profile
                        .common
                        .counter_set_unit(RUNTIME_FILTER_COMPLETE, complete as i64);
                    profile
                        .common
                        .counter_set_unit(RUNTIME_FILTER_UNAVAILABLE, 0);
                }
                AcquiredRuntimeFilters::Unavailable(_) => {
                    profile.common.counter_set_unit(RUNTIME_FILTER_COMPLETE, 0);
                    profile.common.counter_set_unit(
                        RUNTIME_FILTER_UNAVAILABLE,
                        self.runtime_filters_expected as i64,
                    );
                }
            }
        }
        self.acquired = Some(acquired);
        self.runtime_filters_loaded = true;
    }

    fn log_runtime_filters_loaded(
        &self,
        in_filters: &[Arc<RuntimeInFilter>],
        membership_filters: &[Arc<RuntimeMembershipFilter>],
    ) {
        debug!(
            "exchange runtime filters loaded: node_id={} expected={} in_filters={} membership_filters={}",
            self.node.node_id,
            self.runtime_filters_expected,
            in_filters.len(),
            membership_filters.len()
        );
        for filter in in_filters {
            let filter = filter.as_ref();
            debug!(
                "exchange runtime in filter: node_id={} filter_id={} slot_id={:?} empty={}",
                self.node.node_id,
                filter.filter_id(),
                filter.slot_id(),
                filter.is_empty()
            );
        }
        for filter in membership_filters {
            let filter = filter.as_ref();
            let kind = match filter {
                RuntimeMembershipFilter::Bloom(_) => "bloom",
                RuntimeMembershipFilter::Bitset(_) => "bitset",
                RuntimeMembershipFilter::Empty(_) => "empty",
            };
            debug!(
                "exchange runtime membership filter: node_id={} filter_id={} kind={} slot_id={:?} ltype={:?} size={} has_null={} join_mode={} empty={}",
                self.node.node_id,
                filter.filter_id(),
                kind,
                filter.slot_id(),
                filter.ltype(),
                filter.size(),
                filter.has_null(),
                filter.join_mode(),
                filter.is_empty()
            );
        }
    }

    fn apply_runtime_filters(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let Some(acquired) = self.acquired.as_ref() else {
            return Ok(Some(chunk));
        };
        let input_rows = chunk.len();
        let AcquiredRuntimeFilters::Complete(snapshot) = acquired else {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_INPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
            }
            return Ok(Some(chunk));
        };
        if snapshot.is_empty() {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_INPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
            }
            return Ok(Some(chunk));
        }
        let filters_len =
            (snapshot.in_filters().len() + snapshot.membership_filters().len()) as i64;
        let result = if let Some(profile) = self.profiles.as_ref() {
            let _timer = profile.common.scoped_timer(JOIN_RUNTIME_FILTER_TIME);
            self.apply_complete_runtime_filters(snapshot, chunk)
        } else {
            self.apply_complete_runtime_filters(snapshot, chunk)
        }?;
        if let Some(profile) = self.profiles.as_ref() {
            let output_rows = result.as_ref().map(|c| c.len()).unwrap_or(0) as i64;
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_INPUT_ROWS,
                ProfileUnit::Unit,
                input_rows as i64,
            );
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                ProfileUnit::Unit,
                output_rows,
            );
            if filters_len > 0 {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_EVALUATE,
                    ProfileUnit::Unit,
                    filters_len,
                );
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::RuntimeFilterProbeSpec;
    use crate::exec::node::join::CompatJoinRuntimeFilterSpec;
    use crate::exec::pipeline::binding::ExchangeBinding;
    use crate::exec::runtime_filter::{
        LocalRuntimeInFilterSet, RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeBloomFilter,
        RuntimeEmptyFilter, RuntimeFilterType, RuntimeMembershipFilter, RuntimeMinMaxFilter,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::runtime::runtime_state::RuntimeState;

    fn int32_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).expect("test batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn int32_values(chunk: &Chunk) -> Vec<i32> {
        let array = chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("slot column");
        let ints = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        (0..ints.len()).map(|row| ints.value(row)).collect()
    }

    fn in_filter(filter_id: i32, values: Vec<i32>) -> Vec<RuntimeInFilter> {
        let spec = CompatJoinRuntimeFilterSpec {
            filter_id,
            expr_order: 0,
            probe_expr_id: crate::exec::expr::ExprId(0),
            build_expr_id: crate::exec::expr::ExprId(0),
            probe_slot_id: SlotId::new(1),
            build_data_type: DataType::Int32,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        };
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let mut set =
            LocalRuntimeInFilterSet::new(std::slice::from_ref(&spec), std::slice::from_ref(&array))
                .expect("in filter set");
        set.add_build_arrays(std::slice::from_ref(&array))
            .expect("add build values");
        set.into_filters()
    }

    fn pruning_membership_filter(filter_id: i32, values: Vec<i32>) -> RuntimeMembershipFilter {
        let build_values = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                filter_id,
                SlotId::new(1),
                RuntimeFilterType::Int32,
                &build_values,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .expect("build bloom filter"),
        )
    }

    fn passthrough_membership_filter(filter_id: i32) -> RuntimeMembershipFilter {
        let min_max =
            RuntimeMinMaxFilter::full_range(RuntimeFilterType::Int32).expect("min/max range");
        RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
            filter_id,
            SlotId::new(1),
            RuntimeFilterType::Int32,
            false,
            RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            0,
            min_max,
        ))
    }

    #[test]
    fn native_exchange_source_applies_the_shared_membership_mask() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_001,
            finst_id_lo: 91_002,
            node_id: 91_003,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
        };
        let factory = ExchangeSourceFactory::new_native_with_consumers_for_test(
            node, binding, arena, consumers,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        exchange::push_chunks(key, 0, 0, vec![int32_chunk(vec![1, 2, 3, 4])], true);

        let output = source
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(int32_values(&output), vec![2, 4]);
        exchange::remove_fragment(key.finst_id_hi, key.finst_id_lo);
    }

    #[test]
    fn native_exchange_source_continues_after_first_chunk_is_fully_filtered() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let key = exchange::ExchangeKey {
            finst_id_hi: 91_011,
            finst_id_lo: 91_012,
            node_id: 91_013,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let node = ExchangeSourceNode::new(
            key.node_id,
            Duration::from_secs(2),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .unwrap(),
        );
        let binding = ExchangeBinding {
            key,
            expected_senders: 1,
        };
        let factory = ExchangeSourceFactory::new_native_with_consumers_for_test(
            node, binding, arena, consumers,
        )
        .unwrap();
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        exchange::push_chunks(
            key,
            0,
            0,
            vec![int32_chunk(vec![1, 3]), int32_chunk(vec![2, 4])],
            true,
        );

        let output = source
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
            .unwrap();
        assert_eq!(int32_values(&output), vec![2, 4]);
        exchange::remove_fragment(key.finst_id_hi, key.finst_id_lo);
    }

    #[test]
    fn native_factory_registers_receiver_from_binding_not_node() {
        // node_id (5) deliberately differs from the binding's expected_senders (4):
        // a factory that read the sender count from the static node instead of the
        // instance binding would register the wrong receiver state (or use the wrong
        // ExchangeKey), so this pins the cutover contract.
        let key = exchange::ExchangeKey {
            finst_id_hi: 77_001,
            finst_id_lo: 88_002,
            node_id: 5,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        let node = ExchangeSourceNode::new(5, Duration::from_secs(60), chunk_schema);
        let binding = ExchangeBinding {
            key,
            expected_senders: 4,
        };
        ExchangeSourceFactory::new_native(node, binding, Arc::new(ExprArena::default()))
            .expect("native exchange factory");
        let snapshot =
            exchange::snapshot_receiver_state(key).expect("factory registered the receiver state");
        assert_eq!(snapshot.expected_senders, 4);
        exchange::cancel_exchange_key(key);
    }

    #[test]
    fn exchange_runtime_filters_use_frozen_snapshot_after_load() {
        let query_id = QueryId { hi: 30_004, lo: 7 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            crate::exec::pipeline::dependency::DependencyManager::new(),
            query_id,
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        hub.register_probe_specs(
            42,
            &[RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let probe = hub.register_probe(42);
        hub.publish_filters(&[], &[pruning_membership_filter(7, vec![1, 2, 3])]);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let key = exchange::ExchangeKey {
            finst_id_hi: 0,
            finst_id_lo: 0,
            node_id: 42,
        };
        let node = ExchangeSourceNode::new(
            42,
            Duration::from_secs(60),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema"),
        );
        let mut operator = ExchangeSourceOperator {
            name: "exchange".to_string(),
            node,
            binding: ExchangeBinding {
                key,
                expected_senders: 1,
            },
            driver_id: 0,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            runtime_filter_probe: Some(ExchangeRuntimeFilterProbe { probe }),
            local_rf_deps: Vec::new(),
            runtime_filter_exprs: HashMap::from([(7, expr)]),
            runtime_filters_expected: 1,
            runtime_filter_lifecycle_handles: HashMap::new(),
            acquired: None,
            runtime_filters_loaded: false,
            arena: Arc::new(arena),
            native_runtime_filter_consumers: None,
            profiles: Some(crate::runtime::profile::OperatorProfiles::new(
                crate::runtime::profile::RuntimeProfile::new("exchange"),
            )),
            receiver_mem_tracker_ready: false,
        };
        let runtime_state = RuntimeState::new(
            None,
            None,
            Some(query_id),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        operator
            .bind_runtime_state(&runtime_state)
            .expect("bind runtime state");
        operator.load_runtime_filters_if_ready();
        let profiles = operator.profiles.as_ref().expect("test profiles");
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_PLANNED),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_COMPLETE),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_UNAVAILABLE),
            Some(0)
        );

        let late_in_filters = in_filter(7, vec![2]);
        hub.publish_filters(&late_in_filters, &[]);

        let filtered = operator
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("frozen snapshot should not include the late in-filter");
        assert_eq!(int32_values(&filtered), vec![1, 2, 3]);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&7).expect("filter lifecycle");
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.outcome.as_str()),
            Some("complete")
        );
        assert_eq!(filter.applied_input_rows(), 4);
        assert_eq!(filter.applied_output_rows(), 3);
        assert_eq!(filter.applied_evals(), 1);
        registry.remove_query(query_key);
    }

    #[test]
    fn exchange_runtime_filter_lifecycle_records_in_apply() {
        let query_id = QueryId { hi: 30_005, lo: 8 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            crate::exec::pipeline::dependency::DependencyManager::new(),
            query_id,
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        hub.register_probe_specs(
            42,
            &[RuntimeFilterProbeSpec {
                filter_id: 8,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let probe = hub.register_probe(42);
        let in_filters = in_filter(8, vec![1, 3]);
        hub.publish_filters(&in_filters, &[passthrough_membership_filter(8)]);

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let key = exchange::ExchangeKey {
            finst_id_hi: 0,
            finst_id_lo: 0,
            node_id: 42,
        };
        let node = ExchangeSourceNode::new(
            42,
            Duration::from_secs(60),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema"),
        );
        let mut operator = ExchangeSourceOperator {
            name: "exchange".to_string(),
            node,
            binding: ExchangeBinding {
                key,
                expected_senders: 1,
            },
            driver_id: 0,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            runtime_filter_probe: Some(ExchangeRuntimeFilterProbe { probe }),
            local_rf_deps: Vec::new(),
            runtime_filter_exprs: HashMap::from([(8, expr)]),
            runtime_filters_expected: 1,
            runtime_filter_lifecycle_handles: HashMap::new(),
            acquired: None,
            runtime_filters_loaded: false,
            arena: Arc::new(arena),
            native_runtime_filter_consumers: None,
            profiles: None,
            receiver_mem_tracker_ready: false,
        };
        let runtime_state = RuntimeState::new(
            None,
            None,
            Some(query_id),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        operator
            .bind_runtime_state(&runtime_state)
            .expect("bind runtime state");
        operator.load_runtime_filters_if_ready();

        let filtered = operator
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("in filter should keep rows");
        assert_eq!(int32_values(&filtered), vec![1, 3]);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&8).expect("filter lifecycle");
        assert_eq!(filter.applied_input_rows(), 8);
        assert_eq!(filter.applied_output_rows(), 6);
        assert_eq!(filter.applied_evals(), 2);
        registry.remove_query(query_key);
    }

    #[test]
    fn exchange_runtime_filter_lifecycle_records_unavailable_acquired() {
        let query_id = QueryId { hi: 30_006, lo: 9 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            crate::exec::pipeline::dependency::DependencyManager::new(),
            query_id,
        );
        hub.set_wait_timeouts(None, None);

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        hub.register_probe_specs(
            42,
            &[RuntimeFilterProbeSpec {
                filter_id: 9,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let probe = hub.register_probe(42);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let key = exchange::ExchangeKey {
            finst_id_hi: 0,
            finst_id_lo: 0,
            node_id: 42,
        };
        let node = ExchangeSourceNode::new(
            42,
            Duration::from_secs(60),
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema"),
        );
        let mut operator = ExchangeSourceOperator {
            name: "exchange".to_string(),
            node,
            binding: ExchangeBinding {
                key,
                expected_senders: 1,
            },
            driver_id: 0,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            runtime_filter_probe: Some(ExchangeRuntimeFilterProbe { probe }),
            local_rf_deps: Vec::new(),
            runtime_filter_exprs: HashMap::from([(9, expr)]),
            runtime_filters_expected: 1,
            runtime_filter_lifecycle_handles: HashMap::new(),
            acquired: None,
            runtime_filters_loaded: false,
            arena: Arc::new(arena),
            native_runtime_filter_consumers: None,
            profiles: None,
            receiver_mem_tracker_ready: false,
        };
        let runtime_state = RuntimeState::new(
            None,
            None,
            Some(query_id),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        operator
            .bind_runtime_state(&runtime_state)
            .expect("bind runtime state");
        operator.load_runtime_filters_if_ready();

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&9).expect("filter lifecycle");
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.outcome.as_str()),
            Some("unavailable:NoWaitConfigured")
        );
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.latency_ns),
            Some(0)
        );
        registry.remove_query(query_key);
    }
}
