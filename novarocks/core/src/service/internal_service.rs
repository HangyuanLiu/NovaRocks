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
use std::sync::{Arc, OnceLock, mpsc};

use crate::novarocks_logging::{error, info, warn};

use crate::common::app_config;
use crate::common::config::debug_exec_batch_plan_json;
use crate::common::thrift::{thrift_binary_deserialize, thrift_named_json};

use crate::cache::CacheOptions;
use crate::common::types::UniqueId;
use crate::lower::compat::fragment::execute_fragment;
use crate::lower::compat::node::hdfs_scan::cache_iceberg_table_locations;
use crate::lower::compat::node::lower_row_pos_descs;
use crate::runtime::exchange;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{ProfileUnit, Profiler};
use crate::runtime::query_context::{
    QueryContextManager, QueryId, desc_tbl_is_cached, is_desc_tbl_effectively_empty,
    observe_total_fragments, query_context_manager, query_expire_durations,
    resolve_desc_tbl_for_instance,
};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::result_buffer;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::service::fe_report;
use crate::thrift::{data_sinks, descriptors, internal_service, planner, types};

fn profile_name_for_fragment(fragment: &planner::TPlanFragment) -> String {
    let plan_node_id = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .unwrap_or(-1);
    if plan_node_id >= 0 {
        format!("execute_fragment (plan_node_id={plan_node_id})")
    } else {
        "execute_fragment".to_string()
    }
}

fn choose_nonempty_str<'a>(primary: Option<&'a str>, fallback: Option<&'a str>) -> Option<&'a str> {
    match primary {
        Some(s) if !s.is_empty() => Some(s),
        _ => match fallback {
            Some(s) if !s.is_empty() => Some(s),
            _ => None,
        },
    }
}

fn validate_network_address(
    addr: Option<&types::TNetworkAddress>,
    missing_msg: &str,
    field_name: &str,
) -> Result<(), String> {
    let addr = addr.ok_or_else(|| missing_msg.to_string())?;
    if addr.hostname.is_empty() {
        return Err(format!("{field_name} hostname is empty"));
    }
    if addr.port <= 0 {
        return Err(format!("{field_name} port must be positive"));
    }
    Ok(())
}

fn validate_nodes_info(
    nodes_info: &descriptors::TNodesInfo,
    field_name: &str,
) -> Result<(), String> {
    for (idx, node) in nodes_info.nodes.iter().enumerate() {
        if node.host.is_empty() {
            return Err(format!("{field_name}[{idx}] host is empty"));
        }
        if node.async_internal_port <= 0 {
            return Err(format!(
                "{field_name}[{idx}] async_internal_port must be positive"
            ));
        }
    }
    Ok(())
}

fn validate_destinations(
    dests: &[data_sinks::TPlanFragmentDestination],
    field_name: &str,
) -> Result<(), String> {
    for (idx, dest) in dests.iter().enumerate() {
        validate_network_address(
            dest.brpc_server
                .as_ref()
                .or(dest.deprecated_server.as_ref()),
            "missing destination address",
            &format!("{field_name}[{idx}]"),
        )?;
    }
    Ok(())
}

fn validate_internal_addresses(
    exec_params: &internal_service::TPlanFragmentExecParams,
    fragment: Option<&planner::TPlanFragment>,
) -> Result<(), String> {
    if let Some(dests) = exec_params.destinations.as_ref() {
        validate_destinations(dests, "destinations")?;
    }
    if let Some(params) = exec_params.runtime_filter_params.as_ref()
        && let Some(id_to_probers) = params.id_to_prober_params.as_ref()
    {
        for (filter_id, probers) in id_to_probers {
            for (idx, prober) in probers.iter().enumerate() {
                validate_network_address(
                    prober.fragment_instance_address.as_ref(),
                    "missing runtime filter prober address",
                    &format!(
                        "runtime_filter_params.id_to_prober_params[{filter_id}][{idx}].fragment_instance_address"
                    ),
                )?;
            }
        }
    }
    if let Some(fragment) = fragment {
        if let Some(plan) = fragment.plan.as_ref() {
            for node in &plan.nodes {
                if let Some(fetch) = node.fetch_node.as_ref()
                    && let Some(nodes_info) = fetch.nodes_info.as_ref()
                {
                    validate_nodes_info(nodes_info, "fetch.nodes_info")?;
                }
                if let Some(join) = node.hash_join_node.as_ref()
                    && let Some(filters) = join.build_runtime_filters.as_ref()
                {
                    for (filter_idx, desc) in filters.iter().enumerate() {
                        if let Some(merge_nodes) = desc.runtime_filter_merge_nodes.as_ref() {
                            for (node_idx, addr) in merge_nodes.iter().enumerate() {
                                validate_network_address(
                                    Some(addr),
                                    "missing runtime filter merge address",
                                    &format!(
                                        "hash_join.build_runtime_filters[{filter_idx}].runtime_filter_merge_nodes[{node_idx}]"
                                    ),
                                )?;
                            }
                        }
                    }
                }
            }
        }
        if let Some(sink) = fragment.output_sink.as_ref() {
            match sink.type_ {
                data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                    let Some(multi) = sink.multi_cast_stream_sink.as_ref() else {
                        return Err(
                            "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload"
                                .to_string(),
                        );
                    };
                    for (idx, dests) in multi.destinations.iter().enumerate() {
                        validate_destinations(
                            dests,
                            &format!("multi_cast_stream_sink.destinations[{idx}]"),
                        )?;
                    }
                }
                data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
                    let Some(split) = sink.split_stream_sink.as_ref() else {
                        return Err(
                            "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload".to_string()
                        );
                    };
                    if let Some(destinations) = split.destinations.as_ref() {
                        for (idx, dests) in destinations.iter().enumerate() {
                            validate_destinations(
                                dests,
                                &format!("split_stream_sink.destinations[{idx}]"),
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

// TODO(novarocks): Align with StarRocks BE by plumbing
// `node_to_per_driver_seq_scan_ranges` through scan lowering and morsel scheduling directly.
// Current implementation is a compatibility shim:
// FE may send scan ranges only via `node_to_per_driver_seq_scan_ranges` in pipeline mode.
// We fill missing/no-concrete `per_node_scan_ranges[node_id]` by flattening per-driver ranges so
// existing lowering paths can consume scan ranges deterministically.
// "no-concrete" means all entries are `empty=true` placeholders.
fn backfill_per_node_scan_ranges(exec_params: &mut internal_service::TPlanFragmentExecParams) {
    fn has_concrete_scan_range(ranges: &[internal_service::TScanRangeParams]) -> bool {
        ranges.iter().any(|range| !range.empty.unwrap_or(false))
    }

    let Some(node_to_per_driver) = exec_params.node_to_per_driver_seq_scan_ranges.as_ref() else {
        return;
    };
    let mut to_insert = Vec::new();
    for (node_id, per_driver) in node_to_per_driver {
        let existing = exec_params.per_node_scan_ranges.get(node_id);
        let need_backfill = existing
            .map(|ranges| !has_concrete_scan_range(ranges))
            .unwrap_or(true);
        if !need_backfill {
            continue;
        }
        let flattened = per_driver
            .values()
            .flat_map(|ranges| ranges.iter().cloned())
            .collect::<Vec<_>>();
        if flattened.is_empty() {
            if existing.is_none() {
                to_insert.push((*node_id, Vec::new()));
            }
            continue;
        }
        to_insert.push((*node_id, flattened));
    }
    if to_insert.is_empty() {
        return;
    }
    for (node_id, ranges) in to_insert {
        exec_params.per_node_scan_ranges.insert(node_id, ranges);
    }
}

fn append_incremental_scan_ranges(
    exec_params: &mut internal_service::TPlanFragmentExecParams,
) -> Result<(), String> {
    backfill_per_node_scan_ranges(exec_params);
    let finst_id = UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    };
    let mgr = query_context_manager();
    for (node_id, scan_ranges) in &exec_params.per_node_scan_ranges {
        if scan_ranges.is_empty() {
            continue;
        }
        mgr.append_incremental_scan_ranges(finst_id, *node_id, scan_ranges.clone())?;
    }
    Ok(())
}

fn add_exchange_sender_counts(counts: &mut HashMap<i32, usize>, fragment: &planner::TPlanFragment) {
    let Some(sink) = fragment.output_sink.as_ref() else {
        return;
    };
    match sink.type_ {
        data_sinks::TDataSinkType::DATA_STREAM_SINK => {
            if let Some(stream_sink) = sink.stream_sink.as_ref() {
                *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
            } else {
                warn!(
                    target: "novarocks::exec",
                    "DATA_STREAM_SINK missing stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            if let Some(multi) = sink.multi_cast_stream_sink.as_ref() {
                for stream_sink in &multi.sinks {
                    *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "MULTI_CAST_DATA_STREAM_SINK missing multi_cast_stream_sink payload while collecting senders"
                );
            }
        }
        data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            if let Some(split) = sink.split_stream_sink.as_ref() {
                if let Some(sinks) = split.sinks.as_ref() {
                    for stream_sink in sinks {
                        *counts.entry(stream_sink.dest_node_id).or_insert(0) += 1;
                    }
                } else {
                    warn!(
                        target: "novarocks::exec",
                        "SPLIT_DATA_STREAM_SINK missing sinks while collecting senders"
                    );
                }
            } else {
                warn!(
                    target: "novarocks::exec",
                    "SPLIT_DATA_STREAM_SINK missing split_stream_sink payload while collecting senders"
                );
            }
        }
        _ => {}
    }
}

fn collect_exchange_sender_counts(
    common: Option<&internal_service::TExecPlanFragmentParams>,
    unique: &[internal_service::TExecPlanFragmentParams],
) -> HashMap<i32, usize> {
    let mut counts = HashMap::new();
    if unique.is_empty() {
        if let Some(fragment) = common.and_then(|c| c.fragment.as_ref()) {
            add_exchange_sender_counts(&mut counts, fragment);
        }
        return counts;
    }

    for one in unique {
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        if let Some(fragment) = fragment {
            add_exchange_sender_counts(&mut counts, fragment);
        }
    }
    counts
}

fn collect_fragment_row_position_metadata(
    fragment: &planner::TPlanFragment,
) -> Result<HashMap<i32, crate::exec::row_position::RowPositionDescriptor>, String> {
    let Some(plan) = fragment.plan.as_ref() else {
        return Ok(HashMap::new());
    };
    let mut prepared: HashMap<i32, crate::exec::row_position::RowPositionDescriptor> =
        HashMap::new();
    for node in &plan.nodes {
        let (node_name, thrift_descs) =
            if node.node_type == crate::thrift::plan_nodes::TPlanNodeType::LOOKUP_NODE {
                let lookup = node
                    .look_up_node
                    .as_ref()
                    .ok_or_else(|| "LOOKUP_NODE missing look_up_node payload".to_string())?;
                (
                    "LOOKUP_NODE",
                    lookup
                        .row_pos_descs
                        .as_ref()
                        .ok_or_else(|| "LOOKUP_NODE missing row_pos_descs".to_string())?,
                )
            } else if node.node_type == crate::thrift::plan_nodes::TPlanNodeType::FETCH_NODE {
                let fetch = node
                    .fetch_node
                    .as_ref()
                    .ok_or_else(|| "FETCH_NODE missing fetch_node payload".to_string())?;
                (
                    "FETCH_NODE",
                    fetch
                        .row_pos_descs
                        .as_ref()
                        .ok_or_else(|| "FETCH_NODE missing row_pos_descs".to_string())?,
                )
            } else {
                continue;
            };
        let descs = lower_row_pos_descs(thrift_descs)?;
        if descs.is_empty() {
            return Err(format!("{node_name} row_pos_descs is empty"));
        }
        for (tuple_id, incoming) in descs {
            if let Some(existing) = prepared.get(&tuple_id) {
                if existing.row_position_type != incoming.row_position_type
                    || existing.row_source_slot != incoming.row_source_slot
                    || existing.fetch_ref_slots != incoming.fetch_ref_slots
                    || existing.lookup_ref_slots != incoming.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={tuple_id}"
                    ));
                }
            } else {
                prepared.insert(tuple_id, incoming);
            }
        }
    }
    Ok(prepared)
}

fn prepare_fragment_row_position_metadata(
    mgr: &QueryContextManager,
    query_id: QueryId,
    fragment: &planner::TPlanFragment,
) -> Result<(), String> {
    let prepared = collect_fragment_row_position_metadata(fragment)?;
    if !prepared.is_empty() {
        // StarRocks prepares LOOKUP/FETCH metadata before acknowledging fragment ingress.
        // Register it synchronously so a remote lookup cannot race async plan lowering.
        mgr.register_row_pos_descs(query_id, prepared)?;
    }
    Ok(())
}

fn prepare_lookup_lifecycle(
    mgr: &QueryContextManager,
    query_id: QueryId,
    fragment: &planner::TPlanFragment,
    exec_params: &internal_service::TPlanFragmentExecParams,
) -> Result<(), String> {
    let Some(plan) = fragment.plan.as_ref() else {
        return Ok(());
    };
    let lookup_node_ids = plan
        .nodes
        .iter()
        .filter(|node| node.node_type == crate::thrift::plan_nodes::TPlanNodeType::LOOKUP_NODE)
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    if lookup_node_ids.is_empty() {
        return Ok(());
    }

    let mut lifecycles = HashMap::new();
    for node_id in lookup_node_ids {
        let lifecycle = match exec_params
            .per_look_up_num_fetchers
            .as_ref()
            .and_then(|counts| counts.get(&node_id))
        {
            Some(count) => crate::runtime::query_context::LookupFetcherLifecycle::Exact(
                usize::try_from(*count).map_err(|_| {
                    format!("lookup node {node_id} has negative fetcher count {count}")
                })?,
            ),
            None => crate::runtime::query_context::LookupFetcherLifecycle::Unknown,
        };
        lifecycles.insert(node_id, lifecycle);
    }
    mgr.register_lookup_fetchers(query_id, lifecycles)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LookupCloseTarget {
    lookup_node_id: i32,
    host: String,
    port: i32,
}

fn collect_lookup_close_targets(fragment: &planner::TPlanFragment) -> Vec<LookupCloseTarget> {
    let Some(plan) = fragment.plan.as_ref() else {
        return Vec::new();
    };
    let mut targets = HashSet::new();
    for node in &plan.nodes {
        if node.node_type != crate::thrift::plan_nodes::TPlanNodeType::FETCH_NODE {
            continue;
        }
        let Some(fetch) = node.fetch_node.as_ref() else {
            continue;
        };
        let (Some(lookup_node_id), Some(nodes_info)) =
            (fetch.target_node_id, fetch.nodes_info.as_ref())
        else {
            continue;
        };
        for target in &nodes_info.nodes {
            targets.insert(LookupCloseTarget {
                lookup_node_id,
                host: target.host.clone(),
                port: target.async_internal_port,
            });
        }
    }
    targets.into_iter().collect()
}

struct LookupCloseGuard {
    query_id: QueryId,
    targets: Vec<LookupCloseTarget>,
}

#[derive(Debug)]
struct LookupCloseTask {
    query_id: QueryId,
    target: LookupCloseTarget,
}

struct LookupCloseDispatcher {
    sender: mpsc::SyncSender<LookupCloseTask>,
}

const LOOKUP_CLOSE_WORKERS: usize = 4;
const LOOKUP_CLOSE_QUEUE_CAPACITY: usize = 256;

impl LookupCloseDispatcher {
    fn start() -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel(LOOKUP_CLOSE_QUEUE_CAPACITY);
        let receiver = Arc::new(std::sync::Mutex::new(receiver));
        for index in 0..LOOKUP_CLOSE_WORKERS {
            let receiver = Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("lookup-close-{index}"))
                .spawn(move || lookup_close_worker(receiver))
                .map_err(|error| format!("failed to start lookup_close worker {index}: {error}"))?;
        }
        Ok(Self { sender })
    }

    fn try_dispatch(&self, task: LookupCloseTask) -> Result<(), String> {
        self.sender.try_send(task).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => "lookup_close queue is full".to_string(),
            mpsc::TrySendError::Disconnected(_) => {
                "lookup_close dispatcher is disconnected".to_string()
            }
        })
    }
}

fn lookup_close_dispatcher() -> Result<&'static LookupCloseDispatcher, String> {
    static DISPATCHER: OnceLock<Result<LookupCloseDispatcher, String>> = OnceLock::new();
    DISPATCHER
        .get_or_init(LookupCloseDispatcher::start)
        .as_ref()
        .map_err(Clone::clone)
}

fn lookup_close_worker(receiver: Arc<std::sync::Mutex<mpsc::Receiver<LookupCloseTask>>>) {
    loop {
        let task = {
            let receiver = receiver
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            receiver.recv()
        };
        let Ok(task) = task else {
            return;
        };
        let port = match u16::try_from(task.target.port) {
            Ok(port) => port,
            Err(_) => {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %task.query_id,
                    lookup_node_id = task.target.lookup_node_id,
                    host = %task.target.host,
                    port = task.target.port,
                    "lookup_close skipped: async_internal_port out of u16 range"
                );
                continue;
            }
        };
        if let Err(err) = crate::service::internal_rpc_client::lookup_close(
            &task.target.host,
            port,
            task.query_id,
            task.target.lookup_node_id,
        ) {
            warn!(
                target: "novarocks::rpc",
                query_id = %task.query_id,
                lookup_node_id = task.target.lookup_node_id,
                host = %task.target.host,
                port,
                error = %err,
                "lookup_close failed"
            );
        }
    }
}

impl Drop for LookupCloseGuard {
    fn drop(&mut self) {
        let dispatcher = match lookup_close_dispatcher() {
            Ok(dispatcher) => dispatcher,
            Err(error) => {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %self.query_id,
                    error = %error,
                    "lookup_close dispatch unavailable"
                );
                return;
            }
        };
        for target in self.targets.drain(..) {
            let lookup_node_id = target.lookup_node_id;
            let host = target.host.clone();
            let port = target.port;
            if let Err(error) = dispatcher.try_dispatch(LookupCloseTask {
                query_id: self.query_id,
                target,
            }) {
                warn!(
                    target: "novarocks::rpc",
                    query_id = %self.query_id,
                    lookup_node_id,
                    host = %host,
                    port,
                    error = %error,
                    "lookup_close dispatch rejected"
                );
            }
        }
    }
}

fn spawn_exec_fragment(
    fragment: planner::TPlanFragment,
    desc_tbl: Option<descriptors::TDescriptorTable>,
    exec_params: internal_service::TPlanFragmentExecParams,
    query_opts: Option<internal_service::TQueryOptions>,
    session_time_zone: Option<String>,
    pipeline_dop: i32,
    group_execution_scan_dop: Option<i32>,
    db_name: Option<String>,
    finst_id: UniqueId,
    query_id: QueryId,
    backend_num: Option<i32>,
    profiler: Option<Profiler>,
    last_query_id: Option<String>,
    fe_addr: Option<types::TNetworkAddress>,
    mem_tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    typed_result_sink: bool,
    mgr: Arc<QueryContextManager>,
) {
    let lookup_close_targets = collect_lookup_close_targets(&fragment);
    let uses_fetch_result_buffer = matches!(
        fragment.output_sink.as_ref().map(|sink| sink.type_),
        Some(data_sinks::TDataSinkType::RESULT_SINK)
    );
    if uses_fetch_result_buffer {
        if typed_result_sink {
            result_buffer::create_typed_sender(finst_id);
        } else {
            result_buffer::create_sender(finst_id);
        }
        if let Some(root) = mem_tracker.as_ref() {
            let label = format!("ResultBuffer: finst={}", finst_id);
            let tracker = crate::runtime::mem_tracker::MemTracker::new_child(label, root);
            result_buffer::set_mem_tracker(finst_id, tracker);
        }
    }
    mgr.register_finst(finst_id, query_id);
    std::thread::spawn(move || {
        let query_opts = query_opts.as_ref();
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let out = {
            // One guard per fragment instance, not per pipeline driver. Dropping it after the
            // entire executor returns mirrors StarRocks FetchProcessorFactory::close_context.
            let _lookup_close_guard = LookupCloseGuard {
                query_id,
                targets: lookup_close_targets,
            };
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_fragment(
                    &fragment,
                    desc_tbl.as_ref(),
                    Some(&exec_params),
                    query_opts,
                    session_time_zone.as_deref(),
                    pipeline_dop,
                    group_execution_scan_dop,
                    db_name.as_deref(),
                    profiler,
                    last_query_id.as_deref(),
                    fe_addr.as_ref(),
                    backend_num,
                    mem_tracker,
                    typed_result_sink,
                )
            }))
            .unwrap_or_else(|payload| {
                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                Err(format!("panic in fragment execution: {msg}"))
            })
        };
        if let Some(p) = profiler_for_wall.as_ref() {
            let elapsed_ns =
                crate::runtime::profile::clamp_u128_to_i64(wall_start.elapsed().as_nanos());
            p.counter_set("QueryExecutionWallTime", ProfileUnit::TimeNs, elapsed_ns);
        }
        let mut report_error: Option<String> = None;
        if uses_fetch_result_buffer {
            match out {
                Ok(out) => {
                    if let Some(json) = out.profile_json.as_deref() {
                        info!(
                            target: "novarocks::profile",
                            finst_id = %finst_id,
                            profile_bytes = json.len(),
                            "fragment_profile"
                        );
                    }
                }
                Err(e) => {
                    report_error = Some(e.clone());
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %e,
                        "exec_plan_fragment failed"
                    );
                    result_buffer::close_error(finst_id, e);
                }
            }
        } else if let Err(e) = out {
            report_error = Some(e.clone());
            error!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                error = %e,
                "exec_plan_fragment failed"
            );
        }
        if let Some(ref err_msg) = report_error {
            let finsts = mgr.cancel_query(query_id, err_msg.clone());
            for id in finsts {
                result_buffer::close_error(id, err_msg.clone());
                exchange::cancel_fragment(id.hi, id.lo);
            }
        }
        let report_decision = mgr.finish_fragment_for_report(query_id);
        fe_report::report_fragment_done(
            finst_id,
            report_error,
            report_decision.include_runtime_filter_profile,
        );
        exchange::remove_fragment(finst_id.hi, finst_id.lo);
        mgr.unregister_finst(finst_id);
        mgr.cleanup_after_fragment_report(query_id, report_decision);
    });
}

pub fn submit_exec_batch_plan_fragments(thrift_bytes: &[u8]) -> Result<usize, String> {
    let batch: internal_service::TExecBatchPlanFragmentsParams =
        thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&batch) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_batch_plan_fragments",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let common = batch.common_param.as_ref();
    let unique = batch.unique_param_per_instance.unwrap_or_default();
    let mgr = query_context_manager();
    let common_desc_tbl = common.and_then(|c| c.desc_tbl.as_ref());
    let common_query_opts = common.and_then(|c| c.query_options.as_ref());
    let common_query_id = common.and_then(|c| c.params.as_ref()).map(|p| QueryId {
        hi: p.query_id.hi,
        lo: p.query_id.lo,
    });
    let sender_counts = collect_exchange_sender_counts(common, &unique);
    let mut sender_counts_applied = false;
    if let Some(query_id) = common_query_id {
        let common_query_opts_native = QueryOptions::from_thrift(common_query_opts)?;
        let (delivery_expire, query_expire) =
            query_expire_durations(Some(&common_query_opts_native));
        let require_existing = common_desc_tbl.map(desc_tbl_is_cached).unwrap_or(false);
        mgr.ensure_compat_context(query_id, require_existing, delivery_expire, query_expire)?;
        if let Some(desc_tbl) = common_desc_tbl
            && !desc_tbl_is_cached(desc_tbl)
            && !is_desc_tbl_effectively_empty(desc_tbl)
        {
            mgr.with_context_mut(query_id, |ctx| {
                ctx.desc_tbl = Some(desc_tbl.clone());
                Ok(())
            })?;
        }
    }

    let mut created = 0usize;

    let mut query_id_for_batch = common_query_id;
    for one in unique.iter() {
        let params = one
            .params
            .as_ref()
            .or_else(|| common.and_then(|c| c.params.as_ref()));
        let fragment = one
            .fragment
            .as_ref()
            .or_else(|| common.and_then(|c| c.fragment.as_ref()));
        let coord = one
            .coord
            .as_ref()
            .or_else(|| common.and_then(|c| c.coord.as_ref()));
        let novarocks_report_addr = one
            .novarocks_report_addr
            .clone()
            .or_else(|| common.and_then(|c| c.novarocks_report_addr.clone()));
        let typed_result_sink = one
            .novarocks_typed_result_sink
            .or_else(|| common.and_then(|c| c.novarocks_typed_result_sink))
            .unwrap_or(false);
        let backend_num = one
            .backend_num
            .or_else(|| common.and_then(|c| c.backend_num));
        // NOTE: backend_num must match FE's instance index (ExecutionDAG index).
        // If this value is wrong, FE will treat reportExecStatus as "unknown backend number"
        // and drop sink_commit_infos, causing Iceberg commit to be skipped.
        let db_name = choose_nonempty_str(
            one.db_name.as_deref(),
            common.and_then(|c| c.db_name.as_deref()),
        );
        let query_opts = one
            .query_options
            .as_ref()
            .or(common.and_then(|c| c.query_options.as_ref()));
        let query_globals = one
            .query_globals
            .as_ref()
            .or_else(|| common.and_then(|c| c.query_globals.as_ref()));
        let last_query_id = query_globals
            .and_then(|g| g.last_query_id.as_deref())
            .map(|s| s.to_string());
        let session_time_zone = query_globals.and_then(|g| g.time_zone.clone());

        let Some(exec_params) = params else {
            continue;
        };
        let Some(fragment) = fragment else {
            continue;
        };

        let query_id = QueryId {
            hi: exec_params.query_id.hi,
            lo: exec_params.query_id.lo,
        };
        if let Some(existing) = query_id_for_batch {
            if existing != query_id {
                return Err("mixed query_id in exec_batch_plan_fragments".to_string());
            }
        } else {
            query_id_for_batch = Some(query_id);
        }

        let query_opts_native = QueryOptions::from_thrift(query_opts)?;
        let (delivery_expire, query_expire) = query_expire_durations(Some(&query_opts_native));
        let require_existing = one
            .desc_tbl
            .as_ref()
            .map(desc_tbl_is_cached)
            .unwrap_or(false);
        mgr.get_or_register_compat(query_id, require_existing, delivery_expire, query_expire)?;
        let cache_options = CacheOptions::from_query_options(Some(&query_opts_native))?;
        mgr.set_cache_options(query_id, cache_options)?;
        if !sender_counts_applied && !sender_counts.is_empty() {
            mgr.update_exchange_sender_counts(query_id, sender_counts.clone())?;
            sender_counts_applied = true;
        }
        let desc_tbl = resolve_desc_tbl_for_instance(
            mgr.as_ref(),
            query_id,
            one.desc_tbl.as_ref(),
            common_desc_tbl,
        )?;

        let finst_id = UniqueId {
            hi: exec_params.fragment_instance_id.hi,
            lo: exec_params.fragment_instance_id.lo,
        };
        let query_mem_tracker = mgr
            .query_mem_tracker(query_id)
            .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
        let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
        let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        // Result buffer timeout is derived from QueryContext by finst_id.
        let enable_profile = query_opts
            .and_then(|opts| opts.enable_profile)
            .unwrap_or(false);
        let profiler = if enable_profile {
            Some(Profiler::new(profile_name_for_fragment(fragment)))
        } else {
            None
        };
        let report_interval_ns = if enable_profile {
            let from_query = query_opts
                .and_then(|opts| opts.runtime_profile_report_interval)
                .filter(|v| *v > 0)
                .and_then(|v| v.checked_mul(1_000_000_000));
            from_query.or_else(|| {
                app_config::config()
                    .ok()
                    .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
            })
        } else {
            None
        };
        if let (Some(report_addr), Some(backend_num)) = (novarocks_report_addr, backend_num) {
            let report_endpoint =
                crate::runtime::endpoint::RuntimeEndpoint::from_network_address(&report_addr)?;
            fe_report::register_novarocks_instance(
                finst_id,
                query_id,
                report_endpoint,
                backend_num,
                enable_profile,
                profiler.clone(),
                Some(Arc::clone(&fragment_mem_tracker)),
                Some(Arc::clone(&query_mem_tracker)),
                report_interval_ns,
            );
        } else if let (Some(coord), Some(backend_num)) = (coord.cloned(), backend_num) {
            fe_report::register_instance(
                finst_id,
                query_id,
                coord,
                backend_num,
                enable_profile,
                profiler.clone(),
                Some(Arc::clone(&fragment_mem_tracker)),
                Some(Arc::clone(&query_mem_tracker)),
                report_interval_ns,
            );
        } else {
            warn!(
                target: "novarocks::report",
                finst_id = %finst_id,
                "missing report destination/backend_num for reportExecStatus"
            );
        }
        mgr.with_context_mut(query_id, |ctx| {
            observe_total_fragments(ctx, exec_params);
            Ok(())
        })?;
        let desc_snapshot = mgr.descriptor_snapshot(query_id);
        cache_iceberg_table_locations(desc_snapshot.as_deref());
        let pipeline_dop = resolve_pipeline_dop(one);
        let group_execution_scan_dop = one.group_execution_scan_dop;
        let query_opts = query_opts.cloned();
        let mut exec_params = exec_params.clone();
        let fragment = fragment.clone();
        backfill_per_node_scan_ranges(&mut exec_params);
        validate_internal_addresses(&exec_params, Some(&fragment))?;
        prepare_fragment_row_position_metadata(mgr.as_ref(), query_id, &fragment)?;
        prepare_lookup_lifecycle(mgr.as_ref(), query_id, &fragment, &exec_params)?;
        if let Some(params) = exec_params.runtime_filter_params.clone() {
            let params = RuntimeFilterParams::from_thrift(&params)?;
            mgr.set_runtime_filter_params(query_id, params)?;
        }
        spawn_exec_fragment(
            fragment,
            desc_tbl.clone(),
            exec_params,
            query_opts,
            session_time_zone,
            pipeline_dop,
            group_execution_scan_dop,
            db_name.map(|s| s.to_string()),
            finst_id,
            query_id,
            backend_num,
            profiler,
            last_query_id,
            coord.cloned(),
            Some(fragment_mem_tracker),
            typed_result_sink,
            Arc::clone(&mgr),
        );
        created += 1;
    }

    if !sender_counts_applied
        && !sender_counts.is_empty()
        && let Some(query_id) = query_id_for_batch
    {
        mgr.update_exchange_sender_counts(query_id, sender_counts)?;
    }

    if query_id_for_batch.is_none() {
        return Ok(0);
    }
    Ok(created)
}

pub fn submit_exec_plan_fragment(thrift_bytes: &[u8]) -> Result<(), String> {
    let one: internal_service::TExecPlanFragmentParams = thrift_binary_deserialize(thrift_bytes)?;
    if debug_exec_batch_plan_json() {
        match thrift_named_json(&one) {
            Ok(json) => info!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                named_json = %json,
                "named_json"
            ),
            Err(e) => warn!(
                target: "novarocks::rpc",
                rpc = "exec_plan_fragment",
                error = %e,
                "named_json_failed"
            ),
        }
    }
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    if one.fragment.is_none() {
        let mut params = params.clone();
        append_incremental_scan_ranges(&mut params)?;
        return Ok(());
    }
    let fragment = one.fragment.as_ref().expect("checked above");
    let coord = one.coord.as_ref();
    let novarocks_report_addr = one.novarocks_report_addr.clone();
    let typed_result_sink = one.novarocks_typed_result_sink.unwrap_or(false);
    let backend_num = one.backend_num;
    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };
    let query_opts = one.query_options.as_ref();
    let query_globals = one.query_globals.as_ref();
    let last_query_id = query_globals
        .and_then(|g| g.last_query_id.as_deref())
        .map(|s| s.to_string());
    let session_time_zone = query_globals.and_then(|g| g.time_zone.clone());
    let query_opts_native = QueryOptions::from_thrift(query_opts)?;
    let (delivery_expire, query_expire) = query_expire_durations(Some(&query_opts_native));
    let mgr = query_context_manager();
    let require_existing = one
        .desc_tbl
        .as_ref()
        .map(desc_tbl_is_cached)
        .unwrap_or(false);
    mgr.get_or_register_compat(query_id, require_existing, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(Some(&query_opts_native))?;
    mgr.set_cache_options(query_id, cache_options)?;
    mgr.with_context_mut(query_id, |ctx| {
        observe_total_fragments(ctx, params);
        Ok(())
    })?;
    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let desc_tbl =
        resolve_desc_tbl_for_instance(mgr.as_ref(), query_id, one.desc_tbl.as_ref(), None)?;
    let desc_snapshot = mgr.descriptor_snapshot(query_id);
    cache_iceberg_table_locations(desc_snapshot.as_deref());
    // Result buffer timeout is derived from QueryContext by finst_id.
    let enable_profile = query_opts
        .and_then(|opts| opts.enable_profile)
        .unwrap_or(false);
    let profiler = if enable_profile {
        Some(Profiler::new(profile_name_for_fragment(fragment)))
    } else {
        None
    };
    let report_interval_ns = if enable_profile {
        let from_query = query_opts
            .and_then(|opts| opts.runtime_profile_report_interval)
            .filter(|v| *v > 0)
            .and_then(|v| v.checked_mul(1_000_000_000));
        from_query.or_else(|| {
            app_config::config()
                .ok()
                .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
    } else {
        None
    };
    if let (Some(report_addr), Some(backend_num)) = (novarocks_report_addr, backend_num) {
        let report_endpoint =
            crate::runtime::endpoint::RuntimeEndpoint::from_network_address(&report_addr)?;
        fe_report::register_novarocks_instance(
            finst_id,
            query_id,
            report_endpoint,
            backend_num,
            enable_profile,
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Some(Arc::clone(&query_mem_tracker)),
            report_interval_ns,
        );
    } else if let (Some(coord), Some(backend_num)) = (coord.cloned(), backend_num) {
        fe_report::register_instance(
            finst_id,
            query_id,
            coord,
            backend_num,
            enable_profile,
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Some(Arc::clone(&query_mem_tracker)),
            report_interval_ns,
        );
    } else {
        warn!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "missing report destination/backend_num for reportExecStatus"
        );
    }

    let pipeline_dop = resolve_pipeline_dop(&one);
    let group_execution_scan_dop = one.group_execution_scan_dop;

    let mut params = params.clone();
    let fragment = fragment.clone();
    backfill_per_node_scan_ranges(&mut params);
    validate_internal_addresses(&params, Some(&fragment))?;
    prepare_fragment_row_position_metadata(mgr.as_ref(), query_id, &fragment)?;
    prepare_lookup_lifecycle(mgr.as_ref(), query_id, &fragment, &params)?;
    if let Some(rf_params) = params.runtime_filter_params.clone() {
        let rf_params = RuntimeFilterParams::from_thrift(&rf_params)?;
        mgr.set_runtime_filter_params(query_id, rf_params)?;
    }
    spawn_exec_fragment(
        fragment,
        desc_tbl.clone(),
        params,
        one.query_options.clone(),
        session_time_zone,
        pipeline_dop,
        group_execution_scan_dop,
        one.db_name.clone(),
        finst_id,
        query_id,
        backend_num,
        profiler,
        last_query_id,
        coord.cloned(),
        Some(fragment_mem_tracker),
        typed_result_sink,
        Arc::clone(&mgr),
    );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct SyncExecPlanResult {
    pub(crate) finst_id: UniqueId,
}

pub(crate) fn execute_plan_fragment_sync(
    one: internal_service::TExecPlanFragmentParams,
) -> Result<SyncExecPlanResult, String> {
    let Some(params) = one.params.as_ref() else {
        return Err("missing params in TExecPlanFragmentParams".to_string());
    };
    let Some(fragment) = one.fragment.as_ref() else {
        return Err("missing fragment in TExecPlanFragmentParams".to_string());
    };

    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };

    let query_opts = one.query_options.as_ref();
    let query_globals = one.query_globals.as_ref();
    let last_query_id = query_globals.and_then(|g| g.last_query_id.as_deref());
    let session_time_zone = query_globals.and_then(|g| g.time_zone.as_deref());
    let query_opts_native = QueryOptions::from_thrift(query_opts)?;
    let (delivery_expire, query_expire) = query_expire_durations(Some(&query_opts_native));
    let mgr = query_context_manager();
    let require_existing = one
        .desc_tbl
        .as_ref()
        .map(desc_tbl_is_cached)
        .unwrap_or(false);
    mgr.get_or_register_compat(query_id, require_existing, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(Some(&query_opts_native))?;
    mgr.set_cache_options(query_id, cache_options)?;
    mgr.with_context_mut(query_id, |ctx| {
        observe_total_fragments(ctx, params);
        Ok(())
    })?;

    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let desc_tbl =
        resolve_desc_tbl_for_instance(mgr.as_ref(), query_id, one.desc_tbl.as_ref(), None)?;
    let desc_snapshot = mgr.descriptor_snapshot(query_id);
    cache_iceberg_table_locations(desc_snapshot.as_deref());

    let pipeline_dop = resolve_pipeline_dop(&one);
    let group_execution_scan_dop = one.group_execution_scan_dop;
    let typed_result_sink = one.novarocks_typed_result_sink.unwrap_or(false);
    let mut params = params.clone();
    let fragment = fragment.clone();
    backfill_per_node_scan_ranges(&mut params);
    validate_internal_addresses(&params, Some(&fragment))?;
    if let Some(rf_params) = params.runtime_filter_params.clone() {
        let rf_params = RuntimeFilterParams::from_thrift(&rf_params)?;
        mgr.set_runtime_filter_params(query_id, rf_params)?;
    }

    let exec_result = execute_fragment(
        &fragment,
        desc_tbl.as_ref(),
        Some(&params),
        query_opts,
        session_time_zone,
        pipeline_dop,
        group_execution_scan_dop,
        one.db_name.as_deref(),
        None,
        last_query_id,
        one.coord.as_ref(),
        one.backend_num,
        Some(fragment_mem_tracker),
        typed_result_sink,
    );
    exchange::remove_fragment(finst_id.hi, finst_id.lo);
    mgr.finish_fragment(query_id);

    match exec_result {
        Ok(_) => Ok(SyncExecPlanResult { finst_id }),
        Err(err) => {
            crate::runtime::sink_commit::unregister(finst_id);
            Err(err)
        }
    }
}

fn resolve_pipeline_dop(request: &internal_service::TExecPlanFragmentParams) -> i32 {
    // Align with StarRocks: pipeline_dop is a per-fragment-instance (unique request) parameter.
    crate::runtime::exec_env::calc_pipeline_dop(request.pipeline_dop.unwrap_or(0))
}
