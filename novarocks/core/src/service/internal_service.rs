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
use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::compat::endpoint::destination_address;
use crate::protocol::starrocks::compat::request::backfill_per_node_scan_ranges;
use crate::protocol::starrocks::decode::node::lower_row_pos_descs;
use crate::protocol::starrocks::decode::{
    StarRocksDecodeInput, StarRocksFragmentDraft, StarRocksReportDestination,
    StarRocksSubmissionMetadata, decode_incremental_scan_ranges, decode_query_options,
    decode_runtime_endpoint, finish_fragment_submission, prepare_fragment_submission,
};
use crate::runtime::exchange;
use crate::runtime::fragment::starrocks_execution::{
    StarRocksExecutionContext, StarRocksExecutionMetadata, execute_starrocks_submission,
    uses_fetch_result_buffer,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{ProfileUnit, Profiler};
use crate::runtime::query_context::{
    LookupFetcherLifecycle, QueryContextManager, QueryExecutionKey, QueryId,
    StarRocksQueryGeneration, StarRocksQueryHandoff, query_context_manager, query_expire_durations,
};
use crate::runtime::result_buffer;
use crate::service::fe_report;
use crate::service::starrocks_fragment_dependency_resolver::resolve_dependencies;
use crate::service::starrocks_fragment_transport::{
    StarRocksDescriptorPreparation, StarRocksPrelaunchCancellationToken, StarRocksPrelaunchGuard,
    commit_descriptor_handoff, prepare_batch_descriptor, prepare_descriptor, snapshot_decode_facts,
    starrocks_prelaunch_registry,
};
use crate::thrift::{data_sinks, descriptors, internal_service, planner, types};

#[cfg(test)]
static TEST_FRAGMENT_LAUNCH_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn test_fragment_launch_count() -> usize {
    TEST_FRAGMENT_LAUNCH_COUNT.load(std::sync::atomic::Ordering::SeqCst)
}

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
            destination_address(dest),
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

fn append_incremental_scan_ranges(
    exec_params: &mut internal_service::TPlanFragmentExecParams,
) -> Result<(), String> {
    backfill_per_node_scan_ranges(exec_params);
    let finst_id = UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    };
    let mgr = query_context_manager();
    let mut decoded_updates = Vec::new();
    for (node_id, scan_ranges) in &exec_params.per_node_scan_ranges {
        if scan_ranges.is_empty() {
            continue;
        }
        let change_op_slot = mgr.incremental_change_op_slot(finst_id, *node_id)?;
        let decoded = decode_incremental_scan_ranges(*node_id, scan_ranges, change_op_slot)
            .map_err(|error| error.to_string())?;
        decoded_updates.push((*node_id, decoded));
    }
    for (node_id, scan_ranges) in decoded_updates {
        mgr.append_incremental_scan_ranges(finst_id, node_id, scan_ranges)?;
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

struct PreparedStarRocksFragment {
    submission: crate::runtime::fragment::submission::FragmentSubmission,
    metadata: StarRocksSubmissionMetadata,
    total_fragments: Option<usize>,
}

struct StarRocksFragmentDraftEnvelope {
    draft: StarRocksFragmentDraft,
    total_fragments: Option<usize>,
}

#[allow(clippy::too_many_arguments)]
fn prepare_starrocks_draft(
    fragment: &planner::TPlanFragment,
    descriptor: Option<&descriptors::TDescriptorTable>,
    params: &internal_service::TPlanFragmentExecParams,
    query_opts: Option<&internal_service::TQueryOptions>,
    query_globals: Option<&internal_service::TQueryGlobals>,
    db_name: Option<&str>,
    coord: Option<&types::TNetworkAddress>,
    novarocks_report_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    pipeline_dop: i32,
    group_execution_scan_dop: Option<i32>,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    typed_result_sink: bool,
) -> Result<StarRocksFragmentDraftEnvelope, String> {
    validate_internal_addresses(params, Some(fragment))?;
    let facts = snapshot_decode_facts(params)?;
    let novarocks_report_endpoint = novarocks_report_addr
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("novarocks_report_addr"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    let draft = prepare_fragment_submission(StarRocksDecodeInput {
        fragment,
        descriptors: descriptor,
        params,
        query_options: query_opts,
        query_globals,
        db_name,
        coord,
        novarocks_report_endpoint: novarocks_report_endpoint.as_ref(),
        backend_num,
        pipeline_dop,
        group_execution_scan_dop,
        batch_exchange_sender_counts,
        typed_result_sink,
        facts: &facts,
    })
    .map_err(|error| error.to_string())?;
    Ok(StarRocksFragmentDraftEnvelope {
        draft,
        total_fragments: params.instances_number.map(|value| value.max(0) as usize),
    })
}

fn resolve_starrocks_draft(
    draft: &StarRocksFragmentDraftEnvelope,
    token: &StarRocksPrelaunchCancellationToken,
) -> Result<crate::protocol::starrocks::decode::StarRocksResolvedDependencies, String> {
    resolve_dependencies(draft.draft.external_dependencies(), token)
        .map_err(|error| error.to_string())
}

fn finish_starrocks_draft(
    draft: StarRocksFragmentDraftEnvelope,
    resolved: crate::protocol::starrocks::decode::StarRocksResolvedDependencies,
) -> Result<PreparedStarRocksFragment, String> {
    let decoded =
        finish_fragment_submission(draft.draft, resolved).map_err(|error| error.to_string())?;
    let (submission, metadata) = decoded.into_parts();
    Ok(PreparedStarRocksFragment {
        submission,
        metadata,
        total_fragments: draft.total_fragments,
    })
}

struct PreparedLaunchResources {
    finst_id: UniqueId,
    execution: QueryExecutionKey,
    profiler: Option<Profiler>,
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    prepared: PreparedStarRocksFragment,
}

fn same_row_position_descriptor(
    left: &crate::exec::row_position::RowPositionDescriptor,
    right: &crate::exec::row_position::RowPositionDescriptor,
) -> bool {
    left.row_position_type == right.row_position_type
        && left.row_source_slot == right.row_source_slot
        && left.fetch_ref_slots == right.fetch_ref_slots
        && left.lookup_ref_slots == right.lookup_ref_slots
}

fn prepare_query_handoff(
    prepared: &[PreparedStarRocksFragment],
    generation: u64,
) -> Result<StarRocksQueryHandoff, String> {
    let first = prepared
        .first()
        .ok_or_else(|| "StarRocks handoff requires at least one fragment".to_string())?;
    let query_id = first.submission.instance().query_id();
    let generation = StarRocksQueryGeneration::new(generation)?;
    let execution = QueryExecutionKey::starrocks(query_id, generation);
    let query_options = first
        .submission
        .instance()
        .runtime_options()
        .query_options();
    let cache_options = CacheOptions::from_query_options(Some(query_options))?;
    let (delivery_expire, query_expire) = query_expire_durations(Some(query_options));
    let mut descriptor_snapshot = None;
    let mut total_fragments = None;
    let mut row_pos_descs = HashMap::new();
    let mut lookup_fetchers = HashMap::new();
    let mut instances = Vec::with_capacity(prepared.len());

    for item in prepared {
        let instance = item.submission.instance();
        if instance.query_id() != query_id {
            return Err("mixed query_id in prepared StarRocks batch".to_string());
        }
        let incoming_cache =
            CacheOptions::from_query_options(Some(instance.runtime_options().query_options()))?;
        if incoming_cache != cache_options {
            return Err("cache options mismatch for query".to_string());
        }
        if let Some(snapshot) = item.metadata.descriptor_snapshot() {
            descriptor_snapshot = Some(Arc::new(snapshot.clone()));
        }
        if let Some(incoming_total) = item.total_fragments {
            total_fragments = Some(
                total_fragments
                    .map_or(incoming_total, |current: usize| current.max(incoming_total)),
            );
        }
        for (tuple_id, incoming) in item.metadata.row_position_descriptors() {
            if let Some(existing) = row_pos_descs.get(tuple_id)
                && !same_row_position_descriptor(existing, incoming)
            {
                return Err(format!(
                    "conflicting row position descriptor for tuple_id={tuple_id}"
                ));
            }
            row_pos_descs
                .entry(*tuple_id)
                .or_insert_with(|| incoming.clone());
        }
        for (node_id, incoming) in item.metadata.lookup_fetcher_lifecycles() {
            lookup_fetchers
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
        instances.push((
            instance.fragment_instance_id().get(),
            item.submission.incremental_scan_contracts(),
        ));
    }

    Ok(StarRocksQueryHandoff {
        execution,
        delivery_expire,
        query_expire,
        fragment_count: prepared.len(),
        cache_options,
        descriptor_snapshot,
        total_fragments,
        row_pos_descs,
        lookup_fetchers,
        instances,
    })
}

fn profile_report_interval_ns(
    enable_profile: bool,
    query_options: &crate::runtime::query_options::QueryOptions,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    query_options
        .runtime_profile_report_interval
        .filter(|value| *value > 0)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .or_else(|| {
            app_config::config()
                .ok()
                .map(|config| config.runtime.profile_report_interval.max(1) * 1_000_000_000)
        })
}

fn launch_prepared_fragments(
    prepared: Vec<PreparedStarRocksFragment>,
    descriptor_preparation: StarRocksDescriptorPreparation,
    guard: StarRocksPrelaunchGuard,
) -> Result<usize, String> {
    if prepared.is_empty() {
        return Ok(0);
    }
    let mgr = query_context_manager();
    let query_id = prepared[0].submission.instance().query_id();
    if prepared
        .iter()
        .any(|item| item.submission.instance().query_id() != query_id)
    {
        return Err("mixed query_id in prepared StarRocks batch".to_string());
    }
    let handoff = prepare_query_handoff(&prepared, descriptor_preparation.generation())?;
    let execution = handoff.execution;
    let query_mem_tracker = guard.handoff(|| {
        commit_descriptor_handoff(&descriptor_preparation, |lease_factory| {
            mgr.commit_starrocks_handoff(handoff, || {
                lease_factory.map(|factory| factory.into_cleanup_lease())
            })
        })
    })?;

    let mut launches = Vec::with_capacity(prepared.len());
    for prepared in prepared {
        let instance = prepared.submission.instance();
        let finst_id = instance.fragment_instance_id().get();
        let query_options = instance.runtime_options().query_options().clone();
        let fragment_mem_tracker = MemTracker::new_child(
            format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo),
            &query_mem_tracker,
        );
        let profiler = query_options.enable_profile.then(|| {
            Profiler::new(format!(
                "execute_fragment (plan_node_id={})",
                prepared.submission.program().root_plan_node_id().get()
            ))
        });
        launches.push(PreparedLaunchResources {
            finst_id,
            execution,
            profiler,
            query_mem_tracker: Arc::clone(&query_mem_tracker),
            fragment_mem_tracker,
            prepared,
        });
    }
    let created = launches.len();
    for launch in launches {
        let instance = launch.prepared.submission.instance();
        let query_options = instance.runtime_options().query_options();
        let enable_profile = query_options.enable_profile;
        let report_interval_ns = profile_report_interval_ns(enable_profile, query_options);
        match launch.prepared.metadata.report_destination() {
            Some(StarRocksReportDestination::NovaRocks(endpoint)) => {
                fe_report::register_novarocks_instance(
                    launch.finst_id,
                    launch.execution.query_id(),
                    endpoint.clone(),
                    instance.backend_num().get(),
                    enable_profile,
                    launch.profiler.clone(),
                    Some(Arc::clone(&launch.fragment_mem_tracker)),
                    Some(Arc::clone(&launch.query_mem_tracker)),
                    report_interval_ns,
                );
            }
            Some(StarRocksReportDestination::Coordinator(endpoint)) => {
                fe_report::register_instance(
                    launch.finst_id,
                    launch.execution.query_id(),
                    types::TNetworkAddress::new(endpoint.host().to_string(), endpoint.port()),
                    instance.backend_num().get(),
                    enable_profile,
                    launch.profiler.clone(),
                    Some(Arc::clone(&launch.fragment_mem_tracker)),
                    Some(Arc::clone(&launch.query_mem_tracker)),
                    report_interval_ns,
                );
            }
            None => warn!(
                target: "novarocks::report",
                finst_id = %launch.finst_id,
                "missing report destination for reportExecStatus"
            ),
        }
        spawn_exec_fragment(
            launch.prepared,
            launch.finst_id,
            launch.execution,
            launch.profiler,
            Some(launch.fragment_mem_tracker),
            Arc::clone(&mgr),
        );
    }
    Ok(created)
}

fn spawn_exec_fragment(
    prepared: PreparedStarRocksFragment,
    finst_id: UniqueId,
    execution: QueryExecutionKey,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<crate::runtime::mem_tracker::MemTracker>>,
    mgr: Arc<QueryContextManager>,
) {
    let query_id = execution.query_id();
    #[cfg(test)]
    TEST_FRAGMENT_LAUNCH_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let uses_fetch_result_buffer = uses_fetch_result_buffer(&prepared.submission);
    let lookup_close_targets = prepared
        .metadata
        .lookup_close_targets()
        .iter()
        .map(|target| LookupCloseTarget {
            lookup_node_id: target.lookup_node_id(),
            host: target.host().to_string(),
            port: i32::from(target.port()),
        })
        .collect();
    if uses_fetch_result_buffer {
        if prepared
            .submission
            .instance()
            .runtime_options()
            .typed_result_sink()
        {
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
    std::thread::spawn(move || {
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let runtime_metadata = StarRocksExecutionMetadata {
            result_override: prepared.metadata.result_override().cloned(),
            root_sink_dop: prepared.metadata.root_sink_dop(),
            group_execution_scan_dop: prepared.metadata.group_execution_scan_dop(),
        };
        let out = {
            // One guard per fragment instance, not per pipeline driver. Dropping it after the
            // entire executor returns mirrors StarRocks FetchProcessorFactory::close_context.
            let _lookup_close_guard = LookupCloseGuard {
                query_id,
                targets: lookup_close_targets,
            };
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                execute_starrocks_submission(
                    prepared.submission,
                    runtime_metadata,
                    StarRocksExecutionContext {
                        profiler,
                        mem_tracker,
                    },
                )
                .map_err(|error| error.to_string())
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
        run_async_cleanup_sequence(
            report_error,
            |err_msg| {
                let finsts = mgr.cancel_query_execution(execution, err_msg.to_string());
                for id in finsts {
                    result_buffer::close_error(id, err_msg.to_string());
                    exchange::cancel_fragment(id.hi, id.lo);
                }
            },
            || mgr.finish_fragment_for_report_execution(execution),
            |error, decision| {
                fe_report::report_fragment_done(
                    finst_id,
                    error,
                    decision.include_runtime_filter_profile,
                );
            },
            || exchange::remove_fragment(finst_id.hi, finst_id.lo),
            || mgr.unregister_finst_execution(finst_id, execution),
            |decision| mgr.cleanup_after_fragment_report(query_id, decision),
        );
    });
}

fn run_async_cleanup_sequence<T>(
    report_error: Option<String>,
    cancel: impl FnOnce(&str),
    finish_for_report: impl FnOnce() -> T,
    report_done: impl FnOnce(Option<String>, &T),
    remove_exchange: impl FnOnce(),
    unregister_finst: impl FnOnce(),
    cleanup_after_report: impl FnOnce(T),
) {
    if let Some(error) = report_error.as_deref() {
        cancel(error);
    }
    let decision = finish_for_report();
    report_done(report_error, &decision);
    remove_exchange();
    unregister_finst();
    cleanup_after_report(decision);
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
    if unique.is_empty() {
        return Ok(0);
    }
    let sender_counts = collect_exchange_sender_counts(common, &unique);
    let common_desc_tbl = common.and_then(|value| value.desc_tbl.as_ref());
    let mut envelopes = Vec::with_capacity(unique.len());
    let mut finst_ids = Vec::with_capacity(unique.len());
    let mut query_id_for_batch = None;
    for one in &unique {
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
        let exec_params = params.ok_or_else(|| {
            "missing params in exec_batch_plan_fragments unique instance".to_string()
        })?;
        let fragment = fragment.ok_or_else(|| {
            "missing fragment in exec_batch_plan_fragments unique instance".to_string()
        })?;

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

        let finst_id = UniqueId {
            hi: exec_params.fragment_instance_id.hi,
            lo: exec_params.fragment_instance_id.lo,
        };
        let mut exec_params = exec_params.clone();
        backfill_per_node_scan_ranges(&mut exec_params);
        finst_ids.push(finst_id);
        envelopes.push((
            fragment,
            exec_params,
            query_opts,
            query_globals,
            db_name,
            coord,
            novarocks_report_addr,
            backend_num,
            resolve_pipeline_dop(one),
            one.group_execution_scan_dop,
            typed_result_sink,
            one.desc_tbl.as_ref(),
        ));
    }
    let query_id = query_id_for_batch.expect("non-empty batch has query id");
    let unique_descriptors = envelopes.iter().map(|entry| entry.11).collect::<Vec<_>>();
    let descriptor_preparation =
        prepare_batch_descriptor(query_id, common_desc_tbl, &unique_descriptors)?;
    let generation = descriptor_preparation.generation();
    let mut guard =
        starrocks_prelaunch_registry().install(query_id, generation, finst_ids.clone())?;
    let frontend_endpoint = envelopes
        .first()
        .and_then(|entry| entry.5)
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("coord"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    guard.set_frontend_endpoint(frontend_endpoint);
    let token = guard.cancellation_token();
    let mut drafts = Vec::with_capacity(envelopes.len());
    for entry in envelopes {
        drafts.push(prepare_starrocks_draft(
            entry.0,
            descriptor_preparation.descriptor(),
            &entry.1,
            entry.2,
            entry.3,
            entry.4,
            entry.5,
            entry.6.as_ref(),
            entry.7,
            entry.8,
            entry.9,
            &sender_counts,
            entry.10,
        )?);
    }
    token.check(0).map_err(|error| error.to_string())?;
    let resolutions = drafts
        .iter()
        .map(|draft| resolve_starrocks_draft(draft, &token))
        .collect::<Result<Vec<_>, _>>()?;
    token.check(0).map_err(|error| error.to_string())?;
    let prepared = drafts
        .into_iter()
        .zip(resolutions)
        .map(|(draft, resolved)| finish_starrocks_draft(draft, resolved))
        .collect::<Result<Vec<_>, _>>()?;
    launch_prepared_fragments(prepared, descriptor_preparation, guard)
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
    let finst_id = UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    };
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };
    let mut params = params.clone();
    backfill_per_node_scan_ranges(&mut params);
    let descriptor_preparation = prepare_descriptor(query_id, one.desc_tbl.as_ref(), None)?;
    let mut guard = starrocks_prelaunch_registry().install(
        query_id,
        descriptor_preparation.generation(),
        [finst_id],
    )?;
    guard.set_frontend_endpoint(
        one.coord
            .as_ref()
            .map(|address| {
                decode_runtime_endpoint(
                    address,
                    FieldPath::root("exec_plan_fragment").field("coord"),
                )
                .map_err(|error| error.to_string())
            })
            .transpose()?,
    );
    let token = guard.cancellation_token();
    let draft = prepare_starrocks_draft(
        fragment,
        descriptor_preparation.descriptor(),
        &params,
        one.query_options.as_ref(),
        one.query_globals.as_ref(),
        one.db_name.as_deref(),
        one.coord.as_ref(),
        one.novarocks_report_addr.as_ref(),
        one.backend_num,
        resolve_pipeline_dop(&one),
        one.group_execution_scan_dop,
        &HashMap::new(),
        one.novarocks_typed_result_sink.unwrap_or(false),
    )?;
    let resolved = resolve_starrocks_draft(&draft, &token)?;
    let prepared = finish_starrocks_draft(draft, resolved)?;
    launch_prepared_fragments(vec![prepared], descriptor_preparation, guard)?;
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

    let mut params = params.clone();
    backfill_per_node_scan_ranges(&mut params);
    let descriptor_preparation = prepare_descriptor(query_id, one.desc_tbl.as_ref(), None)?;
    let mut guard = starrocks_prelaunch_registry().install(
        query_id,
        descriptor_preparation.generation(),
        [finst_id],
    )?;
    guard.set_frontend_endpoint(
        one.coord
            .as_ref()
            .map(|address| {
                decode_runtime_endpoint(
                    address,
                    FieldPath::root("exec_plan_fragment").field("coord"),
                )
                .map_err(|error| error.to_string())
            })
            .transpose()?,
    );
    let token = guard.cancellation_token();
    let draft = prepare_starrocks_draft(
        fragment,
        descriptor_preparation.descriptor(),
        &params,
        one.query_options.as_ref(),
        one.query_globals.as_ref(),
        one.db_name.as_deref(),
        one.coord.as_ref(),
        one.novarocks_report_addr.as_ref(),
        one.backend_num,
        resolve_pipeline_dop(&one),
        one.group_execution_scan_dop,
        &HashMap::new(),
        one.novarocks_typed_result_sink.unwrap_or(false),
    )?;
    let resolved = resolve_starrocks_draft(&draft, &token)?;
    let prepared = finish_starrocks_draft(draft, resolved)?;

    let runtime_metadata = StarRocksExecutionMetadata {
        result_override: prepared.metadata.result_override().cloned(),
        root_sink_dop: prepared.metadata.root_sink_dop(),
        group_execution_scan_dop: prepared.metadata.group_execution_scan_dop(),
    };
    let lookup_close_targets = prepared
        .metadata
        .lookup_close_targets()
        .iter()
        .map(|target| LookupCloseTarget {
            lookup_node_id: target.lookup_node_id(),
            host: target.host().to_string(),
            port: i32::from(target.port()),
        })
        .collect();

    let mgr = query_context_manager();
    let handoff = prepare_query_handoff(
        std::slice::from_ref(&prepared),
        descriptor_preparation.generation(),
    )?;
    let execution = handoff.execution;
    let query_mem_tracker = guard.handoff(|| {
        commit_descriptor_handoff(&descriptor_preparation, |lease_factory| {
            mgr.commit_starrocks_handoff(handoff, || {
                lease_factory.map(|factory| factory.into_cleanup_lease())
            })
        })
    })?;
    let fragment_mem_tracker = MemTracker::new_child(
        format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo),
        &query_mem_tracker,
    );

    let exec_result = {
        let _lookup_close_guard = LookupCloseGuard {
            query_id,
            targets: lookup_close_targets,
        };
        execute_starrocks_submission(
            prepared.submission,
            runtime_metadata,
            StarRocksExecutionContext {
                profiler: None,
                mem_tracker: Some(fragment_mem_tracker),
            },
        )
        .map_err(|error| error.to_string())
    };
    exchange::remove_fragment(finst_id.hi, finst_id.lo);
    mgr.unregister_finst_execution(finst_id, execution);
    mgr.finish_fragment_execution(execution);

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;

    use crate::common::thrift::{thrift_binary_deserialize, thrift_binary_serialize};
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::service::starrocks_fragment_transport::{
        descriptor_cache_snapshot_count, starrocks_prelaunch_registry,
    };
    use crate::thrift::{data_sinks, internal_service, partitions, plan_nodes, planner, types};

    use super::{
        TEST_FRAGMENT_LAUNCH_COUNT, run_async_cleanup_sequence, submit_exec_batch_plan_fragments,
        submit_exec_plan_fragment,
    };

    #[derive(Debug, Eq, PartialEq)]
    struct RegistrationSnapshot {
        query_context: bool,
        finst_mapping: Option<QueryId>,
        reporter: bool,
        launch_count: usize,
    }

    #[derive(Debug, Eq, PartialEq)]
    struct HandoffSnapshot {
        prelaunch_count: usize,
        descriptor_cache_count: usize,
        fragment_counts: Option<(usize, usize)>,
        finst_mappings: Vec<Option<QueryId>>,
        runtime_filter_lifecycle: bool,
        launch_count: usize,
    }

    fn handoff_snapshot(query_id: QueryId, finst_ids: &[UniqueId]) -> HandoffSnapshot {
        HandoffSnapshot {
            prelaunch_count: starrocks_prelaunch_registry().snapshot_count(),
            descriptor_cache_count: descriptor_cache_snapshot_count(),
            fragment_counts: query_context_manager().fragment_counts_for_test(query_id),
            finst_mappings: finst_ids
                .iter()
                .map(|finst_id| query_context_manager().query_id_by_finst(*finst_id))
                .collect(),
            runtime_filter_lifecycle: RuntimeFilterLifecycleRegistry::global()
                .snapshot(QueryKey::from_hi_lo(query_id.hi, query_id.lo))
                .is_some(),
            launch_count: TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst),
        }
    }

    fn registration_snapshot(query_id: QueryId, finst_id: UniqueId) -> RegistrationSnapshot {
        RegistrationSnapshot {
            query_context: query_context_manager()
                .with_context_mut(query_id, |_| Ok(()))
                .is_ok(),
            finst_mapping: query_context_manager().query_id_by_finst(finst_id),
            reporter: crate::service::fe_report::list_report_instances()
                .iter()
                .any(|(registered, _)| *registered == finst_id),
            launch_count: TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst),
        }
    }

    fn empty_set_node() -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode::new(
            11,
            plan_nodes::TPlanNodeType::EMPTY_SET_NODE,
            0,
            -1,
            vec![],
            vec![],
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn noop_sink() -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn fragment(plan: Option<plan_nodes::TPlan>) -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            plan,
            None,
            noop_sink(),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None,
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn params(query: UniqueId, finst: UniqueId) -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId::new(query.hi, query.lo),
            types::TUniqueId::new(finst.hi, finst.lo),
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn request(
        query: UniqueId,
        finst: UniqueId,
        fragment: planner::TPlanFragment,
    ) -> internal_service::TExecPlanFragmentParams {
        internal_service::TExecPlanFragmentParams::new(
            internal_service::InternalServiceVersion::V1,
            Some(fragment),
            None,
            Some(params(query, finst)),
            None,
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(true),
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn query_options_with_cache_probability(probability: i32) -> internal_service::TQueryOptions {
        let mut options: internal_service::TQueryOptions =
            thrift_binary_deserialize(&[0]).expect("empty query options");
        options.datacache_evict_probability = Some(probability);
        options
    }

    #[test]
    fn malformed_fragment_fails_before_registration() {
        let query = UniqueId {
            hi: 85_001,
            lo: 85_002,
        };
        let finst = UniqueId {
            hi: 85_003,
            lo: 85_004,
        };
        let request = request(query, finst, fragment(None));
        let before = registration_snapshot(
            QueryId {
                hi: query.hi,
                lo: query.lo,
            },
            finst,
        );

        let result = submit_exec_plan_fragment(
            &thrift_binary_serialize(&request).expect("serialize malformed request"),
        );
        let after = registration_snapshot(
            QueryId {
                hi: query.hi,
                lo: query.lo,
            },
            finst,
        );

        assert!(
            result.is_err(),
            "malformed fragment must fail synchronously"
        );
        assert_eq!(after, before, "decode failure must not register or launch");
    }

    #[test]
    fn batch_second_unique_malformed_launches_nothing() {
        let query = UniqueId {
            hi: 85_101,
            lo: 85_102,
        };
        let first_finst = UniqueId {
            hi: 85_103,
            lo: 85_104,
        };
        let second_finst = UniqueId {
            hi: 85_105,
            lo: 85_106,
        };
        let valid = request(
            query,
            first_finst,
            fragment(Some(plan_nodes::TPlan::new(vec![empty_set_node()]))),
        );
        let malformed = request(query, second_finst, fragment(None));
        let batch = internal_service::TExecBatchPlanFragmentsParams::new(
            None,
            Some(vec![valid, malformed]),
        );
        let before = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);

        let result = submit_exec_batch_plan_fragments(
            &thrift_binary_serialize(&batch).expect("serialize malformed batch"),
        );
        let after = TEST_FRAGMENT_LAUNCH_COUNT.load(Ordering::SeqCst);

        assert!(
            result.is_err(),
            "batch decode must reject the malformed unique"
        );
        assert_eq!(
            after, before,
            "batch must launch no fragment on decode failure"
        );
        assert_eq!(query_context_manager().query_id_by_finst(first_finst), None);
        assert_eq!(
            query_context_manager().query_id_by_finst(second_finst),
            None
        );
    }

    #[test]
    fn batch_second_fragment_cache_options_conflict_leaves_handoff_unpublished() {
        let query = UniqueId {
            hi: 85_111,
            lo: 85_112,
        };
        let query_id = QueryId {
            hi: query.hi,
            lo: query.lo,
        };
        let first_finst = UniqueId {
            hi: 85_113,
            lo: 85_114,
        };
        let second_finst = UniqueId {
            hi: 85_115,
            lo: 85_116,
        };
        let plan = || fragment(Some(plan_nodes::TPlan::new(vec![empty_set_node()])));
        let mut first = request(query, first_finst, plan());
        first.query_options = Some(query_options_with_cache_probability(10));
        let mut second = request(query, second_finst, plan());
        second.query_options = Some(query_options_with_cache_probability(20));
        let batch =
            internal_service::TExecBatchPlanFragmentsParams::new(None, Some(vec![first, second]));
        let finst_ids = [first_finst, second_finst];
        let before = handoff_snapshot(query_id, &finst_ids);

        let result = submit_exec_batch_plan_fragments(
            &thrift_binary_serialize(&batch).expect("serialize cache-conflict batch"),
        );
        let after = handoff_snapshot(query_id, &finst_ids);

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("cache options mismatch")),
            "unexpected result: {result:?}"
        );
        assert_eq!(
            after, before,
            "handoff validation failure must leave P/D/Q/RF/mapping/launch unchanged"
        );
    }

    #[test]
    fn async_error_cleanup_preserves_report_and_release_order() {
        let events = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let record = |event: &'static str| {
            let events = std::rc::Rc::clone(&events);
            move || events.borrow_mut().push(event)
        };
        let cancel_events = std::rc::Rc::clone(&events);
        let report_events = std::rc::Rc::clone(&events);
        let cleanup_events = std::rc::Rc::clone(&events);

        run_async_cleanup_sequence(
            Some("execution failed".to_string()),
            move |error| {
                assert_eq!(error, "execution failed");
                cancel_events.borrow_mut().push("cancel-fanout");
            },
            || {
                record("finish-for-report")();
                7
            },
            move |error, decision| {
                assert_eq!(error.as_deref(), Some("execution failed"));
                assert_eq!(*decision, 7);
                report_events.borrow_mut().push("report-done");
            },
            record("exchange-remove"),
            record("finst-unregister"),
            move |decision| {
                assert_eq!(decision, 7);
                cleanup_events.borrow_mut().push("query-cleanup");
            },
        );

        assert_eq!(
            events.borrow().as_slice(),
            [
                "cancel-fanout",
                "finish-for-report",
                "report-done",
                "exchange-remove",
                "finst-unregister",
                "query-cleanup",
            ]
        );
    }
}
