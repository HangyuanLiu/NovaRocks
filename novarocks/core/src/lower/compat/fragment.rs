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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
use crate::exec::row_position::RowPositionDescriptor;
use crate::novarocks_connectors::ConnectorRegistry;

use crate::cache::DataCacheManager;
use crate::common::config::debug_exec_node_output;
use crate::common::types::UniqueId;
use crate::exec::pipeline::executor::execute_compat_plan_with_pipeline_with_root_sink_dop;
use crate::protocol::common::error::FieldPath;
use crate::protocol::starrocks::decode::layout::{
    build_tuple_slot_order, infer_tuple_slot_order, reorder_tuple_slots,
};
use crate::protocol::starrocks::decode::node::{StarRocksPlanDecodeContext, lower_plan};
use crate::protocol::starrocks::decode::sink::fragment::{
    DecodedStarRocksFragmentSink, decode_fragment_sink,
};
use crate::protocol::starrocks::decode::{
    StarRocksDecodeFacts, StarRocksExternalDependency, StarRocksExternalDependencyDraft,
    StarRocksJdbcFacts, StarRocksObjectStoreDefaults, StarRocksPathRewriteFacts,
    decode_fragment_destination, decode_query_options, decode_runtime_endpoint,
    decode_runtime_filter_params,
};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::sink::materialize_fragment_sink_components_with_result;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_params::RuntimeFilterParams;
use crate::thrift::{descriptors, internal_service, planner, types};

enum FragmentDecodeAttempt<T> {
    Ready(T),
    Pending(Vec<StarRocksExternalDependency>),
    DecodeError(String),
}

fn classify_fragment_decode_attempt<T>(
    result: Result<T, String>,
    draft: &StarRocksExternalDependencyDraft,
) -> FragmentDecodeAttempt<T> {
    match result {
        Err(error) => FragmentDecodeAttempt::DecodeError(error),
        Ok(value) => {
            let requirements = draft.external_dependencies();
            if requirements.is_empty() {
                FragmentDecodeAttempt::Ready(value)
            } else {
                // Discard the draft value: it may contain dependency placeholders.
                FragmentDecodeAttempt::Pending(requirements)
            }
        }
    }
}

fn process_fragment_decode_attempt<T>(
    attempt: FragmentDecodeAttempt<T>,
    mut resolve: impl FnMut(&StarRocksExternalDependency) -> Result<bool, String>,
) -> Result<Option<T>, String> {
    match attempt {
        FragmentDecodeAttempt::Ready(value) => Ok(Some(value)),
        FragmentDecodeAttempt::DecodeError(error) => Err(error),
        FragmentDecodeAttempt::Pending(requirements) => {
            let mut resolved = 0usize;
            for requirement in &requirements {
                resolved += usize::from(resolve(requirement)?);
            }
            if resolved != requirements.len() {
                return Err(format!(
                    "StarRocks plan decode dependency resolution completed {resolved}/{} requirements",
                    requirements.len(),
                ));
            }
            Ok(None)
        }
    }
}

fn merge_row_pos_descs(
    target: &mut HashMap<i32, RowPositionDescriptor>,
    incoming: &HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    for (tuple_id, desc) in incoming {
        match target.get(tuple_id) {
            None => {
                target.insert(*tuple_id, desc.clone());
            }
            Some(existing) => {
                if existing.row_position_type != desc.row_position_type
                    || existing.row_source_slot != desc.row_source_slot
                    || existing.fetch_ref_slots != desc.fetch_ref_slots
                    || existing.lookup_ref_slots != desc.lookup_ref_slots
                {
                    return Err(format!(
                        "conflicting row position descriptor for tuple_id={}",
                        tuple_id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn collect_glm_metadata(
    node: &ExecNode,
    row_pos_descs: &mut HashMap<i32, RowPositionDescriptor>,
) -> Result<(), String> {
    match &node.kind {
        ExecNodeKind::LookUp(lookup) => {
            merge_row_pos_descs(row_pos_descs, &lookup.row_pos_descs)?;
        }
        ExecNodeKind::Fetch(fetch) => {
            merge_row_pos_descs(row_pos_descs, &fetch.row_pos_descs)?;
            collect_glm_metadata(&fetch.input, row_pos_descs)?;
        }
        ExecNodeKind::AssertNumRows(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Project(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Filter(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Repeat(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ChangeEventExpand(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::UnionAll(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::Limit(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::ExchangeSource(_) => {}
        ExecNodeKind::Scan(_) => {}
        ExecNodeKind::Aggregate(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Join(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::NestedLoopJoin(node) => {
            collect_glm_metadata(&node.left, row_pos_descs)?;
            collect_glm_metadata(&node.right, row_pos_descs)?;
        }
        ExecNodeKind::Sort(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::TableFunction(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Analytic(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::SetOp(node) => {
            for input in &node.inputs {
                collect_glm_metadata(input, row_pos_descs)?;
            }
        }
        ExecNodeKind::NativeRuntimeFilterConsumer(node) => {
            collect_glm_metadata(&node.input, row_pos_descs)?;
        }
        ExecNodeKind::Values(_) => {}
        ExecNodeKind::IcebergDeltaScan(_) => {}
    }
    Ok(())
}

fn unique_id_from_exec_params(exec_params: &internal_service::TPlanFragmentExecParams) -> UniqueId {
    UniqueId {
        hi: exec_params.fragment_instance_id.hi,
        lo: exec_params.fragment_instance_id.lo,
    }
}

fn require_exec_params_for_sink(
    sink: &crate::thrift::data_sinks::TDataSink,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<(), String> {
    if exec_params.is_some() {
        return Ok(());
    }
    let label = match sink.type_ {
        crate::thrift::data_sinks::TDataSinkType::DATA_STREAM_SINK => "DATA_STREAM_SINK",
        crate::thrift::data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
            "MULTI_CAST_DATA_STREAM_SINK"
        }
        crate::thrift::data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK => {
            "SPLIT_DATA_STREAM_SINK"
        }
        crate::thrift::data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK => {
            "ICEBERG_CHANGE_STREAM_ROUTER_SINK"
        }
        _ => return Ok(()),
    };
    Err(format!("{label} requires exec_params"))
}

fn snapshot_decode_facts(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<StarRocksDecodeFacts, String> {
    let mut stream_load_paths = BTreeMap::new();
    if let Some(exec_params) = exec_params {
        for ranges in exec_params.per_node_scan_ranges.values() {
            for params in ranges {
                let Some(broker) = params.scan_range.broker_scan_range.as_ref() else {
                    continue;
                };
                for range in &broker.ranges {
                    if range.file_type != types::TFileType::FILE_STREAM {
                        continue;
                    }
                    let load_id = range
                        .load_id
                        .as_ref()
                        .ok_or_else(|| "FILE_STREAM range is missing load_id".to_string())?;
                    let path = crate::service::stream_load_registry::resolve_stream_load_file_path(
                        load_id,
                    )
                    .ok_or_else(|| {
                        format!(
                            "no registered local file for FILE_STREAM load_id={}:{}",
                            load_id.hi, load_id.lo
                        )
                    })?;
                    stream_load_paths.insert(
                        UniqueId {
                            hi: load_id.hi,
                            lo: load_id.lo,
                        },
                        path,
                    );
                }
            }
        }
    }

    let config = crate::common::app_config::config().map_err(|error| error.to_string())?;
    let rewrite = &config.runtime.path_rewrite;
    let path_rewrite = rewrite.enable.then(|| {
        StarRocksPathRewriteFacts::new(rewrite.from_prefix.clone(), rewrite.to_prefix.clone())
    });
    let datacache_available = config.runtime.cache.datacache_enable
        && DataCacheManager::instance().block_cache().is_some();
    let jdbc = config.jdbc_config().map(|jdbc| {
        StarRocksJdbcFacts::new(
            jdbc.url.clone(),
            jdbc.user.clone(),
            jdbc.password.clone(),
            jdbc.default_db.clone(),
        )
    });
    let object_storage = &config.runtime.object_storage;
    let object_store_defaults = StarRocksObjectStoreDefaults::new(
        object_storage.retry_max_times,
        object_storage.retry_min_delay_ms,
        object_storage.retry_max_delay_ms,
        object_storage.timeout_ms,
        object_storage.io_timeout_ms,
    );
    Ok(StarRocksDecodeFacts::new(
        stream_load_paths,
        path_rewrite,
        datacache_available,
        jdbc,
        object_store_defaults,
    ))
}

#[cfg(feature = "compat")]
fn runtime_query_options_from_thrift(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> Result<Option<QueryOptions>, String> {
    query_opts
        .map(|opts| decode_query_options(Some(opts)).map_err(|error| error.to_string()))
        .transpose()
}

#[cfg(not(feature = "compat"))]
fn runtime_query_options_from_thrift(
    query_opts: Option<&internal_service::TQueryOptions>,
) -> Result<Option<QueryOptions>, String> {
    if query_opts.is_some() {
        return Err("thrift query options require the compat feature".to_string());
    }
    Ok(None)
}

#[cfg(feature = "compat")]
fn runtime_filter_params_from_thrift(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<Option<RuntimeFilterParams>, String> {
    exec_params
        .and_then(|params| params.runtime_filter_params.as_ref())
        .map(|params| {
            decode_runtime_filter_params(
                params,
                FieldPath::root("exec_plan_fragment")
                    .field("params")
                    .field("runtime_filter_params"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()
}

#[cfg(not(feature = "compat"))]
fn runtime_filter_params_from_thrift(
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
) -> Result<Option<RuntimeFilterParams>, String> {
    if exec_params
        .and_then(|params| params.runtime_filter_params.as_ref())
        .is_some()
    {
        return Err("thrift runtime filter params require the compat feature".to_string());
    }
    Ok(None)
}

pub(crate) fn execute_fragment(
    fragment: &planner::TPlanFragment,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    query_opts: Option<&internal_service::TQueryOptions>,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    _group_execution_scan_dop: Option<i32>,
    db_name: Option<&str>,
    profiler: Option<Profiler>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>>,
    typed_result_sink: bool,
) -> Result<FragmentOutput, String> {
    if let Some(sink) = fragment.output_sink.as_ref() {
        require_exec_params_for_sink(sink, exec_params)?;
    }
    let runtime_fe_addr = fe_addr
        .map(|address| {
            decode_runtime_endpoint(
                address,
                FieldPath::root("exec_plan_fragment").field("coord"),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()?;
    let runtime_query_opts = runtime_query_options_from_thrift(query_opts)?;
    let runtime_query_opts = apply_query_option_overrides(runtime_query_opts);

    let profile_name = fragment
        .plan
        .as_ref()
        .and_then(|plan| plan.nodes.first().map(|n| n.node_id))
        .filter(|id| *id >= 0)
        .map(|id| format!("execute_fragment (plan_node_id={id})"));
    let profiler = if profiler.is_some() {
        profiler
    } else if runtime_query_opts
        .as_ref()
        .map(|opts| opts.enable_profile)
        .unwrap_or(false)
    {
        Some(Profiler::new(
            profile_name.as_deref().unwrap_or("execute_fragment"),
        ))
    } else {
        None
    };

    let query_id = exec_params.map(|params| QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    });
    let runtime_filter_params = runtime_filter_params_from_thrift(exec_params)?;
    let fragment_instance_id = exec_params.map(|params| UniqueId {
        hi: params.fragment_instance_id.hi,
        lo: params.fragment_instance_id.lo,
    });
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: runtime_query_opts.clone(),
            query_id,
            runtime_filter_params,
            fragment_instance_id,
            backend_num,
            mem_tracker,
        },
        profiler.as_ref(),
    )?;

    if let Some(plan) = fragment.plan.as_ref() {
        let mut tuple_slots = build_tuple_slot_order(desc_tbl);
        let inferred = infer_tuple_slot_order(fragment);
        if tuple_slots.is_empty() {
            tuple_slots = inferred.clone();
        } else {
            for (tuple_id, slots) in &inferred {
                if tuple_slots.contains_key(tuple_id) {
                    continue;
                }
                tuple_slots.insert(*tuple_id, slots.clone());
            }
        }
        reorder_tuple_slots(&mut tuple_slots, desc_tbl);
        let allow_throw_exception = runtime_query_opts
            .as_ref()
            .map(|opts| opts.allow_throw_exception)
            .unwrap_or(false);
        let allow_throw_exception = allow_throw_exception
            || query_opts.is_some_and(|opts| {
                matches!(
                    opts.overflow_mode,
                    Some(mode) if mode == internal_service::TOverflowMode::REPORT_ERROR
                )
            });
        // Layout hints are used by scan nodes to decide which columns to materialize.
        //
        // For exchange fragments, pruning only by "local usage" is not correct because downstream
        // fragments may require additional columns that do not appear in this fragment's exprs.
        // The descriptor table already encodes the materialized slots for each tuple, so we use it
        // as the source of truth to avoid producing mismatched layouts at runtime.
        let layout_hints = tuple_slots.clone();
        let connectors = ConnectorRegistry::default();
        let sink = fragment
            .output_sink
            .as_ref()
            .ok_or_else(|| "PlanFragment must have output_sink field".to_string())?;
        let mut resolved_query_profiles = BTreeMap::new();
        let mut resolved_lake_meta_storage = BTreeMap::new();
        let (arena, lowered, prepared_sink) = loop {
            let external_dependencies =
                StarRocksExternalDependencyDraft::new_with_lake_meta_storage(
                    runtime_fe_addr.clone(),
                    resolved_query_profiles.clone(),
                    resolved_lake_meta_storage.clone(),
                );
            let mut arena = ExprArena::default();
            arena.set_allow_throw_exception(allow_throw_exception);
            arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
            let decode_result = {
                let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
                (|| {
                    let decode_facts = snapshot_decode_facts(exec_params)?;
                    let empty_scan_ranges = BTreeMap::new();
                    let raw_scan_ranges = exec_params
                        .map(|params| &params.per_node_scan_ranges)
                        .unwrap_or(&empty_scan_ranges);
                    let (_, scan_assignments) =
                        crate::protocol::starrocks::decode::decode_scan_contracts_and_assignments(
                            &plan.nodes,
                            raw_scan_ranges,
                            &decode_facts,
                            FieldPath::root("exec_plan_fragment")
                                .field("params")
                                .field("per_node_scan_ranges"),
                        )
                        .map_err(|error| error.to_string())?;
                    let broker_file_program_facts =
                        crate::protocol::starrocks::decode::node::decode_broker_file_program_facts(
                            &plan.nodes,
                            raw_scan_ranges,
                            &mut arena,
                            FieldPath::root("exec_plan_fragment")
                                .field("fragment")
                                .field("plan")
                                .field("nodes"),
                            FieldPath::root("exec_plan_fragment")
                                .field("params")
                                .field("per_node_scan_ranges"),
                        )
                        .map_err(|error| error.to_string())?;
                    let lake_scan_program_facts =
                        crate::protocol::starrocks::decode::decode_lake_scan_program_facts(
                            &plan.nodes,
                            raw_scan_ranges,
                            FieldPath::root("exec_plan_fragment")
                                .field("params")
                                .field("per_node_scan_ranges"),
                        )
                        .map_err(|error| error.to_string())?;
                    let lake_meta_scan_range_facts =
                        crate::protocol::starrocks::decode::decode_lake_meta_scan_range_facts(
                            &plan.nodes,
                            raw_scan_ranges,
                            FieldPath::root("exec_plan_fragment")
                                .field("params")
                                .field("per_node_scan_ranges"),
                        )
                        .map_err(|error| error.to_string())?;
                    let plan_context = StarRocksPlanDecodeContext::new(
                        exec_params.map(|params| QueryId {
                            hi: params.query_id.hi,
                            lo: params.query_id.lo,
                        }),
                        exec_params.map(unique_id_from_exec_params),
                        Some(&scan_assignments),
                        Some(&broker_file_program_facts),
                        Some(&lake_scan_program_facts),
                        Some(&lake_meta_scan_range_facts),
                        exec_params.map(|params| &params.per_exch_num_senders),
                        batch_exchange_sender_counts,
                        decode_query_options(query_opts).map_err(|error| error.to_string())?,
                        &decode_facts,
                    );
                    let lowered = lower_plan(
                        plan,
                        &mut arena,
                        &tuple_slots,
                        desc_tbl,
                        fragment.query_global_dicts.as_deref(),
                        fragment.query_global_dict_exprs.as_ref(),
                        &plan_context,
                        db_name,
                        &connectors,
                        &layout_hints,
                        last_query_id,
                        Some(&external_dependencies),
                        FieldPath::root("exec_plan_fragment")
                            .field("fragment")
                            .field("query_global_dicts"),
                        FieldPath::root("exec_plan_fragment")
                            .field("fragment")
                            .field("query_global_dict_exprs"),
                        FieldPath::root("exec_plan_fragment")
                            .field("fragment")
                            .field("plan"),
                    )
                    .map_err(|error| error.to_string())?;
                    let destinations = exec_params
                        .and_then(|params| params.destinations.as_deref())
                        .unwrap_or(&[])
                        .iter()
                        .enumerate()
                        .map(|(index, destination)| {
                            decode_fragment_destination(
                                destination,
                                FieldPath::root("exec_plan_fragment")
                                    .field("params")
                                    .field("destinations")
                                    .index(index),
                            )
                            .map_err(|error| error.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let prepared_sink = decode_fragment_sink(
                        sink,
                        fragment,
                        &destinations,
                        exec_params.and_then(|params| params.sender_id),
                        desc_tbl,
                        &mut arena,
                        &lowered,
                        last_query_id,
                        session_time_zone,
                        &external_dependencies,
                        FieldPath::root("exec_plan_fragment")
                            .field("fragment")
                            .field("output_sink"),
                        FieldPath::root("exec_plan_fragment").field("fragment"),
                    )
                    .map_err(|error| error.to_string())?;
                    Ok((lowered, prepared_sink))
                })()
            };
            let attempt = classify_fragment_decode_attempt(decode_result, &external_dependencies);
            let decoded = process_fragment_decode_attempt(
                attempt,
                |dependency| match dependency {
                    StarRocksExternalDependency::QueryProfile { query_id, .. } => {
                        if resolved_query_profiles.contains_key(query_id) {
                            return Ok(false);
                        }
                        let coord = fe_addr.ok_or_else(|| {
                        "StarRocks plan decode requires a frontend address to resolve query-profile dependencies"
                            .to_string()
                    })?;
                        let profile =
                            crate::service::fe_report::fetch_query_profile(coord, query_id)?;
                        resolved_query_profiles.insert(query_id.clone(), profile);
                        Ok(true)
                    }
                    StarRocksExternalDependency::LakeMetaStorage { id, request } => {
                        if resolved_lake_meta_storage.contains_key(id) {
                            return Ok(false);
                        }
                        let facts = crate::connector::starrocks::lake_meta_storage::resolve_lake_meta_storage(
                        request,
                    )?;
                        resolved_lake_meta_storage.insert(*id, facts);
                        Ok(true)
                    }
                },
            )?;
            if let Some((lowered, prepared_sink)) = decoded {
                break (arena, lowered, prepared_sink);
            }
        };

        let mut exec_plan = ExecPlan {
            arena,
            root: lowered.node,
        };
        if let Some(query_id) = query_id {
            let mut row_pos_descs = HashMap::new();
            collect_glm_metadata(&exec_plan.root, &mut row_pos_descs)?;
            if !row_pos_descs.is_empty() {
                query_context_manager().register_row_pos_descs(query_id, row_pos_descs)?;
            }
        }
        crate::protocol::starrocks::decode::runtime_filter_pushdown::push_down_local_runtime_filters(
            &mut exec_plan.root,
            &exec_plan.arena,
        );
        let root_plan_node_id = plan.nodes.first().map(|n| n.node_id).unwrap_or(-1);

        let DecodedStarRocksFragmentSink {
            spec,
            assignment,
            result_override,
            root_sink_dop,
        } = prepared_sink;
        let fragment_instance_id = exec_params
            .map(unique_id_from_exec_params)
            .unwrap_or(UniqueId { hi: 0, lo: 0 });
        let exchange_finst_id = exec_params.map(|params| {
            (
                params.fragment_instance_id.hi,
                params.fragment_instance_id.lo,
            )
        });
        let sink_factory = materialize_fragment_sink_components_with_result(
            &spec,
            &assignment,
            fragment_instance_id,
            typed_result_sink,
            root_plan_node_id,
            result_override,
        )
        .map_err(|error| error.to_string())?;
        let _exec_timer = profiler
            .as_ref()
            .map(|p| p.scoped_timer("PipelineExecuteTime"));
        execute_compat_plan_with_pipeline_with_root_sink_dop(
            exec_plan,
            debug_exec_node_output(),
            Duration::from_millis(50),
            sink_factory,
            exchange_finst_id,
            profiler.clone(),
            pipeline_dop,
            Arc::clone(&runtime_state),
            query_id,
            runtime_fe_addr.clone(),
            backend_num,
            root_sink_dop,
        )?;
        return Ok(FragmentOutput { profile_json: None });
    }

    Err("unsupported fragment: missing plan".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::protocol::starrocks::decode::layout::Layout;
    use crate::protocol::starrocks::decode::node::Lowered;
    use crate::protocol::starrocks::decode::sink::fragment::{
        iceberg_router_input_from_compat, multi_cast_inputs_from_compat,
    };
    use crate::runtime::fragment::instance::FragmentSinkAssignment;
    use crate::thrift::data_sinks;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TStringLiteral};
    use crate::thrift::partitions::{TDataPartition, TPartitionType};

    fn test_expr_node(
        node_type: TExprNodeType,
        type_: types::TTypeDesc,
        num_children: i32,
    ) -> TExprNode {
        TExprNode {
            node_type,
            type_,
            opcode: None,
            num_children,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: -1,
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
        }
    }

    fn get_query_profile_expr(query_id: &str) -> TExpr {
        let string_type = crate::types::arrow_thrift::thrift_type_desc_from_primitive(
            types::TPrimitiveType::VARCHAR,
        );
        let mut call = test_expr_node(TExprNodeType::FUNCTION_CALL, string_type.clone(), 1);
        call.fn_ = Some(types::TFunction::new(
            types::TFunctionName::new(None, "get_query_profile".to_string()),
            types::TFunctionBinaryType::BUILTIN,
            vec![string_type.clone()],
            string_type.clone(),
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
        ));
        let mut literal = test_expr_node(TExprNodeType::STRING_LITERAL, string_type, 0);
        literal.string_literal = Some(TStringLiteral::new(query_id.to_string()));
        TExpr::new(vec![call, literal])
    }

    fn profile_partitioned_stream_sink(query_id: &str) -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            7,
            TDataPartition::new(
                TPartitionType::HASH_PARTITIONED,
                vec![get_query_profile_expr(query_id)],
                None,
                None,
            ),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn profile_partitioned_data_sink(query_id: &str) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::DATA_STREAM_SINK,
            profile_partitioned_stream_sink(query_id),
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

    fn data_sink_with_split(
        sink_type: data_sinks::TDataSinkType,
        split: Option<data_sinks::TSplitDataStreamSink>,
    ) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            sink_type, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, split, None,
        )
    }

    #[test]
    fn distributed_stream_sinks_without_exec_params_preserve_fail_fast_boundary() {
        for (sink_type, label) in [
            (
                data_sinks::TDataSinkType::DATA_STREAM_SINK,
                "DATA_STREAM_SINK",
            ),
            (
                data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK,
                "MULTI_CAST_DATA_STREAM_SINK",
            ),
            (
                data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK,
                "SPLIT_DATA_STREAM_SINK",
            ),
            (
                data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK,
                "ICEBERG_CHANGE_STREAM_ROUTER_SINK",
            ),
        ] {
            let sink = data_sink_with_split(sink_type, None);
            let error = require_exec_params_for_sink(&sink, None)
                .expect_err("distributed stream sink must require exec params");
            assert_eq!(error, format!("{label} requires exec_params"));
        }
    }

    fn bool_literal_expr(value: bool) -> TExpr {
        let bool_type = crate::types::arrow_thrift::thrift_type_desc_from_primitive(
            types::TPrimitiveType::BOOLEAN,
        );
        let mut literal = test_expr_node(TExprNodeType::BOOL_LITERAL, bool_type, 0);
        literal.bool_literal = Some(crate::thrift::exprs::TBoolLiteral::new(value));
        TExpr::new(vec![literal])
    }

    fn test_fragment(sink: data_sinks::TDataSink) -> planner::TPlanFragment {
        planner::TPlanFragment::new(
            None,
            None,
            sink,
            TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn test_exec_params() -> internal_service::TPlanFragmentExecParams {
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId::new(1, 2),
            types::TUniqueId::new(3, 4),
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            Some(0),
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

    fn empty_lowered() -> Lowered {
        Lowered {
            node: ExecNode {
                kind: ExecNodeKind::Values(crate::exec::node::values::ValuesNode {
                    chunk: crate::exec::chunk::Chunk::default(),
                    node_id: 0,
                }),
            },
            layout: empty_layout(),
        }
    }

    fn unpartitioned_stream_sink() -> data_sinks::TDataStreamSink {
        data_sinks::TDataStreamSink::new(
            7,
            TDataPartition::new(TPartitionType::UNPARTITIONED, None, None, None),
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn destination_without_endpoint() -> data_sinks::TPlanFragmentDestination {
        data_sinks::TPlanFragmentDestination::new(types::TUniqueId::new(11, 12), None, None, None)
    }

    fn empty_layout() -> Layout {
        Layout {
            order: Vec::new(),
            index: HashMap::new(),
        }
    }

    #[test]
    fn decode_error_is_not_reclassified_as_pending() {
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let _ = draft.query_profile("query-7");
        let attempt =
            classify_fragment_decode_attempt::<()>(Err("malformed plan".to_string()), &draft);
        let mut resolver_calls = 0;

        let error = process_fragment_decode_attempt(attempt, |_| {
            resolver_calls += 1;
            Ok(true)
        })
        .expect_err("a real decode failure must be preserved");

        assert_eq!(error, "malformed plan");
        assert_eq!(resolver_calls, 0);
    }

    #[test]
    fn successful_decode_with_requirements_is_pending_not_ready() {
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let _ = draft.query_profile("query-7");

        let attempt = classify_fragment_decode_attempt(Ok(7), &draft);

        assert!(matches!(attempt, FragmentDecodeAttempt::Pending(_)));
    }

    #[test]
    fn stream_sink_dependency_is_resolved_once_before_decode_is_ready() {
        let sink = profile_partitioned_data_sink("query-7");
        let fragment = test_fragment(sink.clone());
        let exec_params = test_exec_params();
        let lowered = empty_lowered();
        let mut profiles = BTreeMap::new();
        let mut resolver_calls = 0;
        let destinations = exec_params
            .destinations
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                decode_fragment_destination(
                    destination,
                    FieldPath::root("exec_plan_fragment")
                        .field("params")
                        .field("destinations")
                        .index(index),
                )
                .expect("test destination")
            })
            .collect::<Vec<_>>();

        let draft = StarRocksExternalDependencyDraft::new(None, profiles.clone());
        let first = decode_fragment_sink(
            &sink,
            &fragment,
            &destinations,
            exec_params.sender_id,
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink"),
            FieldPath::root("exec_plan_fragment").field("fragment"),
        );
        let first =
            classify_fragment_decode_attempt(first.map_err(|error| error.to_string()), &draft);
        let retry = process_fragment_decode_attempt(first, |dependency| {
            resolver_calls += 1;
            let StarRocksExternalDependency::QueryProfile { query_id, .. } = dependency else {
                return Err("unexpected dependency".to_string());
            };
            profiles.insert(query_id.clone(), "resolved-profile".to_string());
            Ok(true)
        })
        .expect("dependency resolution must succeed");
        assert!(retry.is_none());

        let draft = StarRocksExternalDependencyDraft::new(None, profiles);
        let second = decode_fragment_sink(
            &sink,
            &fragment,
            &destinations,
            exec_params.sender_id,
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink"),
            FieldPath::root("exec_plan_fragment").field("fragment"),
        );
        let second =
            classify_fragment_decode_attempt(second.map_err(|error| error.to_string()), &draft);
        let ready = process_fragment_decode_attempt(second, |_| {
            resolver_calls += 1;
            Ok(true)
        })
        .expect("resolved sink decode must succeed");

        assert!(ready.is_some_and(|decoded| matches!(
            decoded.spec.program(),
            FragmentSinkProgram::DataStream(_)
        )));
        assert_eq!(resolver_calls, 1);
    }

    #[test]
    fn schema_table_sink_decodes_to_unified_noop_contract() {
        let sink = data_sink_with_split(data_sinks::TDataSinkType::SCHEMA_TABLE_SINK, None);
        let fragment = test_fragment(sink.clone());
        let lowered = empty_lowered();
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());

        let decoded = decode_fragment_sink(
            &sink,
            &fragment,
            &[],
            None,
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink"),
            FieldPath::root("exec_plan_fragment").field("fragment"),
        )
        .expect("schema table sink");

        assert!(matches!(decoded.spec.program(), FragmentSinkProgram::Noop));
        assert!(matches!(decoded.assignment, FragmentSinkAssignment::None));
    }

    #[test]
    fn split_sink_decode_separates_static_branches_from_destination_assignment() {
        let split = data_sinks::TSplitDataStreamSink::new(
            vec![unpartitioned_stream_sink()],
            vec![Vec::new()],
            vec![bool_literal_expr(true)],
        );
        let sink = data_sink_with_split(
            data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK,
            Some(split),
        );
        let fragment = test_fragment(sink.clone());
        let exec_params = test_exec_params();
        let lowered = empty_lowered();
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let destinations = exec_params
            .destinations
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                decode_fragment_destination(
                    destination,
                    FieldPath::root("exec_plan_fragment")
                        .field("params")
                        .field("destinations")
                        .index(index),
                )
                .expect("test destination")
            })
            .collect::<Vec<_>>();

        let decoded = decode_fragment_sink(
            &sink,
            &fragment,
            &destinations,
            exec_params.sender_id,
            None,
            &mut ExprArena::default(),
            &lowered,
            None,
            None,
            &draft,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink"),
            FieldPath::root("exec_plan_fragment").field("fragment"),
        )
        .expect("split sink");

        let FragmentSinkProgram::SplitDataStream(program) = decoded.spec.program() else {
            panic!("split sink must decode to the static split program");
        };
        assert_eq!(program.sinks().len(), 1);
        assert_eq!(program.split_exprs().len(), 1);
        let FragmentSinkAssignment::DestinationGroups { groups, sender_id } = decoded.assignment
        else {
            panic!("split sink must bind destinations in the instance assignment");
        };
        assert_eq!(groups.len(), 1);
        assert_eq!(sender_id, Some(0));
    }

    #[test]
    fn split_nested_destination_reports_fragment_branch_path() {
        let stream = unpartitioned_stream_sink();
        let split = data_sinks::TSplitDataStreamSink::new(
            vec![stream.clone(), stream],
            vec![vec![], vec![destination_without_endpoint()]],
            vec![bool_literal_expr(true), bool_literal_expr(false)],
        );
        let sink = data_sink_with_split(
            data_sinks::TDataSinkType::SPLIT_DATA_STREAM_SINK,
            Some(split),
        );
        let fragment = test_fragment(sink.clone());
        let draft = StarRocksExternalDependencyDraft::new(None, BTreeMap::new());
        let error = match decode_fragment_sink(
            &sink,
            &fragment,
            &[],
            None,
            None,
            &mut ExprArena::default(),
            &empty_lowered(),
            None,
            None,
            &draft,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink"),
            FieldPath::root("exec_plan_fragment").field("fragment"),
        ) {
            Ok(_) => panic!("nested split destination without endpoint must fail"),
            Err(error) => error,
        };

        assert_eq!(
            error.to_string(),
            "starrocks protocol error at exec_plan_fragment.fragment.output_sink.split_stream_sink.destinations[1][0].brpc_server (missing field): destination requires brpc_server or deprecated_server"
        );
    }

    #[test]
    fn multicast_nested_destination_reports_fragment_branch_path() {
        let stream = unpartitioned_stream_sink();
        let multi_cast = data_sinks::TMultiCastDataStreamSink::new(
            vec![stream.clone(), stream],
            vec![vec![], vec![destination_without_endpoint()]],
        );
        let error = multi_cast_inputs_from_compat(
            &multi_cast,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink")
                .field("multi_cast_stream_sink"),
            &mut ExprArena::default(),
            &empty_layout(),
            None,
            None,
        )
        .err()
        .expect("nested destination without an endpoint must be rejected");

        assert_eq!(
            error.to_string(),
            "starrocks protocol error at exec_plan_fragment.fragment.output_sink.multi_cast_stream_sink.destinations[1][0].brpc_server (missing field): destination requires brpc_server or deprecated_server"
        );
    }

    #[test]
    fn router_branch_destination_reports_fragment_branch_path() {
        let stream = unpartitioned_stream_sink();
        let router = data_sinks::TIcebergChangeStreamRouterSink::new(
            23,
            None,
            vec![
                data_sinks::TIcebergChangeStreamRouterBranch::new(
                    0,
                    data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV,
                    stream.clone(),
                    vec![],
                ),
                data_sinks::TIcebergChangeStreamRouterBranch::new(
                    1,
                    data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA,
                    stream,
                    vec![destination_without_endpoint()],
                ),
            ],
        );
        let error = iceberg_router_input_from_compat(
            &router,
            FieldPath::root("exec_plan_fragment")
                .field("fragment")
                .field("output_sink")
                .field("iceberg_change_stream_router_sink"),
            &mut ExprArena::default(),
            &empty_layout(),
            None,
            None,
        )
        .err()
        .expect("router destination without an endpoint must be rejected");

        assert_eq!(
            error.to_string(),
            "starrocks protocol error at exec_plan_fragment.fragment.output_sink.iceberg_change_stream_router_sink.branches[1].destinations[0].brpc_server (missing field): destination requires brpc_server or deprecated_server"
        );
    }
}
