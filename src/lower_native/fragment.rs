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

//! Proto fragment lowering.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use thrift::OrderedFloat;

use super::node::{NodeLoweringContext, lower_proto_node};
use crate::cache::CacheOptions;
use crate::common::config::{
    debug_exec_node_output, runtime_filter_scan_wait_time_ms_override,
    runtime_filter_wait_timeout_ms_override,
};
use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{NoopSinkFactory, ResultBufferSinkFactory};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::exec::spill::QuerySpillManager;
use crate::lower::fragment::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;
use crate::runtime::result_buffer;
use crate::runtime::runtime_state::RuntimeState;
use crate::thrift::{data_sinks, internal_service, runtime_filter, types};
use crate::{connector, proto};

pub(crate) fn execute_fragment_native(
    fragment: &proto::plan::PlanFragment,
    instance_params: &proto::novarocks::InstanceParams,
    session_time_zone: Option<&str>,
    pipeline_dop: i32,
    _db_name: Option<&str>,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
) -> Result<FragmentOutput, String> {
    let query_options = instance_params
        .query_options
        .as_ref()
        .map(query_options_from_native)
        .transpose()?;
    let query_options = apply_query_option_overrides(query_options);
    let query_id = instance_params
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())
        .map(query_id_from_native)?;
    let fragment_instance_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(unique_id_from_native)?;
    let runtime_filter_params = instance_params
        .runtime_filter_params
        .as_ref()
        .map(runtime_filter_params_from_native)
        .transpose()?;
    let cache_options = CacheOptions::from_query_options(query_options.as_ref())?;
    let spill_config = crate::exec::spill::query_options_wire::spill_config_from_query_options(
        query_options.as_ref(),
    )?;
    let spill_manager = spill_config
        .as_ref()
        .map(|config| Arc::new(QuerySpillManager::new(config.clone(), profiler.as_ref())));
    let result_buffer_tracker = mem_tracker.clone();
    let runtime_state = Arc::new(RuntimeState::new(
        query_options.clone(),
        Some(cache_options),
        Some(query_id),
        runtime_filter_params,
        Some(fragment_instance_id),
        Some(instance_params.backend_num),
        mem_tracker,
        spill_config,
        spill_manager,
    ));

    let root = fragment
        .root
        .as_ref()
        .ok_or_else(|| "native PlanFragment missing root".to_string())?;
    let sink = fragment
        .sink
        .as_ref()
        .ok_or_else(|| "native PlanFragment missing sink".to_string())?;

    let mut arena = ExprArena::default();
    let allow_throw_exception = query_options
        .as_ref()
        .and_then(|opts| opts.allow_throw_exception)
        .unwrap_or(false);
    arena.set_allow_throw_exception(allow_throw_exception);
    arena.set_session_time_zone(session_time_zone.map(str::to_string));

    let ctx = node_context_from_instance_params(
        instance_params,
        query_options.clone(),
        fragment_instance_id,
    )?;
    let lowered = {
        let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
        lower_proto_node(root, &mut arena, &ctx)?
    };

    let mut exec_plan = ExecPlan {
        arena,
        root: lowered.node,
    };
    push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

    prepare_result_buffer_for_native_sink(
        sink,
        fragment_instance_id,
        instance_params.typed_result_sink,
        result_buffer_tracker.as_ref(),
    )?;
    let exchange_finst_id = Some((fragment_instance_id.hi, fragment_instance_id.lo));
    let sink_factory = sink_factory_from_native(fragment, sink, instance_params.typed_result_sink)?;
    let _exec_timer = profiler
        .as_ref()
        .map(|p| p.scoped_timer("PipelineExecuteTime"));
    execute_plan_with_pipeline(
        exec_plan,
        debug_exec_node_output(),
        Duration::from_millis(50),
        sink_factory,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        Some(query_id),
        None,
        Some(instance_params.backend_num),
    )?;

    Ok(FragmentOutput { profile_json: None })
}

fn unique_id_from_native(src: &proto::common::UniqueId) -> UniqueId {
    UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn query_id_from_native(src: &proto::common::UniqueId) -> QueryId {
    QueryId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn query_options_from_native(
    src: &proto::novarocks::QueryOptions,
) -> Result<internal_service::TQueryOptions, String> {
    let mut opts = internal_service::TQueryOptions::default();
    opts.batch_size = (src.batch_size > 0).then_some(src.batch_size);
    opts.mem_limit = (src.mem_limit > 0).then_some(src.mem_limit);
    opts.query_timeout = (src.query_timeout > 0).then_some(src.query_timeout);
    opts.enable_profile = Some(src.enable_profile);
    opts.pipeline_dop = (src.pipeline_dop > 0).then_some(src.pipeline_dop);
    opts.query_mem_limit = (src.query_mem_limit > 0).then_some(src.query_mem_limit);
    opts.connector_io_tasks_per_scan_operator = (src.connector_io_tasks_per_scan_operator > 0)
        .then_some(src.connector_io_tasks_per_scan_operator);
    opts.io_tasks_per_scan_operator = opts.connector_io_tasks_per_scan_operator;
    opts.runtime_filter_scan_wait_time_ms =
        (src.runtime_filter_scan_wait_time_ms > 0).then_some(src.runtime_filter_scan_wait_time_ms);
    opts.runtime_filter_wait_timeout_ms =
        (src.runtime_filter_wait_timeout_ms > 0).then_some(src.runtime_filter_wait_timeout_ms);
    opts.allow_throw_exception = Some(src.allow_throw_exception);
    opts.group_concat_max_len = (src.group_concat_max_len > 0).then_some(src.group_concat_max_len);
    opts.enable_spill = Some(src.enable_spill);
    opts.spill_options = src.spill_options.as_ref().map(spill_options_from_native);
    if let Some(spill_options) = opts.spill_options.as_ref() {
        opts.spill_mode = spill_options.spill_mode;
        opts.spill_mem_table_size = spill_options.spill_mem_table_size;
        opts.spill_mem_table_num = spill_options.spill_mem_table_num;
        opts.spill_mem_limit_threshold = spill_options.spill_mem_limit_threshold;
        opts.spill_operator_min_bytes = spill_options.spill_operator_min_bytes;
        opts.spill_operator_max_bytes = spill_options.spill_operator_max_bytes;
        opts.spill_encode_level = spill_options.spill_encode_level;
    } else if src.enable_spill {
        return Err("native QueryOptions enable_spill=true requires spill_options".to_string());
    }
    Ok(opts)
}

fn spill_options_from_native(
    src: &proto::novarocks::SpillOptions,
) -> internal_service::TSpillOptions {
    let mut opts = internal_service::TSpillOptions::default();
    opts.spill_mode = (src.spill_mode != 0).then_some(src.spill_mode.into());
    opts.spill_mem_limit_threshold = (src.spill_mem_limit_threshold > 0.0)
        .then_some(OrderedFloat(src.spill_mem_limit_threshold));
    opts.spill_operator_min_bytes =
        (src.spill_operator_min_bytes > 0).then_some(src.spill_operator_min_bytes);
    opts.spill_operator_max_bytes =
        (src.spill_operator_max_bytes > 0).then_some(src.spill_operator_max_bytes);
    opts.spill_encode_level = (src.spill_encode_level > 0).then_some(src.spill_encode_level);
    opts.enable_spill_buffer_read = Some(src.enable_spill_buffer_read);
    opts.max_spill_read_buffer_bytes_per_driver = (src.max_spill_read_buffer_bytes_per_driver > 0)
        .then_some(src.max_spill_read_buffer_bytes_per_driver);
    opts.spill_mem_table_size = (src.spill_mem_table_size > 0).then_some(src.spill_mem_table_size);
    opts.spill_mem_table_num = (src.spill_mem_table_num > 0).then_some(src.spill_mem_table_num);
    opts
}

fn apply_query_option_overrides(
    mut query_options: Option<internal_service::TQueryOptions>,
) -> Option<internal_service::TQueryOptions> {
    if let Some(opts) = query_options.as_mut() {
        if let Some(ms) = runtime_filter_scan_wait_time_ms_override() {
            opts.runtime_filter_scan_wait_time_ms = Some(ms);
        }
        if let Some(ms) = runtime_filter_wait_timeout_ms_override() {
            opts.runtime_filter_wait_timeout_ms = Some(i32::try_from(ms).unwrap_or(i32::MAX));
        }
    }
    query_options
}

fn runtime_filter_params_from_native(
    src: &proto::novarocks::RuntimeFilterParams,
) -> Result<runtime_filter::TRuntimeFilterParams, String> {
    let id_to_prober_params = src
        .id_to_prober_params
        .iter()
        .map(|(filter_id, list)| {
            let params = list
                .params
                .iter()
                .map(prober_params_from_native)
                .collect::<Result<Vec<_>, _>>()?;
            Ok((*filter_id, params))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let runtime_filter_builder_number = src
        .runtime_filter_builder_number
        .iter()
        .map(|(filter_id, count)| (*filter_id, *count))
        .collect::<BTreeMap<_, _>>();

    Ok(runtime_filter::TRuntimeFilterParams::new(
        (!id_to_prober_params.is_empty()).then_some(id_to_prober_params),
        (!runtime_filter_builder_number.is_empty()).then_some(runtime_filter_builder_number),
        (src.runtime_filter_max_size > 0).then_some(src.runtime_filter_max_size),
        None,
    ))
}

fn prober_params_from_native(
    src: &proto::novarocks::ProberParams,
) -> Result<runtime_filter::TRuntimeFilterProberParams, String> {
    let fragment_instance_id = src
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native ProberParams missing fragment_instance_id".to_string())?;
    let fragment_instance_address = network_address_from_native(&src.fragment_instance_address)?;
    Ok(runtime_filter::TRuntimeFilterProberParams::new(
        types::TUniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo),
        fragment_instance_address,
    ))
}

fn network_address_from_native(src: &str) -> Result<types::TNetworkAddress, String> {
    let (host, port) = src
        .rsplit_once(':')
        .ok_or_else(|| format!("native network address must be host:port, got '{src}'"))?;
    if host.is_empty() {
        return Err(format!("native network address has empty host: '{src}'"));
    }
    let port = port
        .parse::<i32>()
        .map_err(|e| format!("native network address has invalid port '{src}': {e}"))?;
    Ok(types::TNetworkAddress::new(host.to_string(), port))
}

fn node_context_from_instance_params(
    instance_params: &proto::novarocks::InstanceParams,
    query_options: Option<internal_service::TQueryOptions>,
    fragment_instance_id: UniqueId,
) -> Result<NodeLoweringContext, String> {
    let mut ctx = NodeLoweringContext::default()
        .with_connector_registry(Arc::new(connector::ConnectorRegistry::default()))
        .with_query_options(query_options)
        .with_fragment_instance_id(fragment_instance_id.hi, fragment_instance_id.lo);
    for (node_id, ranges) in &instance_params.per_node_scan_ranges {
        ctx = ctx.with_scan_ranges(*node_id, ranges.ranges.clone());
    }
    for (node_id, sender_count) in &instance_params.per_exch_num_senders {
        if *sender_count <= 0 {
            return Err(format!(
                "native InstanceParams per_exch_num_senders node_id={} must be positive, got {}",
                node_id, sender_count
            ));
        }
        ctx = ctx.with_exchange_sender_count(
            crate::runtime::exchange::ExchangeKey {
                finst_id_hi: fragment_instance_id.hi,
                finst_id_lo: fragment_instance_id.lo,
                node_id: *node_id,
            },
            usize::try_from(*sender_count).map_err(|_| {
                format!(
                    "native InstanceParams per_exch_num_senders node_id={} cannot convert {} to usize",
                    node_id, sender_count
                )
            })?,
        );
    }
    Ok(ctx)
}

fn prepare_result_buffer_for_native_sink(
    sink: &proto::plan::DataSink,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) -> Result<(), String> {
    let uses_fetch_result_buffer = matches!(
        sink.kind.as_ref(),
        Some(proto::plan::data_sink::Kind::Result(true))
    );
    if !uses_fetch_result_buffer {
        return Ok(());
    }
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let label = format!("ResultBuffer: finst={}", finst_id);
        let tracker = MemTracker::new_child(label, root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
    Ok(())
}

fn sink_factory_from_native(
    fragment: &proto::plan::PlanFragment,
    sink: &proto::plan::DataSink,
    typed_result_sink: bool,
) -> Result<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>, String> {
    let kind = sink
        .kind
        .as_ref()
        .ok_or_else(|| "native PlanFragment sink kind missing".to_string())?;
    match kind {
        proto::plan::data_sink::Kind::Result(true) => {
            if !fragment.output_exprs.is_empty() {
                return Err(
                    "native RESULT sink does not support fragment output_exprs yet".to_string(),
                );
            }
            Ok(Box::new(ResultBufferSinkFactory::new(
                None,
                Some(data_sinks::TResultSinkType::MYSQL_PROTOCAL),
                None,
                None,
                typed_result_sink,
            )))
        }
        proto::plan::data_sink::Kind::Noop(true) => Ok(Box::new(NoopSinkFactory::new())),
        proto::plan::data_sink::Kind::Result(false) => {
            Err("native RESULT sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::Noop(false) => {
            Err("native NOOP sink marker must be true".to_string())
        }
        proto::plan::data_sink::Kind::IcebergWrite(_) => {
            Err("native Iceberg write sink lowering is not implemented yet".to_string())
        }
        proto::plan::data_sink::Kind::IcebergChangeStreamRouter(_) => Err(
            "native Iceberg change-stream router sink lowering is not implemented yet".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{common, plan};

    fn int_output_column(id: u32, name: &str) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: name.to_string(),
            r#type: Some(common::TypeDesc {
                kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                    r#type: common::PrimitiveType::Int as i32,
                    ..Default::default()
                })),
            }),
            nullable: false,
            is_internal: false,
        }
    }

    fn noop_values_fragment() -> plan::PlanFragment {
        let columns = vec![int_output_column(1, "v")];
        plan::PlanFragment {
            fragment_id: 1,
            root: Some(plan::DistributedNode {
                node_id: 10,
                fragment_id: 1,
                limit: -1,
                payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                    output_columns: columns.clone(),
                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                        rows: Vec::new(),
                        columns: columns.clone(),
                    })),
                })),
                ..Default::default()
            }),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::Noop(true)),
            }),
            output_columns: columns,
            ..Default::default()
        }
    }

    fn instance_params() -> proto::novarocks::InstanceParams {
        proto::novarocks::InstanceParams {
            query_id: Some(common::UniqueId { hi: 11, lo: 12 }),
            fragment_instance_id: Some(common::UniqueId { hi: 21, lo: 22 }),
            backend_num: 1,
            query_options: Some(proto::novarocks::QueryOptions {
                batch_size: 1024,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn converts_native_query_options_consumed_subset() {
        let opts = query_options_from_native(&proto::novarocks::QueryOptions {
            batch_size: 8192,
            enable_profile: true,
            connector_io_tasks_per_scan_operator: 7,
            runtime_filter_wait_timeout_ms: 123,
            allow_throw_exception: true,
            enable_spill: true,
            spill_options: Some(proto::novarocks::SpillOptions {
                spill_mode: internal_service::TSpillMode::FORCE.0,
                spill_mem_limit_threshold: 0.5,
                spill_operator_min_bytes: 1024,
                spill_mem_table_size: 32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("query options");

        assert_eq!(opts.batch_size, Some(8192));
        assert_eq!(opts.enable_profile, Some(true));
        assert_eq!(opts.io_tasks_per_scan_operator, Some(7));
        assert_eq!(opts.connector_io_tasks_per_scan_operator, Some(7));
        assert_eq!(opts.runtime_filter_wait_timeout_ms, Some(123));
        assert_eq!(opts.allow_throw_exception, Some(true));
        assert_eq!(opts.enable_spill, Some(true));
        let spill = opts.spill_options.expect("spill options");
        assert_eq!(spill.spill_mode, Some(internal_service::TSpillMode::FORCE));
        assert_eq!(spill.spill_mem_limit_threshold, Some(OrderedFloat(0.5)));
        assert_eq!(spill.spill_operator_min_bytes, Some(1024));
        assert_eq!(spill.spill_mem_table_size, Some(32));
    }

    #[test]
    fn rejects_native_spill_without_spill_options() {
        let err = query_options_from_native(&proto::novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert!(err.contains("spill_options"), "{err}");
    }

    #[test]
    fn converts_runtime_filter_params_and_addresses() {
        let rf = runtime_filter_params_from_native(&proto::novarocks::RuntimeFilterParams {
            id_to_prober_params: [(
                3,
                proto::novarocks::ProberParamsList {
                    params: vec![proto::novarocks::ProberParams {
                        fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                        fragment_instance_address: "127.0.0.1:9050".to_string(),
                    }],
                },
            )]
            .into_iter()
            .collect(),
            runtime_filter_builder_number: [(3, 2)].into_iter().collect(),
            runtime_filter_max_size: 4096,
        })
        .expect("runtime filter params");

        assert_eq!(rf.runtime_filter_max_size, Some(4096));
        assert_eq!(rf.runtime_filter_builder_number.unwrap().get(&3), Some(&2));
        let prober = &rf.id_to_prober_params.unwrap()[&3][0];
        assert_eq!(
            prober.fragment_instance_id,
            Some(types::TUniqueId::new(1, 2))
        );
        assert_eq!(
            prober.fragment_instance_address,
            Some(types::TNetworkAddress::new("127.0.0.1".to_string(), 9050))
        );
    }

    #[test]
    fn rejects_native_fragment_without_query_id() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.query_id = None;

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("query_id is required");
        assert!(err.contains("query_id"), "{err}");
    }

    #[test]
    fn rejects_native_fragment_without_fragment_instance_id() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.fragment_instance_id = None;

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("fragment_instance_id is required");
        assert!(err.contains("fragment_instance_id"), "{err}");
    }

    #[test]
    fn rejects_nonpositive_exchange_sender_count() {
        let fragment = noop_values_fragment();
        let mut params = instance_params();
        params.per_exch_num_senders.insert(30, 0);

        let err = execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect_err("sender count must be positive");
        assert!(err.contains("must be positive"), "{err}");
    }

    #[test]
    fn executes_native_noop_values_fragment() {
        let fragment = noop_values_fragment();
        let params = instance_params();

        execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect("native noop fragment executes");
    }
}
