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

mod sink_factory;

use std::sync::Arc;
use std::time::Duration;

use sink_factory::{prepare_result_buffer_for_native_sink, sink_factory_from_native};

use super::expr::lower_proto_expr;
use super::node::{NodeLoweringContext, lower_proto_node};
use crate::common::config::debug_exec_node_output;
use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::DataStreamSinkFactoryInput;
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::lower::common::fragment_runtime::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::native_fragment_wire as native_wire;
use crate::runtime::profile::Profiler;
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
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
        .map(native_wire::query_options_from_native)
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
        .map(native_wire::runtime_filter_params_from_native)
        .transpose()?;
    let result_buffer_tracker = mem_tracker.clone();
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: query_options.clone(),
            query_id: Some(query_id),
            runtime_filter_params,
            fragment_instance_id: Some(fragment_instance_id),
            backend_num: Some(instance_params.backend_num),
            mem_tracker,
        },
        profiler.as_ref(),
    )?;

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
        .map(|opts| opts.allow_throw_exception)
        .unwrap_or(false);
    arena.set_allow_throw_exception(allow_throw_exception);
    arena.set_session_time_zone(session_time_zone.map(str::to_string));

    let ctx = node_context_from_instance_params(
        instance_params,
        query_options.clone(),
        fragment_instance_id,
    )?
    .with_query_id(UniqueId {
        hi: query_id.hi,
        lo: query_id.lo,
    });
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
    let sink_factory = sink_factory_from_native(
        fragment,
        sink,
        instance_params,
        instance_params.typed_result_sink,
        &lowered.layout,
    )?;
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

fn stream_destination_from_native(
    src: &proto::plan::StreamDestination,
) -> Result<crate::runtime::endpoint::FragmentDestination, String> {
    let finst_id = src
        .finst_id
        .as_ref()
        .ok_or_else(|| "native StreamDestination missing finst_id".to_string())?;
    Ok(crate::runtime::endpoint::FragmentDestination::new(
        unique_id_from_native(finst_id),
        crate::runtime::endpoint::RuntimeEndpoint::parse(&src.endpoint)?,
    ))
}

fn stream_destinations_from_native(
    src: &proto::plan::StreamDestinationList,
) -> Result<Vec<crate::runtime::endpoint::FragmentDestination>, String> {
    src.destinations
        .iter()
        .map(stream_destination_from_native)
        .collect()
}

fn fragment_instance_id_from_native_params(
    params: &proto::novarocks::InstanceParams,
) -> Result<UniqueId, String> {
    params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(unique_id_from_native)
}

fn data_stream_input_from_native(
    stream: &proto::plan::DataStreamSink,
    destinations: Vec<crate::runtime::endpoint::FragmentDestination>,
    partition_exprs: Vec<crate::exec::expr::ExprId>,
) -> Result<DataStreamSinkFactoryInput, String> {
    let partition = stream
        .output_partition
        .as_ref()
        .ok_or_else(|| "native DATA_STREAM_SINK missing output_partition".to_string())?;
    let partition_type =
        DataStreamSinkFactoryInput::partition_type_from_native_kind(partition.kind)?;
    DataStreamSinkFactoryInput::try_new(
        stream.dest_node_id,
        partition_type,
        Vec::new(),
        partition_exprs,
        stream.output_columns.clone(),
        destinations,
    )
}

fn lower_stream_partition_exprs_from_native(
    partition: &proto::plan::DataPartition,
    partition_arena: &mut ExprArena,
    layout: &super::layout::Layout,
    context: impl Fn(usize) -> String,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    let partition_type =
        DataStreamSinkFactoryInput::partition_type_from_native_kind(partition.kind)?;
    if !DataStreamSinkFactoryInput::partition_type_requires_exprs(partition_type) {
        return Ok(Vec::new());
    }
    partition
        .exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            lower_proto_expr(expr, partition_arena, layout)
                .map_err(|err| format!("{}: {err}", context(idx)))
        })
        .collect()
}

fn node_context_from_instance_params(
    instance_params: &proto::novarocks::InstanceParams,
    query_options: Option<QueryOptions>,
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

    fn unpartitioned_partition() -> plan::DataPartition {
        plan::DataPartition {
            kind: plan::PartitionKind::Unpartitioned as i32,
            exprs: Vec::new(),
        }
    }

    fn random_partition_with_unlowerable_expr() -> plan::DataPartition {
        plan::DataPartition {
            kind: plan::PartitionKind::Random as i32,
            exprs: vec![crate::proto::expr::Expr::default()],
        }
    }

    fn data_stream_sink(dest_node_id: i32) -> plan::DataStreamSink {
        plan::DataStreamSink {
            dest_node_id,
            output_partition: Some(unpartitioned_partition()),
            output_columns: Vec::new(),
            limit: None,
        }
    }

    fn stream_destination_list() -> plan::StreamDestinationList {
        plan::StreamDestinationList {
            destinations: vec![plan::StreamDestination {
                finst_id: Some(common::UniqueId { hi: 31, lo: 32 }),
                endpoint: "127.0.0.1:9031".to_string(),
            }],
        }
    }

    fn with_sink(kind: plan::data_sink::Kind) -> plan::PlanFragment {
        let mut fragment = noop_values_fragment();
        fragment.sink = Some(plan::DataSink { kind: Some(kind) });
        fragment
    }

    fn data_stream_sink_fragment() -> plan::PlanFragment {
        with_sink(plan::data_sink::Kind::DataStream(data_stream_sink(30)))
    }

    fn multi_cast_data_stream_sink_fragment() -> plan::PlanFragment {
        with_sink(plan::data_sink::Kind::MultiCastDataStream(
            plan::MultiCastDataStreamSink {
                sinks: vec![data_stream_sink(30)],
                destinations: vec![stream_destination_list()],
            },
        ))
    }

    fn change_stream_router_sink_fragment() -> plan::PlanFragment {
        with_sink(plan::data_sink::Kind::IcebergChangeStreamRouter(
            plan::IcebergChangeStreamRouterSink {
                group_id: 7,
                change_op_output_ordinal: 0,
                data_route_output_ordinal: None,
                branches: vec![plan::IcebergChangeStreamBranchRoute {
                    branch_id: 0,
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    target_fragment_id: 2,
                    target_exchange_node_id: 30,
                    output_ordinals: vec![0],
                    output_partition_ordinals: Vec::new(),
                    output_partition: Some(unpartitioned_partition()),
                    destinations: Some(stream_destination_list()),
                }],
            },
        ))
    }

    fn instance_params_with_destination() -> proto::novarocks::InstanceParams {
        let mut params = instance_params();
        params.destinations.push(proto::novarocks::Destination {
            finst_id: Some(common::UniqueId { hi: 31, lo: 32 }),
            endpoint: "127.0.0.1:9031".to_string(),
        });
        params
    }

    fn assert_sink_factory_available(
        fragment: &plan::PlanFragment,
        params: &proto::novarocks::InstanceParams,
        label: &str,
    ) {
        let sink = fragment.sink.as_ref().expect("fragment sink");
        let layout = super::super::layout::Layout::default();

        let factory = sink_factory_from_native(fragment, sink, params, false, &layout);

        assert!(
            factory.is_ok(),
            "native {label} was rejected: {}",
            factory.err().unwrap_or_else(|| "unknown error".to_string())
        );
    }

    #[test]
    fn converts_native_query_options_consumed_subset() {
        let opts = native_wire::query_options_from_native(&proto::novarocks::QueryOptions {
            batch_size: 8192,
            enable_profile: true,
            query_mem_limit: 1 << 20,
            connector_io_tasks_per_scan_operator: 7,
            runtime_filter_wait_timeout_ms: Some(123),
            allow_throw_exception: true,
            enable_spill: true,
            spill_options: Some(proto::novarocks::SpillOptions {
                spill_mode: 1,
                spill_mem_limit_threshold: 0.5,
                spill_operator_min_bytes: 1024,
                spill_mem_table_size: 32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("query options");

        assert_eq!(opts.batch_size, Some(8192));
        assert!(opts.enable_profile);
        assert_eq!(opts.exec_mem_limit, Some(1 << 20));
        assert_eq!(opts.connector_io_tasks_per_scan_operator, Some(7));
        assert_eq!(opts.runtime_filter_wait_timeout_ms, Some(123));
        assert!(opts.allow_throw_exception);
        let spill = opts.spill.expect("spill options");
        assert_eq!(spill.spill_mode, crate::exec::spill::SpillMode::Force);
        assert_eq!(spill.spill_mem_limit_threshold, Some(0.5));
        assert_eq!(spill.spill_operator_min_bytes, Some(1024));
        assert_eq!(spill.spill_mem_table_size, Some(32));
    }

    #[test]
    fn rejects_native_spill_without_spill_options() {
        let err = native_wire::query_options_from_native(&proto::novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert!(err.contains("spill_options"), "{err}");
    }

    #[test]
    fn converts_runtime_filter_params_and_addresses() {
        let rf = native_wire::runtime_filter_params_from_native(
            &proto::novarocks::RuntimeFilterParams {
                id_to_prober_params: [(
                    3,
                    proto::novarocks::ProberParamsList {
                        params: vec![proto::novarocks::ProberParams {
                            fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                            endpoint: "127.0.0.1:9050".to_string(),
                        }],
                    },
                )]
                .into_iter()
                .collect(),
                runtime_filter_builder_number: [(3, 2)].into_iter().collect(),
                runtime_filter_max_size: 4096,
            },
        )
        .expect("runtime filter params");

        assert_eq!(rf.runtime_filter_max_size(), Some(4096));
        assert_eq!(rf.runtime_filter_builder_number().get(&3), Some(&2));
        let prober = &rf.id_to_prober_params()[&3][0];
        assert_eq!(
            prober.fragment_instance_id(),
            crate::common::types::UniqueId { hi: 1, lo: 2 }
        );
        assert_eq!(prober.endpoint().as_host_port(), "127.0.0.1:9050");
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
    fn native_data_stream_sink_factory_is_available_without_compat_projection_gate() {
        let fragment = data_stream_sink_fragment();
        let params = instance_params_with_destination();

        assert_sink_factory_available(&fragment, &params, "DATA_STREAM_SINK");
    }

    #[test]
    fn native_random_stream_partition_exprs_are_not_lowered() {
        let mut fragment = data_stream_sink_fragment();
        let sink = fragment.sink.as_mut().expect("fragment sink");
        let Some(plan::data_sink::Kind::DataStream(stream)) =
            sink.kind.as_mut().map(|kind| match kind {
                plan::data_sink::Kind::DataStream(stream) => {
                    plan::data_sink::Kind::DataStream(stream.clone())
                }
                other => other.clone(),
            })
        else {
            panic!("expected data stream sink");
        };
        let mut stream = stream;
        stream.output_partition = Some(random_partition_with_unlowerable_expr());
        sink.kind = Some(plan::data_sink::Kind::DataStream(stream));
        let params = instance_params_with_destination();

        assert_sink_factory_available(&fragment, &params, "RANDOM DATA_STREAM_SINK");
    }

    #[test]
    fn native_multi_cast_data_stream_sink_factory_is_available_without_compat_projection_gate() {
        let fragment = multi_cast_data_stream_sink_fragment();
        let params = instance_params();

        assert_sink_factory_available(&fragment, &params, "MULTI_CAST_DATA_STREAM_SINK");
    }

    #[test]
    fn native_change_stream_router_sink_factory_is_available_without_compat_projection_gate() {
        let fragment = change_stream_router_sink_fragment();
        let params = instance_params();

        assert_sink_factory_available(&fragment, &params, "ICEBERG_CHANGE_STREAM_ROUTER_SINK");
    }

    #[test]
    fn executes_native_noop_values_fragment() {
        let fragment = noop_values_fragment();
        let params = instance_params();

        execute_fragment_native(&fragment, &params, None, 1, None, None, None)
            .expect("native noop fragment executes");
    }
}
