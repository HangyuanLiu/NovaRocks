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

use crate::common::config::debug_exec_node_output;
use crate::common::types::UniqueId;
use crate::exec::expr::ExprArena;
use crate::exec::node::ExecPlan;
use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
use crate::lower::common::fragment_runtime::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::protocol::native::decode::{
    self, DecodedApplyPoint, NativePlanDecodeContext, NativeRuntimeFilterDecodeLedger,
    NativeRuntimeFilterDormancyFact, NativeRuntimeFilterDormancyRole,
    decode_node_with_runtime_filters,
};
use crate::runtime::fragment::instance::FragmentInstanceId;
use crate::runtime::fragment_output::FragmentOutput;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::{
    NATIVE_RUNTIME_FILTER_BINDING_COUNT, NATIVE_RUNTIME_FILTER_DEPLOYMENT_NOT_INSTALLED,
    NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE, NATIVE_RUNTIME_FILTER_VALIDATED_LOOKUP,
    NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS, Profiler,
};
use crate::runtime::query_context::QueryId;
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
    let mut runtime_filter_bindings = NativeRuntimeFilterDecodeLedger::decode(
        fragment.fragment_id,
        fragment.runtime_filter_bindings.as_ref(),
    )?;
    let query_options = instance_params
        .query_options
        .as_ref()
        .map(decode::decode_query_options)
        .transpose()
        .map_err(|error| error.to_string())?;
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
    if instance_params.runtime_filter_params.is_some() {
        return Err(
            "native InstanceParams must not carry legacy runtime-filter params".to_string(),
        );
    }
    let result_buffer_tracker = mem_tracker.clone();
    let runtime_state = build_runtime_state(
        RuntimeStateInputs {
            query_options: query_options.clone(),
            query_id: Some(query_id),
            runtime_filter_params: None,
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

    let ctx = NativePlanDecodeContext::from_native(
        root,
        instance_params,
        query_options.clone(),
        Arc::new(connector::ConnectorRegistry::default()),
        query_id,
        FragmentInstanceId::new(fragment_instance_id),
    )?;
    let (lowered, dormancy_facts) = {
        let _lower_timer = profiler.as_ref().map(|p| p.scoped_timer("LowerPlanTime"));
        let lowered =
            decode_node_with_runtime_filters(root, &mut arena, &ctx, &mut runtime_filter_bindings)?;
        let dormancy_facts = runtime_filter_bindings.finish()?;
        (lowered, dormancy_facts)
    };
    if let Some(profiler) = profiler.as_ref() {
        record_native_runtime_filter_dormancy(profiler, &dormancy_facts);
    }

    let exec_plan = ExecPlan {
        arena,
        root: lowered.node,
    };

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
    execute_native_plan_with_pipeline(
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

fn record_native_runtime_filter_dormancy(
    profiler: &Profiler,
    facts: &[NativeRuntimeFilterDormancyFact],
) {
    let dormancy = profiler.child(NATIVE_RUNTIME_FILTER_DORMANCY_PROFILE);
    dormancy.counter_set_unit(
        NATIVE_RUNTIME_FILTER_BINDING_COUNT,
        i64::try_from(facts.len()).expect("runtime-filter binding count fits i64"),
    );
    for fact in facts {
        let binding = dormancy.child(format!("Binding{}", fact.binding_id));
        binding.add_info_string("BindingId", fact.binding_id.to_string());
        binding.add_info_string("ChannelId", fact.channel_id.to_string());
        binding.add_info_string("NodeId", fact.node_id.to_string());
        binding.add_info_string(
            "ApplyPoint",
            match fact.apply_point {
                DecodedApplyPoint::NodeInput => "NodeInput",
                DecodedApplyPoint::NodeOutput => "NodeOutput",
            },
        );
        binding.add_info_string(
            "Role",
            match fact.role {
                NativeRuntimeFilterDormancyRole::Producer => "Producer",
                NativeRuntimeFilterDormancyRole::Consumer => "Consumer",
            },
        );
        binding.counter_set_unit(NATIVE_RUNTIME_FILTER_VALIDATED_LOOKUP, 1);
        binding.counter_set_unit(NATIVE_RUNTIME_FILTER_DEPLOYMENT_NOT_INSTALLED, 1);
        for counter in NATIVE_RUNTIME_FILTER_ZERO_SIDE_EFFECT_COUNTERS {
            binding.counter_set_unit(counter, 0);
        }
    }
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
            runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                fragment_id: 1,
                bindings: Vec::new(),
            }),
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
        let layout = decode::Layout::default();

        let factory = sink_factory_from_native(fragment, sink, params, false, &layout);

        assert!(
            factory.is_ok(),
            "native {label} was rejected: {}",
            factory.err().unwrap_or_else(|| "unknown error".to_string())
        );
    }

    #[test]
    fn converts_native_query_options_consumed_subset() {
        let opts = decode::decode_query_options(&proto::novarocks::QueryOptions {
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
        let err = decode::decode_query_options(&proto::novarocks::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert!(err.to_string().contains("spill_options"), "{err}");
    }

    #[test]
    fn converts_runtime_filter_params_and_addresses() {
        let rf = decode::decode_runtime_filter_params(&proto::novarocks::RuntimeFilterParams {
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
        })
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

    #[test]
    fn native_fragment_emits_zero_binding_dormancy_profile() {
        let fragment = noop_values_fragment();
        let params = instance_params();
        let profiler = Profiler::new("native fragment");

        execute_fragment_native(
            &fragment,
            &params,
            None,
            1,
            None,
            Some(profiler.clone()),
            None,
        )
        .expect("native noop fragment executes");

        let dormancy = profiler
            .get_child("NativeRuntimeFilterDormancy")
            .expect("zero-binding fragment still emits structured dormancy profile");
        assert_eq!(dormancy.counter_value("BindingCount"), Some(0));
        assert!(dormancy.children().is_empty());
    }

    #[test]
    fn dormancy_profile_records_every_binding_and_explicit_zero_side_effects() {
        use crate::protocol::native::decode::{
            DecodedApplyPoint, NativeRuntimeFilterDormancyFact, NativeRuntimeFilterDormancyRole,
        };

        let profiler = Profiler::new("native fragment");
        record_native_runtime_filter_dormancy(
            &profiler,
            &[
                NativeRuntimeFilterDormancyFact {
                    binding_id: 1,
                    channel_id: 9,
                    node_id: 11,
                    apply_point: DecodedApplyPoint::NodeInput,
                    role: NativeRuntimeFilterDormancyRole::Consumer,
                },
                NativeRuntimeFilterDormancyFact {
                    binding_id: 2,
                    channel_id: 9,
                    node_id: 12,
                    apply_point: DecodedApplyPoint::NodeOutput,
                    role: NativeRuntimeFilterDormancyRole::Producer,
                },
            ],
        );

        let dormancy = profiler
            .get_child("NativeRuntimeFilterDormancy")
            .expect("dormancy profile");
        assert_eq!(dormancy.counter_value("BindingCount"), Some(2));
        for (binding_id, role) in [(1, "Consumer"), (2, "Producer")] {
            let binding = dormancy
                .get_child(&format!("Binding{binding_id}"))
                .expect("binding profile");
            assert_eq!(
                binding.get_info_string("BindingId"),
                Some(binding_id.to_string())
            );
            assert_eq!(binding.get_info_string("Role").as_deref(), Some(role));
            assert_eq!(binding.counter_value("ValidatedLookup"), Some(1));
            assert_eq!(binding.counter_value("DeploymentNotInstalled"), Some(1));
            for counter in [
                "ArtifactBuild",
                "ArtifactPublish",
                "LegacyRegister",
                "DependencyWait",
                "SnapshotPoll",
                "Apply",
            ] {
                assert_eq!(
                    binding.counter_value(counter),
                    Some(0),
                    "{counter} must be explicit zero for binding {binding_id}"
                );
            }
        }
    }
}
