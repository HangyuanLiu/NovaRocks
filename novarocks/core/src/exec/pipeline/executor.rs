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
//! Top-level pipeline executor entrypoint.
//!
//! Responsibilities:
//! - Builds runtime pipeline context and executes one plan fragment to completion.
//! - Bridges fragment context, driver executor, and terminal sink orchestration.
//!
//! Key exported interfaces:
//! - Functions: fixed native and compat pipeline execution entrypoints.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::time::Duration;

use crate::common::app_config;
use crate::exec::node::ExecPlan;
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::novarocks_logging::info;
use crate::runtime::query_context::query_context_manager;
use crate::runtime::runtime_state::RuntimeState;

#[cfg(feature = "compat")]
use super::builder::build_compat_pipeline_graph_for_exec_plan_with_root_sink_dop;
use super::builder::build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context;
use super::dependency::DependencyManager;
use super::fragment_context::FragmentContext;
use super::global_driver_executor::{DriverTask, FragmentCompletion, global_driver_executor};
use super::operator_factory::OperatorFactory;
use super::pipeline::Pipeline;
use crate::runtime::endpoint::RuntimeEndpoint;

use crate::runtime::profile::Profiler;

/// Execute one plan fragment through pipeline runtime and return the terminal sink outcome.
pub(crate) fn execute_native_plan_with_pipeline(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: std::sync::Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
) -> Result<(), String> {
    execute_native_plan_with_pipeline_with_root_sink_dop(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        None,
    )
}

pub(crate) fn execute_native_plan_with_pipeline_with_root_sink_dop(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: std::sync::Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
) -> Result<(), String> {
    execute_plan_with_pipeline_in_mode(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        root_sink_dop,
        PipelineExecutionMode::Native,
    )
}

#[cfg(feature = "compat")]
pub(crate) fn execute_compat_plan_with_pipeline(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
) -> Result<(), String> {
    execute_compat_plan_with_pipeline_with_root_sink_dop(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        None,
    )
}

#[cfg(feature = "compat")]
pub(crate) fn execute_compat_plan_with_pipeline_with_root_sink_dop(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
) -> Result<(), String> {
    execute_plan_with_pipeline_in_mode(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        root_sink_dop,
        PipelineExecutionMode::Compat,
    )
}

enum PipelineExecutionMode {
    Native,
    #[cfg(feature = "compat")]
    Compat,
}

#[allow(clippy::too_many_arguments)]
fn execute_plan_with_pipeline_in_mode(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    mode: PipelineExecutionMode,
) -> Result<(), String> {
    let fragment_profiler = profiler.clone();
    let dep_manager = DependencyManager::new();
    #[cfg(feature = "compat")]
    let runtime_filter_hub = match (&mode, query_id) {
        (PipelineExecutionMode::Native, _) => None,
        (PipelineExecutionMode::Compat, Some(qid)) => {
            if let Some(hub) = query_context_manager().get_runtime_filter_hub(qid)? {
                Some(hub)
            } else {
                let hub = Arc::new(
                    crate::runtime::runtime_filter_hub::RuntimeFilterHub::new_for_query(
                        DependencyManager::new(),
                        qid,
                    ),
                );
                query_context_manager().set_runtime_filter_hub(qid, Arc::clone(&hub))?;
                Some(hub)
            }
        }
        (PipelineExecutionMode::Compat, None) => Some(Arc::new(
            crate::runtime::runtime_filter_hub::RuntimeFilterHub::new(DependencyManager::new()),
        )),
    };
    #[cfg(feature = "compat")]
    if let Some(runtime_filter_hub) = runtime_filter_hub.as_ref() {
        runtime_filter_hub.set_wait_timeouts(
            runtime_state.runtime_filter_scan_wait_timeout(),
            runtime_state.runtime_filter_wait_timeout(),
        );
        if let Some(qid) = query_id {
            if let Some(params) = runtime_state.runtime_filter_params().cloned() {
                query_context_manager().set_runtime_filter_params(qid, params)?;
            }
            query_context_manager().get_or_create_runtime_filter_worker(qid)?;
        }
    }

    // Use the FE-calculated DOP as the base graph DOP. Some terminal sinks can
    // request a narrower root pipeline when their finalization state must be local.
    let graph = match mode {
        PipelineExecutionMode::Native => {
            build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context(
                &plan,
                debug,
                dep_manager.clone(),
                exchange_finst_id,
                exchange_bindings,
                scan_bindings,
                pipeline_dop,
                root_sink_dop,
                runtime_state.native_runtime_filter_context().cloned(),
            )?
        }
        #[cfg(feature = "compat")]
        PipelineExecutionMode::Compat => {
            build_compat_pipeline_graph_for_exec_plan_with_root_sink_dop(
                &plan,
                debug,
                dep_manager.clone(),
                exchange_finst_id,
                exchange_bindings,
                scan_bindings,
                pipeline_dop,
                root_sink_dop,
                runtime_filter_hub.expect("compat runtime-filter hub"),
            )?
        }
    };

    let finst_id = runtime_state.fragment_instance_id();
    let ctx = Arc::new(FragmentContext::new(
        profiler,
        Arc::clone(&runtime_state),
        exchange_finst_id,
        query_id,
        fe_addr,
        backend_num,
    ));
    let mut sink = Some(sink);

    // Collect all drivers
    let mut all_drivers = Vec::new();
    for pipeline_plan in graph.pipelines {
        let mut factories = pipeline_plan.factories;
        if pipeline_plan.id == graph.root_id {
            if !pipeline_plan.needs_sink {
                return Err("root pipeline missing sink requirement".to_string());
            }
            let root_sink = sink
                .take()
                .ok_or_else(|| "root pipeline sink already attached".to_string())?;
            factories.push(root_sink);
        } else if pipeline_plan.needs_sink {
            return Err("non-root pipeline requires sink".to_string());
        }

        let pipeline = Pipeline::new(pipeline_plan.id, factories, pipeline_plan.dop);
        let drivers = pipeline.instantiate_drivers(&ctx)?;
        all_drivers.extend(drivers);
    }

    if sink.is_some() {
        return Err("root pipeline sink not attached".to_string());
    }

    // Fixed time slice: 10ms (similar to StarRocks)
    const TIME_SLICE_MS: u64 = 10;
    let time_slice_fixed = Duration::from_millis(TIME_SLICE_MS);

    // Get executor thread count from config
    let num_threads = app_config::config()
        .ok()
        .map(|c| c.runtime.actual_exec_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

    // Use a shared global executor across fragments, following StarRocks' design.
    // When `num_threads <= 1`, keep the caller-provided time slice for backward compatibility.
    let effective_time_slice = if num_threads > 1 {
        info!(
            "Using global executor: threads={}, dop={}, time_slice={}ms",
            num_threads, pipeline_dop, TIME_SLICE_MS
        );
        time_slice_fixed
    } else {
        info!("Using global executor: threads=1, dop={}", pipeline_dop);
        time_slice
    };

    let completion = FragmentCompletion::new(all_drivers.len(), Arc::clone(&ctx));
    let completion_execution = if query_id.is_some()
        && let Some(finst_id) = finst_id
    {
        query_context_manager().register_fragment_completion(finst_id, Arc::clone(&completion))
    } else {
        None
    };
    if let Some(query_id) = query_id
        && query_context_manager().is_query_canceled(query_id)
    {
        completion.abort_from_query("query canceled".to_string());
    }
    let mut tasks = Vec::with_capacity(all_drivers.len());
    for driver in all_drivers {
        let task = DriverTask::new(driver, Arc::clone(&completion), effective_time_slice);
        tasks.push(task);
    }
    let _fragment_wall_timer = fragment_profiler
        .as_ref()
        .map(|p| p.scoped_timer("FragmentWallTime"));
    global_driver_executor().submit(tasks);
    let res = runtime_state
        .query_options()
        .and_then(|opts| opts.query_timeout)
        .filter(|secs| *secs > 0)
        .map(|secs| {
            let timeout = Duration::from_secs(secs as u64);
            completion.wait_timeout(timeout, format!("query timed out after {} ms", secs * 1000))
        })
        .unwrap_or_else(|| completion.wait());
    if let Some(finst_id) = finst_id {
        if let Some(execution) = completion_execution {
            query_context_manager().unregister_fragment_completion_execution(finst_id, execution);
        } else {
            query_context_manager().unregister_fragment_completion(finst_id);
        }
    }
    res?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
    use crate::exec::node::analytic::{
        AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
        WindowFunctionSpec, WindowType,
    };
    use crate::exec::node::join::{
        JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution, JoinType,
        NativeJoinRuntimeFilterProducerSpec,
    };
    use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
    use crate::exec::node::runtime_filter::{
        NativeRuntimeFilterConsumerNode, NativeRuntimeFilterConsumerSpec,
        NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, CompletionRequirement, ConsumerActivation, ContributionKind,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

    use super::execute_native_plan_with_pipeline;

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    #[test]
    fn dormant_native_filter_fails_open_for_local_shard_missing_key() {
        let query_id = QueryId { hi: 80_007, lo: 29 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let lifecycle = RuntimeFilterLifecycleRegistry::global();
        lifecycle.remove_query(query_key);
        let context_manager = query_context_manager();
        let deployment_lifecycle = RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(1),
            query_expire: Duration::from_secs(5),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(5),
            transport_max_pending_entries: 128,
            transport_max_pending_bytes: 1024 * 1024,
        };
        context_manager
            .ensure_native_context(
                query_id,
                false,
                deployment_lifecycle.delivery_expire,
                deployment_lifecycle.query_expire,
            )
            .expect("create native query context");
        context_manager
            .install_runtime_filter_deployment(
                query_id,
                deployment_lifecycle,
                crate::runtime::query_context::runtime_filter_service_lifecycle_tests::participant_install(),
            )
            .expect("install query-owned runtime-filter Service");
        for _ in 0..2 {
            context_manager
                .get_or_register_native(
                    query_id,
                    false,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("register shared NativeService query context");
        }
        let hub_error = match context_manager.get_runtime_filter_hub(query_id) {
            Err(error) => error,
            Ok(_) => panic!("NativeService context must reject legacy hub access"),
        };
        assert!(hub_error.contains("NativeService"), "{hub_error}");
        let worker_error = match context_manager.get_runtime_filter_worker(query_id) {
            Err(error) => error,
            Ok(_) => panic!("NativeService context must reject legacy worker access"),
        };
        assert!(worker_error.contains("NativeService"), "{worker_error}");
        let initial_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("query context installs a lifecycle event sink");
        let installed_filter_count = initial_lifecycle.filters.len();
        let installed_channel_event_count = initial_lifecycle.channel_events.len();

        let full_build_domain = BTreeSet::from([11_i64, 29]);
        let local_producer_domain = BTreeSet::from([11_i64]);
        let consumer_input = vec![11_i64, 29];
        assert!(full_build_domain.contains(&29));
        assert!(!local_producer_domain.contains(&29));
        assert_eq!(
            consumer_input
                .iter()
                .copied()
                .filter(|key| local_producer_domain.contains(key))
                .collect::<Vec<_>>(),
            vec![11],
            "a local-only artifact would incorrectly reject a valid remote-shard match"
        );

        let probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Int64,
            false,
        )]));
        let build_schema = Arc::new(Schema::new(vec![Field::new(
            "build_key",
            DataType::Int64,
            false,
        )]));
        let join_schema = Arc::new(Schema::new(vec![
            Field::new("probe_key", DataType::Int64, false),
            Field::new("build_key", DataType::Int64, false),
        ]));
        let probe_batch = RecordBatch::try_new(
            Arc::clone(&probe_schema),
            vec![Arc::new(Int64Array::from(vec![11]))],
        )
        .expect("probe batch");
        let build_batch = RecordBatch::try_new(
            Arc::clone(&build_schema),
            vec![Arc::new(Int64Array::from(vec![11]))],
        )
        .expect("local build batch");
        let mut producer_arena = ExprArena::default();
        let probe_expr =
            producer_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let build_expr =
            producer_arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let membership_schema = ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let contract = NativeRuntimeFilterContract::Membership {
            canonical_schema: Arc::from(membership_schema.canonical_bytes()),
            schema_digest: membership_schema.digest().bytes(),
        };
        let producer_plan = ExecPlan {
            arena: producer_arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                probe_batch,
                                chunk_schema_of(&probe_schema, &[SlotId::new(1)]),
                            )
                            .expect("probe chunk"),
                            node_id: 1,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                build_batch,
                                chunk_schema_of(&build_schema, &[SlotId::new(2)]),
                            )
                            .expect("build chunk"),
                            node_id: 2,
                        }),
                    }),
                    node_id: 3,
                    join_type: JoinType::Inner,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(&probe_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&build_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    probe_keys: vec![probe_expr],
                    build_keys: vec![build_expr],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution: JoinRuntimeFilterExecution::Native {
                        producers: vec![NativeJoinRuntimeFilterProducerSpec {
                            binding_id: 3,
                            channel_id: 1,
                            build_expr_id: build_expr,
                            build_key_index: 0,
                            contribution_kinds: BTreeSet::from([
                                ContributionKind::ValueDomainDelta,
                                ContributionKind::ProducerClosed,
                            ]),
                            completion_requirement: CompletionRequirement::ProducerClosed,
                            contract: contract.clone(),
                            reduction: NativeRuntimeFilterReduction::SetUnion,
                        }],
                    },
                }),
            },
        };
        let producer_handle = ResultSinkHandle::new();
        execute_native_plan_with_pipeline(
            producer_plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(producer_handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            Arc::new(
                RuntimeState::default().with_native_runtime_filter_context(Some(
                    context_manager
                        .runtime_filter_context_for_native_execution(
                            query_id,
                            UniqueId { hi: 70, lo: 30 },
                        )
                        .expect("producer runtime-filter context"),
                )),
            ),
            Some(query_id),
            None,
            None,
        )
        .expect("execute dormant producer fragment");
        assert_eq!(
            producer_handle
                .take_chunks()
                .iter()
                .map(Chunk::len)
                .sum::<usize>(),
            1
        );
        let producer_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("producer shares the query lifecycle event sink");
        assert_eq!(producer_lifecycle.filters.len(), installed_filter_count);
        assert_eq!(
            producer_lifecycle.channel_events.len(),
            installed_channel_event_count
        );

        let consumer_schema = Arc::new(Schema::new(vec![Field::new(
            "consumer_key",
            DataType::Int64,
            false,
        )]));
        let consumer_batch = RecordBatch::try_new(
            Arc::clone(&consumer_schema),
            vec![Arc::new(Int64Array::from(consumer_input.clone()))],
        )
        .expect("consumer batch");
        let mut consumer_arena = ExprArena::default();
        let consumer_expr =
            consumer_arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int64);
        let consumer_plan = ExecPlan {
            arena: consumer_arena,
            root: ExecNode {
                kind: ExecNodeKind::NativeRuntimeFilterConsumer(NativeRuntimeFilterConsumerNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                consumer_batch,
                                chunk_schema_of(&consumer_schema, &[SlotId::new(4)]),
                            )
                            .expect("consumer chunk"),
                            node_id: 4,
                        }),
                    }),
                    owner_node_id: 4,
                    bindings: vec![NativeRuntimeFilterConsumerSpec {
                        binding_id: 4,
                        channel_id: 1,
                        expr_id: consumer_expr,
                        activation: ConsumerActivation::BlockingSnapshot,
                        capabilities: BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        contract,
                        reduction: NativeRuntimeFilterReduction::SetUnion,
                    }],
                }),
            },
        };
        let consumer_handle = ResultSinkHandle::new();
        execute_native_plan_with_pipeline(
            consumer_plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(consumer_handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            Arc::new(
                RuntimeState::default().with_native_runtime_filter_context(Some(
                    context_manager
                        .runtime_filter_context_for_native_execution(
                            query_id,
                            UniqueId { hi: 70, lo: 40 },
                        )
                        .expect("consumer runtime-filter context"),
                )),
            ),
            Some(query_id),
            None,
            None,
        )
        .expect("execute dormant consumer fragment");

        let output = consumer_handle
            .take_chunks()
            .into_iter()
            .flat_map(|chunk| {
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("consumer output Int64")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(output, consumer_input);
        let consumer_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("consumer shares the query lifecycle event sink");
        assert_eq!(consumer_lifecycle.filters.len(), installed_filter_count);
        assert_eq!(
            consumer_lifecycle.channel_events.len(),
            installed_channel_event_count
        );
        let hub_error = match context_manager.get_runtime_filter_hub(query_id) {
            Err(error) => error,
            Ok(_) => panic!("NativeService context must remain hub-free"),
        };
        assert!(hub_error.contains("NativeService"), "{hub_error}");
        let worker_error = match context_manager.get_runtime_filter_worker(query_id) {
            Err(error) => error,
            Ok(_) => panic!("NativeService context must remain worker-free"),
        };
        assert!(worker_error.contains("NativeService"), "{worker_error}");
        context_manager.cancel_query(query_id, "test cleanup".to_string());
        context_manager.finish_fragment(query_id);
        context_manager.finish_fragment(query_id);
        assert!(
            context_manager
                .get_runtime_filter_hub(query_id)
                .expect("query context removed after both fragments finish")
                .is_none()
        );
        lifecycle.remove_query(query_key);
    }

    #[test]
    fn group_by_sum_is_correct_with_dop_2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8, 9])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![k],
                    functions: vec![AggFunction {
                        name: "sum".to_string(),
                        inputs: vec![v],
                        input_is_intermediate: false,
                        types: Some(AggTypeSignature {
                            intermediate_type: None,
                            output_type: Some(DataType::Int64),
                            input_arg_type: None,
                        }),
                        ..Default::default()
                    }],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema: chunk_schema_of(
                        &Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int32, false),
                            Field::new("sum", DataType::Int64, true),
                        ])),
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    runtime_filter_spec:
                        crate::exec::node::aggregate::AggregateRuntimeFilterSpec::Native {
                            topn_producers: Vec::new(),
                        },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut out: HashMap<i32, i64> = HashMap::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            assert_eq!(chunk.columns().len(), 2);
            let k_col = chunk.column_by_slot_id(SlotId::new(1)).expect("k column");
            let k_arr = k_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("k Int32");
            let v_col = chunk.column_by_slot_id(SlotId::new(2)).expect("sum column");
            if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i));
                }
            } else if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i) as i64);
                }
            } else {
                panic!("unexpected sum column type: {:?}", v_col.data_type());
            }
        }

        assert_eq!(out.get(&1).copied(), Some(30));
        assert_eq!(out.get(&2).copied(), Some(5));
        assert_eq!(out.get(&3).copied(), Some(24));
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn nljoin_inner_with_conjunct_is_correct() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::Inner,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut pairs = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                pairs.push((a_arr.value(i), b_arr.value(i)));
            }
        }

        assert_eq!(pairs, vec![(1, 2), (1, 4), (3, 4)]);
    }

    #[test]
    fn nljoin_left_outer_emits_null_extended_rows() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3, 5])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::LeftOuter,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let b = if b_arr.is_valid(i) {
                    Some(b_arr.value(i))
                } else {
                    None
                };
                rows.push((a_arr.value(i), b));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, Some(2)), (1, Some(4)), (3, Some(4)), (5, None)]
        );
    }

    #[test]
    fn nljoin_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::FullOuter,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let a = if a_arr.is_valid(i) {
                    Some(a_arr.value(i))
                } else {
                    None
                };
                rows.push((a, b_arr.value(i)));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![(None, 2), (None, 4)]);
    }

    #[test]
    fn hash_left_outer_residual_treats_false_as_no_match() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1, 1, 2])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10, 20, 30])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 1, 3])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 5, 7])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
            Field::new("w", DataType::Int32, true),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::LeftOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: Some(residual),
                    runtime_filter_execution:
                        crate::exec::node::join::JoinRuntimeFilterExecution::Native {
                            producers: Vec::new(),
                        },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let k1 = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let v = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let k2 = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let w = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..chunk.len() {
                let rk = if k2.is_valid(i) {
                    Some(k2.value(i))
                } else {
                    None
                };
                let rw = if w.is_valid(i) {
                    Some(w.value(i))
                } else {
                    None
                };
                rows.push((k1.value(i), v.value(i), rk, rw));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, 10, Some(1), Some(100)),
                (1, 20, Some(1), Some(100)),
                (2, 30, None, None)
            ]
        );
    }

    #[test]
    fn hash_right_outer_emits_unmatched_probe_rows() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 200])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::RightOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution:
                        crate::exec::node::join::JoinRuntimeFilterExecution::Native {
                            producers: Vec::new(),
                        },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left = if lk.is_valid(i) && lv.is_valid(i) {
                    Some((lk.value(i), lv.value(i)))
                } else {
                    None
                };
                rows.push((left, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(Some((1, 10)), 1, 100), (None, 2, 200)]);
    }

    #[test]
    fn hash_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::FullOuter,
                    distribution_mode: JoinDistributionMode::Broadcast,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution:
                        crate::exec::node::join::JoinRuntimeFilterExecution::Native {
                            producers: Vec::new(),
                        },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left_is_null = !lk.is_valid(i) && !lv.is_valid(i);
                rows.push((left_is_null, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(true, 1, 100)]);
    }

    #[test]
    fn analytic_row_number_rank_sum_is_correct() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("o", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let k = Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2])) as arrow::array::ArrayRef;
        let o = Arc::new(Int32Array::from(vec![1, 1, 2, 1, 2])) as arrow::array::ArrayRef;
        let v = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k, o, v]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };
        let analytic_output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("o", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("row_number", DataType::Int64, true),
            Field::new("rank", DataType::Int64, true),
            Field::new("sum", DataType::Int64, true),
        ]));
        let analytic_output_chunk_schema = chunk_schema_of(
            &analytic_output_schema,
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
                SlotId::new(5),
                SlotId::new(6),
            ],
        );

        let mut arena = ExprArena::default();
        let k_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let o_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let v_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Analytic(AnalyticNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    partition_exprs: vec![k_expr],
                    order_by_exprs: vec![o_expr],
                    functions: vec![
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::RowNumber,
                            args: vec![],
                            return_type: DataType::Int64,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Rank,
                            args: vec![],
                            return_type: DataType::Int64,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Sum,
                            args: vec![v_expr],
                            return_type: DataType::Int64,
                        },
                    ],
                    window: Some(WindowFrame {
                        window_type: WindowType::Rows,
                        start: None,
                        end: Some(WindowBoundary::CurrentRow),
                    }),
                    output_columns: vec![
                        AnalyticOutputColumn::InputSlotId(SlotId::new(1)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(2)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(3)),
                        AnalyticOutputColumn::Window(0),
                        AnalyticOutputColumn::Window(1),
                        AnalyticOutputColumn::Window(2),
                    ],
                    output_chunk_schema: analytic_output_chunk_schema,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_rows: Vec<(i32, i32, i32, i64, i64, i64)> = Vec::new();
        for c in handle.take_chunks() {
            if c.is_empty() {
                continue;
            }
            let cols = c.columns();
            assert_eq!(cols.len(), 6);
            let k_arr = cols[0].as_any().downcast_ref::<Int32Array>().unwrap();
            let o_arr = cols[1].as_any().downcast_ref::<Int32Array>().unwrap();
            let v_arr = cols[2].as_any().downcast_ref::<Int32Array>().unwrap();
            let rn_arr = cols[3].as_any().downcast_ref::<Int64Array>().unwrap();
            let r_arr = cols[4].as_any().downcast_ref::<Int64Array>().unwrap();
            let sum_arr = cols[5].as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..c.len() {
                out_rows.push((
                    k_arr.value(i),
                    o_arr.value(i),
                    v_arr.value(i),
                    rn_arr.value(i),
                    r_arr.value(i),
                    sum_arr.value(i),
                ));
            }
        }

        // Preserve input order within each partition.
        assert_eq!(
            out_rows,
            vec![
                (1, 1, 10, 1, 1, 10),
                (1, 1, 20, 2, 1, 30),
                (1, 2, 5, 3, 3, 35),
                (2, 1, 7, 1, 1, 7),
                (2, 2, 8, 2, 2, 15),
            ]
        );
    }

    #[test]
    fn mixed_merge_and_update_aggregates_work() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("sum_state", DataType::Int64, false),
        ]));
        let c1 = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let sum_state = Arc::new(Int64Array::from(vec![30_i64, 5_i64])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![c1, sum_state]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut arena = ExprArena::default();
        let c1_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sum_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![],
                    functions: vec![
                        AggFunction {
                            name: "count".to_string(),
                            inputs: vec![c1_expr],
                            input_is_intermediate: false,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                            ..Default::default()
                        },
                        AggFunction {
                            name: "sum".to_string(),
                            inputs: vec![sum_expr],
                            input_is_intermediate: true,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                            ..Default::default()
                        },
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema: chunk_schema_of(
                        &Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int32, true),
                            Field::new("sum", DataType::Int64, true),
                        ])),
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    runtime_filter_spec:
                        crate::exec::node::aggregate::AggregateRuntimeFilterSpec::Native {
                            topn_producers: Vec::new(),
                        },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            2,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_count = None;
        let mut out_sum = None;
        for chunk in handle.take_chunks() {
            if chunk.is_empty() {
                continue;
            }
            let count_col = chunk
                .column_by_slot_id(SlotId::new(3))
                .expect("count column");
            let sum_col = chunk.column_by_slot_id(SlotId::new(4)).expect("sum column");
            let count_arr = count_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count Int64");
            let sum_arr = sum_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum Int64");
            out_count = Some(count_arr.value(0));
            out_sum = Some(sum_arr.value(0));
        }

        assert_eq!(out_count, Some(2));
        assert_eq!(out_sum, Some(35));
    }
}
