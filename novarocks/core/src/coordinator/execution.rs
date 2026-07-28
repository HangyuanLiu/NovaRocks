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

//! Execution coordinator for multi-fragment SQL execution.
//!
//! Wires and runs:
//! - CTE produce fragments (multicast to consumer exchange nodes)
//! - `Stream` producer fragments, each with a `DATA_STREAM_SINK` that fans out
//!   to every instance of the consumer fragment
//! - The root fragment via the dispatcher (result sink)
//!
//! All instance placement (instance counts, finst ids, backend index,
//! scan-range splits, destinations, prober params, per-exchange sender counts)
//! is owned by [`FragmentScheduler`]. The coordinator translates each placement
//! into native fragment submissions. `RemoteDispatcher` routes per-instance
//! to BEs over gRPC.
//!
//! At a single backend (all-in-one / 1FE+1BE), the scheduler produces one
//! instance per fragment and this path reproduces the prior single-instance
//! wiring exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::coordinator::ports::{CoordinatorExecutionPorts, CoordinatorObserver};
use crate::coordinator::profile::{
    StandaloneQueryProfileGuard, standalone_query_profile_count, take_standalone_query_profiles,
};
use crate::coordinator::report::{StandaloneQueryFailureGuard, take_standalone_query_failure};
use crate::coordinator::runtime_filter_deployment::InstalledRuntimeFilterDeployment;
use crate::coordinator::scheduler::{FragmentInstancePlacement, FragmentScheduler};
use crate::coordinator::write::{RegisteredWriteCoordinator, WriteCoordinator};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::novarocks_logging::debug;
use crate::protocol::native::encode::NativeFragmentBundle;
use crate::query_execution::cancellation::QueryCancellationView;
use crate::query_execution::fragment_transport::{
    FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope,
};
use crate::query_execution::preparation::{
    PreparedFragment, PreparedFragmentRole, PreparedFragmentSet, PreparedOutputColumn,
};
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput, WriterKey};
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_state::QueryState;
use crate::sql::analysis::cte::CteId;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{FragmentEdge, FragmentEdgeKind, FragmentId};

#[cfg(test)]
use crate::coordinator::profile::record_native_standalone_query_profile_report;
#[cfg(test)]
use crate::coordinator::scheduler::SchedulingPlan;

use crate::runtime::query_result::{QueryResult, QueryResultColumn};

fn next_standalone_query_id() -> UniqueId {
    use std::sync::atomic::{AtomicI64, Ordering};

    static NEXT_QUERY_LO: AtomicI64 = AtomicI64::new(100);

    // Fragment/exchange keys can outlive an FE process in still-running BEs,
    // while fragment-instance IDs encode only the query ID's high half. Give
    // every query a fresh high half so IDs cannot repeat either within one FE
    // process or across an immediate FE restart.
    let (uuid_hi, _) = uuid::Uuid::new_v4().as_u64_pair();
    let hi = uuid_hi as i64;
    let lo = NEXT_QUERY_LO.fetch_add(1000, Ordering::Relaxed);
    UniqueId { hi, lo }
}

/// Result of a coordinated execution, exposing the writer-side outcome to the
/// engine layer. `write_commit` is set when writers reported a commit input on
/// the success path. `write_abort` is set when writer-side coordination fails
/// after the root result has been produced and the write coordinator can build
/// an abort input for the engine layer.
#[derive(Debug)]
pub(crate) struct CoordinatedQueryResult {
    pub(crate) query_result: QueryResult,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    pub(crate) fragment_profiles: Vec<RuntimeProfileTree>,
}

/// Coordinates multi-fragment query execution across one or more backends.
///
/// Drives all fragment wiring from [`FragmentScheduler`] placements and submits
/// every instance through the `FragmentDispatcher`. Results are collected by
/// polling the dispatcher for the root fragment's chunks.
pub(crate) struct ExecutionCoordinator {
    prepared: PreparedFragmentSet,
    native_bundle: NativeFragmentBundle,
    execution_ports: CoordinatorExecutionPorts,
    scheduler: Arc<FragmentScheduler>,
    query_options: CoordinatorQueryOptions,
    cancellation: QueryCancellationView,
    #[cfg(test)]
    scheduled_plan_test_drift: Option<ScheduledPlanTestDrift>,
    #[cfg(test)]
    post_install_assembly_test_drift: Option<PostInstallAssemblyTestDrift>,
}

struct PreparedNativeSubmission<'a> {
    tracker: InFlightTracker,
    submissions: Vec<(usize, NativeFragmentEnvelope)>,
    execution_root_fragment_id: FragmentId,
    root_backend_idx: usize,
    root_finst_id: UniqueId,
    timeout_ms: i64,
    root_schedule: &'a PreparedFragment,
    root_uses_result_buffer: bool,
    expected_root_chunk_schema: Option<ChunkSchemaRef>,
    write_registration: Option<RegisteredWriteCoordinator>,
}

/// Query options sealed for the native coordinator boundary.
///
/// Upstream engine entrypoints may omit session options or leave `pipeline_dop`
/// in auto mode. A coordinator, however, must always submit a complete native
/// sidecar with a positive DOP. Keeping that invariant in the field type makes
/// an invalid coordinator state unrepresentable after construction.
struct CoordinatorQueryOptions(QueryOptions);

impl CoordinatorQueryOptions {
    fn from_upstream(query_options: Option<QueryOptions>) -> Self {
        let mut query_options = query_options.unwrap_or_default();
        let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(
            query_options.pipeline_dop.unwrap_or_default(),
        );
        debug_assert!(pipeline_dop > 0, "resolved pipeline DOP must be positive");
        query_options.pipeline_dop = Some(pipeline_dop);
        Self(query_options)
    }

    fn as_runtime_options(&self) -> &QueryOptions {
        &self.0
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum ScheduledPlanTestDrift {
    Missing,
    Unknown,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum PostInstallAssemblyTestDrift {
    MissingRootPlacement,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        execution_ports: CoordinatorExecutionPorts,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<QueryOptions>,
        cancellation: QueryCancellationView,
    ) -> Self {
        Self {
            prepared,
            native_bundle,
            execution_ports,
            scheduler,
            query_options: CoordinatorQueryOptions::from_upstream(query_options),
            cancellation,
            #[cfg(test)]
            scheduled_plan_test_drift: None,
            #[cfg(test)]
            post_install_assembly_test_drift: None,
        }
    }

    pub(crate) fn execute_with_write_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(false)
    }

    pub(crate) fn execute_with_profile_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(true)
    }

    #[cfg(test)]
    pub(crate) fn execute_with_profiles_for_test(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(true)
    }

    fn execute_with_profile_collection(
        self,
        collect_profiles: bool,
    ) -> Result<CoordinatedQueryResult, String> {
        let prepared = self.prepared;
        let native_bundle = self.native_bundle;
        let query_options = self.query_options;
        let cancellation = self.cancellation;
        #[cfg(test)]
        let scheduled_plan_test_drift = self.scheduled_plan_test_drift;
        #[cfg(test)]
        let post_install_assembly_test_drift = self.post_install_assembly_test_drift;
        let CoordinatorExecutionPorts {
            dispatcher,
            report_endpoint,
            observer,
            runtime_filter_policy_provider,
            deployment_epoch_allocator,
            runtime_filter_deployment_control,
        } = self.execution_ports;
        let scheduler = self.scheduler;
        // ---------------------------------------------------------------
        // 1. Allocate query id and run the scheduler.
        // ---------------------------------------------------------------
        // The low half keeps the original sequence so
        // `root_backend_idx = query_id.lo % n` continues to scatter queries.
        let query_id = next_standalone_query_id();
        let edges = prepared.scheduling_view().edges().to_vec();

        debug!(
            "coordinator topology: fragments={} edges={} root={} backends={}",
            native_bundle.fragment_ids().len(),
            edges.len(),
            prepared.scheduling_view().execution_anchor(),
            scheduler.backends().len()
        );
        for e in &edges {
            debug!(
                "coordinator edge: frag {} -> frag {} (exch_node={}, kind={:?}, part={:?})",
                e.source_fragment_id,
                e.target_fragment_id,
                e.target_exchange_node_id,
                match &e.edge_kind {
                    FragmentEdgeKind::Stream => "Stream",
                    FragmentEdgeKind::CteMulticast { .. } => "CteMulticast",
                    FragmentEdgeKind::IcebergChangeStreamRouter { .. } => {
                        "IcebergChangeStreamRouter"
                    }
                },
                e.output_partition.kind,
            );
        }

        validate_prepared_native_payloads(&prepared, &native_bundle)?;
        let plan = scheduler.schedule(prepared.scheduling_view(), query_id.clone())?;
        #[cfg(test)]
        let plan = apply_scheduled_plan_test_drift(plan, scheduled_plan_test_drift);
        validate_artifact_fragment_sets(&prepared, &native_bundle, &plan)?;
        validate_scheduling_placements(&plan)?;

        let installed_runtime_filter_deployment = if let Some(deployment) =
            crate::coordinator::runtime_filter_deployment::prepare_runtime_filter_deployment(
                prepared.runtime_filter_graph(),
                scheduler.live_backend_snapshot(),
                runtime_filter_policy_provider.as_ref(),
                &deployment_epoch_allocator,
            )? {
            let compiled = crate::runtime_filter::deployment::compiler::compile_with_join_progress(
                prepared.runtime_filter_graph(),
                &plan,
                &edges,
                prepared.runtime_filter_join_progress(),
                scheduler.live_backend_snapshot(),
                &deployment.policy.compiler,
                deployment.epoch,
            )
            .map_err(|error| format!("runtime filter deployment compile failed: {error}"))?;
            let installs =
                crate::runtime_filter::deployment::extension::RuntimeFilterDeploymentExtension::new(
                )
                .participant_installs(&compiled)
                .map_err(|error| {
                    format!("runtime filter participant install projection failed: {error}")
                })?;
            let (delivery_expire, query_expire) =
                crate::runtime::query_options::query_expire_durations(Some(
                    query_options.as_runtime_options(),
                ));
            let lifecycle = crate::protocol::native::RuntimeFilterQueryLifecycleOptions {
                delivery_expire,
                query_expire,
                transport_retry_interval: deployment.policy.transport.retry_interval,
                transport_max_attempts: deployment.policy.transport.max_attempts,
                transport_deadline: deployment.policy.transport.deadline,
                transport_max_pending_entries: deployment.policy.transport.max_pending_entries,
                transport_max_pending_bytes: deployment.policy.transport.max_pending_bytes,
            };
            Some(
                crate::coordinator::runtime_filter_deployment::RuntimeFilterInstallBarrier::new(
                    runtime_filter_deployment_control,
                )
                .install_all_or_rollback(
                    query_id,
                    deployment.epoch,
                    lifecycle,
                    deployment.policy.install_rpc_deadline,
                    installs,
                )?,
            )
        } else {
            None
        };
        #[cfg(test)]
        let plan = apply_post_install_assembly_test_drift(plan, post_install_assembly_test_drift);
        let pre_submission_result = (|| -> Result<PreparedNativeSubmission<'_>, String> {
            let execution_root_fragment_id = plan.root_fragment_id;
            let mut native_fragments_by_id =
                native_bundle.into_fragments().collect::<BTreeMap<_, _>>();

            // ---------------------------------------------------------------
            // 2. Build per-edge / CTE consumer indices used for sink wiring.
            // ---------------------------------------------------------------
            // Stream producer fragment id -> its single outgoing plain stream edge.
            // Both edge indices are infallible map-builders: the planner seal
            // (`validate_source_edge_shape`) already owns plain-stream fan-out,
            // plain/router mix, and per-(source, group) router branch/kind/target
            // uniqueness, and guarantees at most one router group per source fragment.
            // Grouping the router edges by source therefore never collides here.
            let stream_edge_by_source = build_stream_edge_by_source(&edges);
            let router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<&FragmentEdge>)> =
                group_router_edges_by_source(&edges)
                    .into_iter()
                    .map(|((source_fragment_id, router_group_id), branch_edges)| {
                        (source_fragment_id, (router_group_id, branch_edges))
                    })
                    .collect();
            // CTE id -> native consumer sidecars: (consumer_fragment_id, exchange_node_id,
            // native partition, output_slot_ids, logical producer column ids).
            let mut cte_consumers: BTreeMap<
                CteId,
                Vec<(
                    FragmentId,
                    i32,
                    crate::proto::plan::DataPartition,
                    Vec<i32>,
                    Vec<ColumnId>,
                )>,
            > = BTreeMap::new();
            for e in &edges {
                match &e.edge_kind {
                    FragmentEdgeKind::Stream => {}
                    FragmentEdgeKind::CteMulticast {
                        cte_id,
                        receive_producer_column_ids,
                    } => {
                        let native_partition =
                            crate::protocol::native::encode::encode_data_partition(
                                &e.output_partition,
                            )?;
                        cte_consumers.entry(*cte_id).or_default().push((
                            e.target_fragment_id,
                            e.target_exchange_node_id,
                            native_partition,
                            e.output_slot_ids.clone(),
                            receive_producer_column_ids.clone(),
                        ));
                    }
                    FragmentEdgeKind::IcebergChangeStreamRouter { .. } => {}
                }
            }
            // CTE consumers may also be expressed via `cte_exchange_nodes` on the
            // consumer fragment when no explicit edge carries them.
            for schedule in prepared.scheduling_view().fragments() {
                for (cte_id, exchange_node_id, receive_producer_column_ids) in
                    schedule.boundary_projection().cte_exchange_nodes()
                {
                    let consumers = cte_consumers.entry(*cte_id).or_default();
                    if !consumers.iter().any(|(fid, nid, _, _, _)| {
                        *fid == schedule.fragment_id() && *nid == *exchange_node_id
                    }) {
                        consumers.push((
                            schedule.fragment_id(),
                            *exchange_node_id,
                            crate::proto::plan::DataPartition {
                                kind: crate::proto::plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            },
                            Vec::new(),
                            receive_producer_column_ids.clone(),
                        ));
                    }
                }
            }

            // ---------------------------------------------------------------
            // 3. Translate every placement into a native fragment submission.
            // ---------------------------------------------------------------
            let needs_fragment_status_report =
                dispatcher.needs_fragment_status_report() || collect_profiles;

            // Snapshot the per-consumer-fragment instance destinations for CTE
            // multicast sub-sinks (each consumer fans out to all of its instances).
            let consumer_dests: BTreeMap<
                FragmentId,
                Vec<crate::runtime::endpoint::FragmentDestination>,
            > = plan
                .by_fragment
                .iter()
                .map(|(fid, insts)| {
                    let dests = insts
                        .iter()
                        .map(|inst| {
                            crate::runtime::endpoint::FragmentDestination::new(
                                inst.finst_id,
                                inst.endpoint.clone(),
                            )
                        })
                        .collect();
                    (*fid, dests)
                })
                .collect();

            let tracker = InFlightTracker::default();
            // Collect submissions by fragment, then submit consumers before
            // producers. This ensures downstream exchange receivers/result buffers
            // are registered before an upstream producer can fail or send data.
            let mut submissions_by_fragment: BTreeMap<
                FragmentId,
                Vec<(usize, NativeFragmentEnvelope)>,
            > = BTreeMap::new();
            let mut expected_writers = Vec::new();

            for (&fragment_id, placements) in &plan.by_fragment {
                let schedule = prepared
                    .fragment(fragment_id)
                    .ok_or_else(|| format!("fragment {fragment_id} missing from prepared set"))?;
                let native_template =
                    native_fragments_by_id.remove(&fragment_id).ok_or_else(|| {
                        format!("native fragment bundle missing fragment {fragment_id}")
                    })?;
                let is_root = fragment_id == execution_root_fragment_id;
                let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
                let router_edges = router_edges_by_source.get(&fragment_id);
                let is_terminal_write = stream_edge.is_none()
                    && router_edges.is_none()
                    && schedule.boundary_projection().cte_id().is_none()
                    && schedule.execution_role().is_terminal_write();
                let is_producer = stream_edge.is_some()
                    || router_edges.is_some()
                    || schedule.boundary_projection().cte_id().is_some();
                validate_fragment_output_kind(
                    fragment_id,
                    is_root,
                    is_terminal_write,
                    is_producer,
                    schedule.execution_role(),
                )?;

                // Classify the fragment once.
                if !is_root
                    && !is_terminal_write
                    && schedule.boundary_projection().cte_id().is_none()
                    && stream_edge.is_none()
                    && router_edges.is_none()
                {
                    return Err(format!(
                        "fragment {fragment_id} is neither root, CTE producer, stream producer, nor \
                     Iceberg change-stream router producer or terminal write fragment; \
                     stream fan-out is not supported in standalone coordinator"
                    ));
                }
                ensure_native_fragment_sink_supported(
                    fragment_id,
                    is_root,
                    is_terminal_write,
                    stream_edge.is_some(),
                    router_edges.is_some(),
                    schedule.boundary_projection().cte_id().is_some(),
                )?;

                for placement in placements {
                    let fragment_has_write_sink = is_terminal_write;
                    let fragment_report_endpoint =
                        if fragment_has_write_sink || needs_fragment_status_report {
                            Some(report_endpoint.clone())
                        } else {
                            None
                        };

                    if fragment_has_write_sink {
                        expected_writers.push(WriterKey {
                            query_id,
                            fragment_instance_id: placement.finst_id,
                            backend_num: placement.instance_index as i32,
                        });
                    }

                    let mut native_fragment = native_template.clone();
                    if !is_root && !is_terminal_write && stream_edge.is_none() {
                        if let Some((router_group_id, branch_edges)) = router_edges {
                            patch_native_iceberg_change_stream_router_sink(
                                &mut native_fragment,
                                fragment_id,
                                *router_group_id,
                                branch_edges,
                                &plan.by_fragment,
                            )?;
                        } else if let Some(cte_id) = schedule.boundary_projection().cte_id() {
                            let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                            patch_native_cte_multicast_sink(
                                &mut native_fragment,
                                fragment_id,
                                cte_id,
                                &consumers,
                                &consumer_dests,
                            )?;
                        }
                    }
                    let typed_result_sink = is_root && needs_fragment_status_report;
                    let native_instance_params =
                        crate::protocol::native::encode::encode_instance_params(
                            &query_id,
                            placement,
                            query_options.as_runtime_options(),
                            placement.instance_index as i32,
                            fragment_report_endpoint.as_ref(),
                            typed_result_sink,
                        )?;
                    let submission =
                        NativeFragmentEnvelope::new(native_fragment, native_instance_params);

                    submissions_by_fragment
                        .entry(fragment_id)
                        .or_default()
                        .push((placement.backend_idx, submission));
                }
            }

            if !native_fragments_by_id.is_empty() {
                return Err(format!(
                    "native fragments remained after submission assembly: {:?}",
                    native_fragments_by_id.keys().collect::<Vec<_>>()
                ));
            }

            if !submissions_by_fragment.contains_key(&execution_root_fragment_id) {
                return Err("root fragment produced no placement".to_string());
            }
            // Submit consumers before producers: iterate the sealed leaves-first
            // topological order in reverse (root first) so downstream exchange
            // receivers / result buffers register before any upstream producer can
            // send. The order is the planner-sealed projection, not recomputed here.
            let mut submissions: Vec<(usize, NativeFragmentEnvelope)> = Vec::new();
            for &fragment_id in prepared.scheduling_view().topological_order().iter().rev() {
                if let Some(mut fragment_submissions) = submissions_by_fragment.remove(&fragment_id)
                {
                    submissions.append(&mut fragment_submissions);
                }
            }
            if !submissions_by_fragment.is_empty() {
                return Err(format!(
                    "submissions remained for unknown fragments: {:?}",
                    submissions_by_fragment.keys().collect::<Vec<_>>()
                ));
            }

            let write_registration = if expected_writers.is_empty() {
                None
            } else {
                Some(RegisteredWriteCoordinator::register(
                    query_id,
                    expected_writers,
                )?)
            };
            let timeout_ms = query_options
                .as_runtime_options()
                .query_timeout
                .map(|t| t as i64 * 1000)
                .unwrap_or(300_000); // 5 minute default
            let root_schedule = prepared
                .fragment(execution_root_fragment_id)
                .ok_or_else(|| "root fragment not found in prepared set".to_string())?;
            let root_uses_result_buffer = !root_schedule.execution_role().is_terminal_write();
            let expected_root_chunk_schema = if root_uses_result_buffer {
                Some(build_root_expected_chunk_schema(root_schedule)?)
            } else {
                None
            };

            Ok(PreparedNativeSubmission {
                tracker,
                submissions,
                execution_root_fragment_id,
                root_backend_idx: plan.root_backend_idx,
                root_finst_id: plan.root_finst_id.clone(),
                timeout_ms,
                root_schedule,
                root_uses_result_buffer,
                expected_root_chunk_schema,
                write_registration,
            })
        })();
        let mut pre_submission = match pre_submission_result {
            Ok(pre_submission) => pre_submission,
            Err(primary_error) => {
                return Err(match installed_runtime_filter_deployment {
                    Some(deployment) => deployment.abort_preserving(primary_error),
                    None => primary_error,
                });
            }
        };
        let write_coordinator = pre_submission
            .write_registration
            .as_ref()
            .map(RegisteredWriteCoordinator::coordinator);

        let fetch_result = submit_and_fetch_loop_with_deployment_lease(
            &dispatcher,
            &mut pre_submission.tracker,
            pre_submission.submissions,
            pre_submission.execution_root_fragment_id,
            pre_submission.root_backend_idx,
            pre_submission.root_finst_id,
            &query_id,
            pre_submission.root_uses_result_buffer,
            pre_submission.timeout_ms,
            pre_submission.expected_root_chunk_schema.as_ref(),
            write_coordinator,
            collect_profiles,
            observer.as_ref(),
            &cancellation,
            installed_runtime_filter_deployment,
        )?;
        if let Some(commit) = fetch_result.write_commit.as_ref() {
            tracing::info!(
                target: "novarocks::write_coordinator",
                write_hi = commit.write_id.hi,
                write_lo = commit.write_id.lo,
                writers = commit.writers.len(),
                "write coordinator commit input ready"
            );
        }

        let root_schedule = pre_submission.root_schedule;
        let chunks = align_fetch_chunks_to_output_columns(
            fetch_result.chunks,
            root_schedule.boundary_projection().output_columns(),
        )?;
        let query_result = QueryResult {
            columns: root_schedule
                .boundary_projection()
                .output_columns()
                .iter()
                .map(|c| QueryResultColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    logical_type: None,
                })
                .collect(),
            chunks,
        };
        Ok(CoordinatedQueryResult {
            query_result,
            write_commit: fetch_result.write_commit,
            write_abort: fetch_result.write_abort,
            fragment_profiles: fetch_result.fragment_profiles.into_values().collect(),
        })
    }

    /// Backward-compatible entry point: runs the coordinated execution and
    /// returns only the query result, discarding the writer outcome. Existing
    /// callers that do not participate in the Iceberg write lifecycle use this.
    pub(crate) fn execute(self) -> Result<QueryResult, String> {
        self.execute_with_write_outcome()
            .and_then(query_result_or_write_abort_error)
    }
}

fn query_result_or_write_abort_error(
    outcome: CoordinatedQueryResult,
) -> Result<QueryResult, String> {
    if let Some(abort) = outcome.write_abort {
        return Err(abort.reason);
    }
    Ok(outcome.query_result)
}

fn build_root_expected_chunk_schema(
    root_fragment: &PreparedFragment,
) -> Result<ChunkSchemaRef, String> {
    let output_columns = root_fragment.boundary_projection().output_columns();
    if output_columns.is_empty() {
        return Ok(Arc::new(ChunkSchema::empty()));
    }

    let mut slots = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        let slot_id = SlotId::new(
            u32::try_from(idx + 1)
                .map_err(|_| "too many root typed result output columns".to_string())?,
        );
        let field = Field::new(
            output.name.clone(),
            output.data_type.clone(),
            output.nullable,
        );
        slots.push(ChunkSlotSchema::new_with_field(slot_id, field, None, None));
    }

    ChunkSchema::try_new(slots).map(Arc::new)
}

fn align_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[PreparedOutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[PreparedOutputColumn],
) -> Result<Chunk, String> {
    if chunk.batch.num_columns() != output_columns.len() {
        return Err(format!(
            "typed root result column count mismatch: chunk has {}, output metadata has {}",
            chunk.batch.num_columns(),
            output_columns.len()
        ));
    }
    if chunk.chunk_schema().slots().len() != output_columns.len() {
        return Err(format!(
            "typed root result slot count mismatch: chunk schema has {}, output metadata has {}",
            chunk.chunk_schema().slots().len(),
            output_columns.len()
        ));
    }

    let mut fields = Vec::with_capacity(output_columns.len());
    let mut arrays = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        let array =
            align_typed_root_array(idx, chunk.batch.column(idx).clone(), &output.data_type)?;
        if let Err(mismatch) = crate::exec::chunk::type_compatibility::check_exact(
            &output.data_type,
            array.data_type(),
        ) {
            return Err(format!(
                "typed root result column {idx} type mismatch: output={:?} chunk={:?} ({:?})",
                output.data_type,
                array.data_type(),
                mismatch.kind
            ));
        }
        fields.push(Field::new(
            output.name.clone(),
            array.data_type().clone(),
            output.nullable || array.null_count() > 0,
        ));
        arrays.push(array);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("align typed root result batch failed: {e}"))?;
    let chunk_schema = chunk
        .chunk_schema()
        .with_fields_in_order(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
        )
        .map(Arc::new)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

fn align_typed_root_array(
    idx: usize,
    array: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    if crate::exec::chunk::type_compatibility::check_exact(output_type, array.data_type()).is_ok() {
        return Ok(array);
    }
    if !same_unit_timestamp_metadata_mismatch(output_type, array.data_type()) {
        return Ok(array);
    }
    crate::exec::chunk::type_compatibility::retag_column(&array, output_type).map_err(|mismatch| {
        format!(
            "typed root result column {idx} timestamp metadata retag failed: output={:?} chunk={:?} ({:?})",
            output_type,
            array.data_type(),
            mismatch.kind
        )
    })
}

fn same_unit_timestamp_metadata_mismatch(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (DataType::Timestamp(expected_unit, _), DataType::Timestamp(actual_unit, _))
            if expected_unit == actual_unit
    )
}

// Index each plain `Stream` producer fragment to its single outgoing stream
// edge. This is an infallible projection of the sealed edge set: the planner
// seal (`validate_source_edge_shape`) already rejects plain-stream fan-out and
// any plain/router mix, so at most one plain stream edge exists per source and
// the insert never overwrites. Re-adding a shape check here would duplicate a
// planner-owned decision (guarded by `planner_topology_contract`).
fn build_stream_edge_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> BTreeMap<FragmentId, &'a FragmentEdge> {
    let mut stream_edge_by_source = BTreeMap::new();
    for edge in edges {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        stream_edge_by_source.insert(edge.source_fragment_id, edge);
    }
    stream_edge_by_source
}

// Group Iceberg change-stream router edges by (source fragment, router group).
// This is an infallible projection of the sealed edge set: the planner seal
// (`validate_source_edge_shape`) already owns plain/router mix rejection and the
// per-(source, group) branch_id / branch_kind / target-exchange uniqueness that
// this used to re-check, so grouping here only collects the sealed branches. Re-
// adding a shape check here would duplicate a planner-owned decision (guarded by
// `planner_topology_contract`).
fn group_router_edges_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> BTreeMap<(FragmentId, i32), Vec<&'a FragmentEdge>> {
    let mut grouped: BTreeMap<(FragmentId, i32), Vec<&FragmentEdge>> = BTreeMap::new();
    for edge in edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id, ..
        } = edge.edge_kind
        else {
            continue;
        };
        grouped
            .entry((edge.source_fragment_id, router_group_id))
            .or_default()
            .push(edge);
    }
    grouped
}

fn validate_write_commit_ready(
    write: &Arc<Mutex<WriteCoordinator>>,
) -> Result<WriteCommitInput, String> {
    write.lock().expect("write coordinator lock").commit_input()
}

fn ensure_native_fragment_sink_supported(
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    has_stream_edge: bool,
    has_router_edges: bool,
    has_cte_id: bool,
) -> Result<(), String> {
    if is_root || is_terminal_write || has_stream_edge || has_router_edges || has_cte_id {
        return Ok(());
    }

    let dynamic_sink = "dynamic fragment sink";
    Err(format!(
        "native submission cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the native sink contract must carry dynamic destinations before this fragment can be submitted"
    ))
}

fn validate_fragment_output_kind(
    fragment_id: FragmentId,
    is_root: bool,
    is_terminal_write: bool,
    is_producer: bool,
    output_kind: PreparedFragmentRole,
) -> Result<(), String> {
    if is_root {
        return match output_kind {
            PreparedFragmentRole::Result | PreparedFragmentRole::TerminalWrite => Ok(()),
            PreparedFragmentRole::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result or TerminalWrite output kind"
            )),
        };
    }
    if is_terminal_write {
        return (output_kind == PreparedFragmentRole::TerminalWrite)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "terminal write fragment {fragment_id} must have TerminalWrite output kind, got {output_kind:?}"
                )
            });
    }
    if is_producer {
        return (output_kind == PreparedFragmentRole::NonTerminal)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "producer fragment {fragment_id} must have NonTerminal output kind, got {output_kind:?}"
                )
            });
    }
    Ok(())
}

fn validate_prepared_native_payloads(
    prepared: &PreparedFragmentSet,
    native_bundle: &NativeFragmentBundle,
) -> Result<(), String> {
    let prepared_ids = prepared.fragment_ids();
    for (fragment_id, fragment) in native_bundle.fragments_in_id_order() {
        if fragment.fragment_id != fragment_id {
            return Err(format!(
                "native fragment bundle key {fragment_id} does not match encoded fragment id {}",
                fragment.fragment_id
            ));
        }
    }
    for fragment_id in &prepared_ids {
        native_bundle.get(*fragment_id).ok_or_else(|| {
            format!("native fragment bundle missing prepared fragment id={fragment_id}")
        })?;
        let fragment = prepared
            .fragment(*fragment_id)
            .ok_or_else(|| format!("prepared fragment set missing id={fragment_id}"))?;
        for (index, boundary) in fragment
            .boundary_projection()
            .contracts()
            .iter()
            .enumerate()
        {
            if !prepared_ids.contains(&boundary.fragment_id) {
                return Err(format!(
                    "prepared boundary {index} for fragment {fragment_id} references missing fragment id={}",
                    boundary.fragment_id
                ));
            }
        }
    }
    Ok(())
}

fn validate_artifact_fragment_sets(
    prepared: &PreparedFragmentSet,
    native_bundle: &NativeFragmentBundle,
    scheduling: &crate::coordinator::scheduler::SchedulingPlan,
) -> Result<(), String> {
    let expected = prepared.fragment_ids();
    let native = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    if native != expected {
        return Err(fragment_set_mismatch("native", &expected, &native));
    }
    let scheduled = scheduling.fragment_ids().collect::<BTreeSet<_>>();
    if scheduled != expected {
        return Err(fragment_set_mismatch("scheduled", &expected, &scheduled));
    }
    Ok(())
}

fn fragment_set_mismatch(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    format!(
        "{label} fragment ids mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    )
}

fn validate_scheduling_placements(
    plan: &crate::coordinator::scheduler::SchedulingPlan,
) -> Result<(), String> {
    for (&fragment_id, placements) in &plan.by_fragment {
        if placements.is_empty() {
            return Err(format!(
                "native scheduling plan fragment {fragment_id} has no placements"
            ));
        }
        for (placement_index, placement) in placements.iter().enumerate() {
            if placement.fragment_id != fragment_id {
                return Err(format!(
                    "native scheduling plan map key {fragment_id} does not match placement \
                     {placement_index} fragment_id {}",
                    placement.fragment_id
                ));
            }
        }
    }
    Ok(())
}

fn patch_native_iceberg_change_stream_router_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    let mut patched_fragment = fragment.clone();
    patch_native_iceberg_change_stream_router_sink_in_place(
        &mut patched_fragment,
        fragment_id,
        router_group_id,
        branch_edges,
        placements,
    )?;
    *fragment = patched_fragment;
    Ok(())
}

fn patch_native_iceberg_change_stream_router_sink_in_place(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&FragmentEdge],
    placements: &BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
) -> Result<(), String> {
    if branch_edges.is_empty() {
        return Err("native Iceberg change-stream router sink has no branch edges".to_string());
    }
    let router = match fragment.sink.as_mut().and_then(|sink| sink.kind.as_mut()) {
        Some(crate::proto::plan::data_sink::Kind::IcebergChangeStreamRouter(router)) => router,
        _ => {
            return Err(format!(
                "fragment {fragment_id} is router source for group {router_group_id} but native \
                 fragment payload is missing ICEBERG_CHANGE_STREAM_ROUTER_SINK"
            ));
        }
    };

    if router.group_id != router_group_id {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} expected group={router_group_id} \
             but encoded group={}",
            router.group_id
        ));
    }

    let mut edge_route_keys = BTreeSet::new();
    for edge in branch_edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: edge_group_id,
            branch_id,
            branch_kind,
        } = &edge.edge_kind
        else {
            return Err(format!(
                "fragment {} edge to fragment {} is not an Iceberg change-stream router edge",
                edge.source_fragment_id, edge.target_fragment_id
            ));
        };
        if *edge_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge_group_id
            ));
        }
        if !edge_route_keys.insert((*branch_id, *branch_kind)) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate branch edge route key branch_id={branch_id} branch_kind={branch_kind:?}"
            ));
        }
    }

    let mut encoded_route_keys = BTreeSet::new();
    for route in &router.branches {
        let branch_kind = native_change_stream_branch_kind(route.branch_kind).map_err(|err| {
            format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 branch_id={} has invalid encoded branch kind: {err}",
                route.branch_id
            )
        })?;
        if !encoded_route_keys.insert((route.branch_id, branch_kind)) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate encoded route key branch_id={} branch_kind={branch_kind:?}",
                route.branch_id
            ));
        }
    }

    if encoded_route_keys != edge_route_keys {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
             route key set mismatch: encoded={encoded_route_keys:?}, branch_edges={edge_route_keys:?}"
        ));
    }

    for edge in branch_edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id: edge_group_id,
            branch_id,
            branch_kind,
        } = edge.edge_kind
        else {
            return Err(format!(
                "fragment {} edge to fragment {} is not an Iceberg change-stream router edge",
                edge.source_fragment_id, edge.target_fragment_id
            ));
        };
        if edge_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge_group_id
            ));
        }

        let route = router
            .branches
            .iter_mut()
            .find(|route| {
                route.branch_id == branch_id
                    && native_change_stream_branch_kind(route.branch_kind)
                        .is_ok_and(|route_kind| route_kind == branch_kind)
            })
            .ok_or_else(|| {
                format!(
                    "native Iceberg change-stream router source={} group={} branch_id={} \
                     branch_kind={:?} has no matching branch route",
                    fragment_id, router_group_id, branch_id, branch_kind
                )
            })?;
        route.target_fragment_id = edge.target_fragment_id;
        route.target_exchange_node_id = edge.target_exchange_node_id;

        if route.output_partition.is_none() {
            return Err(format!(
                "native Iceberg change-stream router source={} group={} branch_id={} \
                 branch_kind={:?} missing output_partition from native encoder",
                fragment_id, router_group_id, branch_id, branch_kind
            ));
        }

        let dests = placements.get(&edge.target_fragment_id).ok_or_else(|| {
            format!(
                "native Iceberg change-stream router source={} group={} branch_id={} target \
                 fragment {} has no placements",
                fragment_id, router_group_id, branch_id, edge.target_fragment_id
            )
        })?;
        route.destinations = Some(crate::proto::plan::StreamDestinationList {
            destinations: dests
                .iter()
                .map(|placement| {
                    native_stream_destination(&crate::runtime::endpoint::FragmentDestination::new(
                        placement.finst_id,
                        placement.endpoint.clone(),
                    ))
                })
                .collect(),
        });
    }

    debug!(
        "patched native Iceberg change-stream router sink: fragment={} group={} branches={}",
        fragment_id,
        router_group_id,
        branch_edges.len()
    );
    Ok(())
}

fn native_change_stream_branch_kind(
    value: i32,
) -> Result<crate::sql::common::ChangeStreamBranchKind, String> {
    match crate::proto::plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown native ChangeStreamBranchKind value {value}"))?
    {
        crate::proto::plan::ChangeStreamBranchKind::DeleteDv => {
            Ok(crate::sql::common::ChangeStreamBranchKind::DeleteDv)
        }
        crate::proto::plan::ChangeStreamBranchKind::ReuseData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::ReuseData)
        }
        crate::proto::plan::ChangeStreamBranchKind::FreshData => {
            Ok(crate::sql::common::ChangeStreamBranchKind::FreshData)
        }
        crate::proto::plan::ChangeStreamBranchKind::Unspecified => {
            Err("native ChangeStreamBranchKind is unspecified".to_string())
        }
    }
}

fn patch_native_cte_multicast_sink(
    fragment: &mut crate::proto::plan::PlanFragment,
    fragment_id: FragmentId,
    cte_id: CteId,
    consumers: &[(
        FragmentId,
        i32,
        crate::proto::plan::DataPartition,
        Vec<i32>,
        Vec<ColumnId>,
    )],
    consumer_dests: &BTreeMap<FragmentId, Vec<crate::runtime::endpoint::FragmentDestination>>,
) -> Result<(), String> {
    if consumers.is_empty() {
        return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
    }
    let mut sinks = Vec::with_capacity(consumers.len());
    let mut destinations = Vec::with_capacity(consumers.len());
    for (
        consumer_fragment_id,
        exchange_node_id,
        partition,
        output_slot_ids,
        receive_producer_column_ids,
    ) in consumers
    {
        let sink_output_columns = native_cte_multicast_sink_output_columns(
            fragment,
            cte_id,
            *consumer_fragment_id,
            *exchange_node_id,
            output_slot_ids,
            receive_producer_column_ids,
        )?;
        sinks.push(crate::proto::plan::DataStreamSink {
            dest_node_id: *exchange_node_id,
            output_partition: Some(partition.clone()),
            output_columns: sink_output_columns,
            limit: None,
        });
        let dests = consumer_dests.get(consumer_fragment_id).ok_or_else(|| {
            format!("CTE consumer fragment {consumer_fragment_id} has no placements")
        })?;
        destinations.push(crate::proto::plan::StreamDestinationList {
            destinations: dests.iter().map(native_stream_destination).collect(),
        });
    }
    fragment.sink = Some(crate::proto::plan::DataSink {
        kind: Some(crate::proto::plan::data_sink::Kind::MultiCastDataStream(
            crate::proto::plan::MultiCastDataStreamSink {
                sinks,
                destinations,
            },
        )),
    });
    debug!(
        "patched native CTE multicast sink: fragment={} cte_id={} sinks={}",
        fragment_id,
        cte_id,
        consumers.len()
    );
    Ok(())
}

fn native_stream_destination(
    src: &crate::runtime::endpoint::FragmentDestination,
) -> crate::proto::plan::StreamDestination {
    crate::proto::plan::StreamDestination {
        finst_id: Some(crate::proto::common::UniqueId {
            hi: src.finst_id().hi,
            lo: src.finst_id().lo,
        }),
        endpoint: src.endpoint().as_host_port(),
    }
}

fn native_cte_multicast_sink_output_columns(
    fragment: &crate::proto::plan::PlanFragment,
    cte_id: CteId,
    consumer_fragment_id: FragmentId,
    exchange_node_id: i32,
    requested_output_slot_ids: &[i32],
    receive_producer_column_ids: &[ColumnId],
) -> Result<Vec<i32>, String> {
    if requested_output_slot_ids.is_empty() {
        return Ok(Vec::new());
    }

    // The CTE producer fragment is sealed with `DataSink::Noop`, so the planner
    // seal (CGO-9C Task 2, `finalize_fragment_output_columns`) adopts the
    // producer root's wire output wholesale into `fragment.output_columns`. That
    // sealed contract is the authoritative producer-root output; read it directly
    // rather than re-walking the encoded tree (the retired
    // `encoded_fragment_root_output_columns` read-walk, deleted in CGO-9C Task 5).
    let root_columns = fragment.output_columns.clone();
    let root_slot_ids = root_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id).map_err(|_| {
                format!(
                    "native CTE source output column {} cannot convert to slot id",
                    column.column_id
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let root_slot_id_set = root_slot_ids.iter().copied().collect::<BTreeSet<_>>();
    if requested_output_slot_ids
        .iter()
        .all(|slot_id| root_slot_id_set.contains(slot_id))
    {
        return Ok(requested_output_slot_ids.to_vec());
    }
    if receive_producer_column_ids.len() == requested_output_slot_ids.len()
        && let Some(mapped) = receive_producer_column_ids
            .iter()
            .map(|column_id| {
                let slot_id = i32::try_from(column_id.0).ok()?;
                root_slot_id_set.contains(&slot_id).then_some(slot_id)
            })
            .collect::<Option<Vec<_>>>()
    {
        return Ok(mapped);
    }
    let contract_slot_map =
        native_cte_multicast_contract_slot_map(fragment, &root_columns, &root_slot_id_set);
    if let Some(mapped) = requested_output_slot_ids
        .iter()
        .map(|slot_id| contract_slot_map.get(slot_id).copied())
        .collect::<Option<Vec<_>>>()
    {
        return Ok(mapped);
    }
    if requested_output_slot_ids.len() == root_slot_ids.len() {
        return Ok(root_slot_ids);
    }
    Err(format!(
        "native CTE multicast sink output columns for cte_id={cte_id} consumer_fragment={consumer_fragment_id} exchange_node_id={exchange_node_id} ({requested_output_slot_ids:?}) do not match source root output columns ({root_slot_ids:?})"
    ))
}

fn native_cte_multicast_contract_slot_map(
    fragment: &crate::proto::plan::PlanFragment,
    root_columns: &[crate::proto::common::OutputColumn],
    root_slot_id_set: &BTreeSet<i32>,
) -> BTreeMap<i32, i32> {
    let mut map = BTreeMap::new();

    if fragment.output_exprs.len() == fragment.output_columns.len() {
        for (output, expr) in fragment
            .output_columns
            .iter()
            .zip(fragment.output_exprs.iter())
        {
            let Some(crate::proto::expr::expr::Kind::ColumnRef(column_ref)) = expr.kind.as_ref()
            else {
                continue;
            };
            let Ok(contract_id) = i32::try_from(output.column_id) else {
                continue;
            };
            let Ok(root_id) = i32::try_from(column_ref.column_id) else {
                continue;
            };
            if root_slot_id_set.contains(&root_id) {
                map.insert(contract_id, root_id);
            }
        }
    }

    for output in &fragment.output_columns {
        let Ok(contract_id) = i32::try_from(output.column_id) else {
            continue;
        };
        if map.contains_key(&contract_id) {
            continue;
        }
        let mut matches = root_columns.iter().filter(|root| {
            root.name == output.name
                && root.nullable == output.nullable
                && root.r#type == output.r#type
        });
        let Some(root) = matches.next() else {
            continue;
        };
        if matches.next().is_some() {
            continue;
        }
        if let Ok(root_id) = i32::try_from(root.column_id) {
            map.insert(contract_id, root_id);
        }
    }

    if fragment.output_columns.len() <= root_columns.len() {
        for (output, root) in fragment.output_columns.iter().zip(root_columns.iter()) {
            let Ok(contract_id) = i32::try_from(output.column_id) else {
                continue;
            };
            if map.contains_key(&contract_id) {
                continue;
            }
            if let Ok(root_id) = i32::try_from(root.column_id) {
                map.insert(contract_id, root_id);
            }
        }
    }

    map
}

// ---------------------------------------------------------------------------
// In-flight instance tracking (per-backend cancellation)
// ---------------------------------------------------------------------------

/// Tracks attempted fragment instances grouped by backend so that, on any
/// failure, cancellation can cover work whose submit outcome is unknown.
#[derive(Default)]
pub(crate) struct InFlightTracker {
    pub(crate) by_backend: BTreeMap<usize, Vec<UniqueId>>,
}

impl InFlightTracker {
    /// Record that submission of `finst_id` is in flight on `backend_idx`.
    pub(crate) fn record_submitted(&mut self, backend_idx: usize, finst_id: UniqueId) {
        self.by_backend
            .entry(backend_idx)
            .or_default()
            .push(finst_id);
    }

    /// Cancel every recorded instance on its backend. Idempotent.
    pub(crate) fn cancel_all(&self, dispatcher: &dyn FragmentDispatcher) {
        for (idx, ids) in &self.by_backend {
            dispatcher.cancel_fragments(*idx, ids);
        }
    }
}

pub(crate) fn poll_write_failure_and_cancel(
    write: &Arc<Mutex<WriteCoordinator>>,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
) -> Result<(), String> {
    let reason = {
        write
            .lock()
            .expect("write coordinator lock")
            .failed_reason()
    };
    let Some(reason) = reason else {
        return Ok(());
    };

    tracker.cancel_all(dispatcher);
    write
        .lock()
        .expect("write coordinator lock")
        .mark_canceled_except_finished(reason.clone());
    Err(reason)
}

#[derive(Debug)]
pub(crate) struct SubmitAndFetchResult {
    pub(crate) chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    pub(crate) fragment_profiles: BTreeMap<UniqueId, RuntimeProfileTree>,
}

struct QueryStateRegistrationGuard {
    query_id: crate::runtime::query_context::QueryId,
}

impl Drop for QueryStateRegistrationGuard {
    fn drop(&mut self) {
        crate::runtime::query_state::in_flight_table().forget(self.query_id);
    }
}

// ---------------------------------------------------------------------------
// Submit-and-fetch orchestration (testable helper)
// ---------------------------------------------------------------------------

fn prevalidate_fragment_submissions(
    submissions: &[(usize, NativeFragmentEnvelope)],
    expected_query_id: UniqueId,
    expected_root_fragment_id: FragmentId,
    root_backend_idx: usize,
    root_finst_id: UniqueId,
) -> Result<Vec<UniqueId>, String> {
    let mut ids = Vec::with_capacity(submissions.len());
    let mut seen = BTreeSet::new();
    let mut root_matches = 0_usize;
    for (index, (backend_idx, submission)) in submissions.iter().enumerate() {
        let finst_id = submission
            .fragment_instance_id()
            .map_err(|e| format!("fragment submission {index}: {e}"))?;
        let context = format!(
            "fragment submission {index} (fragment_id={}, fragment_instance_id={}/{})",
            submission.fragment_id(),
            finst_id.hi,
            finst_id.lo
        );
        if finst_id.hi == 0 && finst_id.lo == 0 {
            return Err(format!("{context} has zero fragment_instance_id"));
        }
        let submission_query_id = submission
            .query_id()
            .map_err(|e| format!("{context}: {e}"))?;
        if submission_query_id.hi == 0 && submission_query_id.lo == 0 {
            return Err(format!("{context} has zero query_id"));
        }
        if submission_query_id != expected_query_id {
            return Err(format!(
                "{context} query_id mismatch: expected {}/{}, got {}/{}",
                expected_query_id.hi,
                expected_query_id.lo,
                submission_query_id.hi,
                submission_query_id.lo
            ));
        }
        if !seen.insert(finst_id) {
            return Err(format!(
                "duplicate fragment_instance_id {finst_id} at {context}"
            ));
        }
        if finst_id == root_finst_id {
            root_matches += 1;
            if submission.fragment_id() != expected_root_fragment_id
                || *backend_idx != root_backend_idx
            {
                return Err(format!(
                    "root submission identity mismatch for fragment_instance_id={}/{}: expected fragment_id={} backend={}, got fragment_id={} backend={}",
                    root_finst_id.hi,
                    root_finst_id.lo,
                    expected_root_fragment_id,
                    root_backend_idx,
                    submission.fragment_id(),
                    backend_idx
                ));
            }
        }
        ids.push(finst_id);
    }
    if root_matches != 1 {
        return Err(format!(
            "root submission missing or duplicated: expected fragment_id={expected_root_fragment_id} backend={root_backend_idx} fragment_instance_id={}/{}, found {root_matches} matching fragment_instance_id values",
            root_finst_id.hi, root_finst_id.lo
        ));
    }
    Ok(ids)
}

/// Submit each `(backend_idx, params)` through the dispatcher in order, tracking
/// accepted instances per backend, then poll the root fragment until EOF.
///
/// On any submit failure or fetch error, all attempted instances are cancelled
/// (fanned out per backend) before the error is returned.
#[cfg(test)]
pub(crate) fn submit_and_fetch_loop(
    dispatcher: &Arc<dyn FragmentDispatcher>,
    tracker: &mut InFlightTracker,
    submissions: Vec<(usize, NativeFragmentEnvelope)>,
    execution_root_fragment_id: FragmentId,
    root_backend_idx: usize,
    root_finst_id: UniqueId,
    query_id: &UniqueId,
    root_uses_result_buffer: bool,
    timeout_ms: i64,
    expected_root_chunk_schema: Option<&ChunkSchemaRef>,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
    collect_profiles: bool,
    observer: &dyn CoordinatorObserver,
    cancellation: &QueryCancellationView,
) -> Result<SubmitAndFetchResult, String> {
    submit_and_fetch_loop_with_deployment_lease(
        dispatcher,
        tracker,
        submissions,
        execution_root_fragment_id,
        root_backend_idx,
        root_finst_id,
        query_id,
        root_uses_result_buffer,
        timeout_ms,
        expected_root_chunk_schema,
        write_coordinator,
        collect_profiles,
        observer,
        cancellation,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn submit_and_fetch_loop_with_deployment_lease(
    dispatcher: &Arc<dyn FragmentDispatcher>,
    tracker: &mut InFlightTracker,
    submissions: Vec<(usize, NativeFragmentEnvelope)>,
    execution_root_fragment_id: FragmentId,
    root_backend_idx: usize,
    root_finst_id: UniqueId,
    query_id: &UniqueId,
    root_uses_result_buffer: bool,
    timeout_ms: i64,
    expected_root_chunk_schema: Option<&ChunkSchemaRef>,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
    collect_profiles: bool,
    observer: &dyn CoordinatorObserver,
    cancellation: &QueryCancellationView,
    mut installed_runtime_filter_deployment: Option<InstalledRuntimeFilterDeployment>,
) -> Result<SubmitAndFetchResult, String> {
    const REMOTE_FETCH_POLL_INTERVAL_MS: i64 = 300;
    let runtime_query_id = crate::runtime::query_context::QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let _query_state_guard = QueryStateRegistrationGuard {
        query_id: runtime_query_id,
    };
    let _failure_guard = StandaloneQueryFailureGuard::register(query_id);
    let _profile_guard = collect_profiles.then(|| StandaloneQueryProfileGuard::register(query_id));
    let validated_finst_ids = match prevalidate_fragment_submissions(
        &submissions,
        *query_id,
        execution_root_fragment_id,
        root_backend_idx,
        root_finst_id,
    ) {
        Ok(validated_finst_ids) => validated_finst_ids,
        Err(primary_error) => {
            return Err(match installed_runtime_filter_deployment.take() {
                Some(deployment) => deployment.abort_preserving(primary_error),
                None => primary_error,
            });
        }
    };
    // Register every validated placement before the first remote submission.
    // Otherwise a backend-loss event between submit success and registration
    // can miss the query permanently because the registry emits only the state
    // transition event. QueryStateRegistrationGuard removes these provisional
    // mappings on every return path, including partial submit failure.
    for ((backend_idx, _), finst_id) in submissions.iter().zip(&validated_finst_ids) {
        crate::runtime::query_state::in_flight_table().register(
            runtime_query_id,
            finst_id.clone(),
            *backend_idx,
        );
    }

    for ((backend_idx, submission), finst_id) in submissions.into_iter().zip(validated_finst_ids) {
        // A unary submit may return an error after the remote participant has
        // accepted the fragment. Arm cancellation before dispatch so the
        // unknown-outcome attempt is covered together with prior accepts.
        tracker.record_submitted(backend_idx, finst_id.clone());
        if let Err(e) = dispatcher.submit_fragment(backend_idx, submission) {
            tracker.cancel_all(dispatcher.as_ref());
            return Err(match installed_runtime_filter_deployment.take() {
                Some(deployment) => deployment.abort_preserving(e),
                None => e,
            });
        }
        observer.fragment_scheduled();
        if let Some(membership) = crate::coordinator::cluster::cluster_membership() {
            membership.record_scheduled_fragment(backend_idx as crate::coordinator::cluster::BeId);
        }
        if crate::runtime::query_state::in_flight_table().state(runtime_query_id)
            == Some(QueryState::Failed)
        {
            let reason = crate::runtime::query_state::in_flight_table()
                .failure_reason(runtime_query_id)
                .unwrap_or_else(|| format!("query {} failed", runtime_query_id));
            tracker.cancel_all(dispatcher.as_ref());
            return Err(match installed_runtime_filter_deployment.take() {
                Some(deployment) => deployment.abort_preserving(reason),
                None => reason,
            });
        }
    }

    if let Some(deployment) = installed_runtime_filter_deployment.take() {
        deployment.release();
    }

    let mut chunks = Vec::new();
    let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
    let deadline = std::time::Instant::now() + timeout;
    if root_uses_result_buffer {
        loop {
            if let Some(write) = write_coordinator
                && let Err(e) = poll_write_failure_and_cancel(write, tracker, dispatcher.as_ref())
            {
                let abort = write.lock().expect("write coordinator lock").abort_input();
                let Some(abort) = abort else {
                    return Err(e);
                };
                return Ok(SubmitAndFetchResult {
                    chunks,
                    write_commit: None,
                    write_abort: Some(abort),
                    fragment_profiles: BTreeMap::new(),
                });
            }
            if let Some(err) = take_standalone_query_failure(query_id) {
                tracker.cancel_all(dispatcher.as_ref());
                return Err(err);
            }
            if cancellation.is_cancelled() {
                tracker.cancel_all(dispatcher.as_ref());
                return Err("client disconnected".to_string());
            }
            if crate::runtime::query_state::in_flight_table().state(runtime_query_id)
                == Some(QueryState::Failed)
            {
                let reason = crate::runtime::query_state::in_flight_table()
                    .failure_reason(runtime_query_id)
                    .unwrap_or_else(|| format!("query {} failed", runtime_query_id));
                tracker.cancel_all(dispatcher.as_ref());
                return Err(reason);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                tracker.cancel_all(dispatcher.as_ref());
                return Err(format!("query timed out after {timeout_ms} ms"));
            }
            let remaining_ms = deadline
                .saturating_duration_since(now)
                .as_millis()
                .min(i64::MAX as u128) as i64;
            let fetch_wait_ms = remaining_ms.clamp(1, REMOTE_FETCH_POLL_INTERVAL_MS);
            match dispatcher.fetch_result(
                root_backend_idx,
                root_finst_id.clone(),
                fetch_wait_ms,
                expected_root_chunk_schema,
            ) {
                Err(e) => {
                    tracker.cancel_all(dispatcher.as_ref());
                    return Err(e);
                }
                Ok(FetchOutcome::Ready(chunk)) => chunks.push(chunk),
                Ok(FetchOutcome::NotReady) => continue,
                Ok(FetchOutcome::Eof) => break,
                Ok(FetchOutcome::Err(e)) => {
                    tracker.cancel_all(dispatcher.as_ref());
                    return Err(e);
                }
            }
        }
    } else if write_coordinator.is_none() {
        tracker.cancel_all(dispatcher.as_ref());
        return Err(format!(
            "root fragment {}/{} does not produce a result buffer and has no write coordinator",
            root_finst_id.hi, root_finst_id.lo
        ));
    } else if cancellation.is_cancelled() {
        tracker.cancel_all(dispatcher.as_ref());
        return Err("client disconnected".to_string());
    } else if std::time::Instant::now() >= deadline {
        tracker.cancel_all(dispatcher.as_ref());
        return Err(format!("query timed out after {timeout_ms} ms"));
    }

    let (write_commit, write_abort) = if let Some(write) = write_coordinator {
        match wait_for_write_commit_ready(
            write,
            tracker,
            dispatcher.as_ref(),
            deadline,
            timeout_ms,
            cancellation,
        ) {
            Ok(commit) => (Some(commit), None),
            Err(e) => {
                let abort = write.lock().expect("write coordinator lock").abort_input();
                let Some(abort) = abort else {
                    return Err(e);
                };
                (None, Some(abort))
            }
        }
    } else {
        (None, None)
    };

    let fragment_profiles = if collect_profiles {
        wait_for_profile_reports(
            query_id,
            tracker.by_backend.values().map(Vec::len).sum(),
            tracker,
            dispatcher.as_ref(),
            deadline,
            timeout_ms,
            runtime_query_id,
            cancellation,
        )?
    } else {
        BTreeMap::new()
    };

    Ok(SubmitAndFetchResult {
        chunks,
        write_commit,
        write_abort,
        fragment_profiles,
    })
}

fn wait_for_profile_reports(
    query_id: &UniqueId,
    expected_reports: usize,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
    deadline: std::time::Instant,
    timeout_ms: i64,
    runtime_query_id: crate::runtime::query_context::QueryId,
    cancellation: &QueryCancellationView,
) -> Result<BTreeMap<UniqueId, RuntimeProfileTree>, String> {
    const PROFILE_REPORT_POLL_INTERVAL_MS: i64 = 10;

    if expected_reports == 0 {
        return Ok(BTreeMap::new());
    }

    loop {
        let received = standalone_query_profile_count(query_id);
        if received >= expected_reports {
            return Ok(take_standalone_query_profiles(query_id));
        }

        if let Some(err) = take_standalone_query_failure(query_id) {
            tracker.cancel_all(dispatcher);
            return Err(err);
        }
        if cancellation.is_cancelled() {
            tracker.cancel_all(dispatcher);
            return Err("client disconnected".to_string());
        }
        if crate::runtime::query_state::in_flight_table().state(runtime_query_id)
            == Some(QueryState::Failed)
        {
            let reason = crate::runtime::query_state::in_flight_table()
                .failure_reason(runtime_query_id)
                .unwrap_or_else(|| format!("query {} failed", runtime_query_id));
            tracker.cancel_all(dispatcher);
            return Err(reason);
        }

        let now = std::time::Instant::now();
        if now >= deadline {
            tracker.cancel_all(dispatcher);
            return Err(format!(
                "query timed out after {timeout_ms} ms waiting for fragment profile reports: received {received} of {expected_reports}"
            ));
        }

        let remaining_ms = deadline
            .saturating_duration_since(now)
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let sleep_ms = remaining_ms.clamp(1, PROFILE_REPORT_POLL_INTERVAL_MS);
        std::thread::sleep(std::time::Duration::from_millis(sleep_ms as u64));
    }
}

fn wait_for_write_commit_ready(
    write: &Arc<Mutex<WriteCoordinator>>,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
    deadline: std::time::Instant,
    timeout_ms: i64,
    cancellation: &QueryCancellationView,
) -> Result<WriteCommitInput, String> {
    const WRITE_COMMIT_POLL_INTERVAL_MS: i64 = 10;

    loop {
        poll_write_failure_and_cancel(write, tracker, dispatcher)?;

        if cancellation.is_cancelled() {
            tracker.cancel_all(dispatcher);
            return Err("client disconnected".to_string());
        }

        let commit_error = match validate_write_commit_ready(write) {
            Ok(commit) => return Ok(commit),
            Err(e) => e,
        };
        #[cfg(test)]
        notify_write_commit_wait_observer(&commit_error);

        let now = std::time::Instant::now();
        if now >= deadline {
            let reason = format!(
                "query timed out after {timeout_ms} ms waiting for write final reports: {commit_error}"
            );
            tracker.cancel_all(dispatcher);
            write
                .lock()
                .expect("write coordinator lock")
                .mark_canceled_except_finished(reason.clone());
            return Err(reason);
        }

        let remaining_ms = deadline
            .saturating_duration_since(now)
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let sleep_ms = remaining_ms.clamp(1, WRITE_COMMIT_POLL_INTERVAL_MS);
        std::thread::sleep(std::time::Duration::from_millis(sleep_ms as u64));
    }
}

#[cfg(test)]
struct WriteCommitWaitObserverGuard;

#[cfg(test)]
struct WriteCommitWaitObserver {
    expected_error_substring: String,
    tx: std::sync::mpsc::Sender<String>,
}

#[cfg(test)]
impl Drop for WriteCommitWaitObserverGuard {
    fn drop(&mut self) {
        *write_commit_wait_observer()
            .lock()
            .expect("write commit wait observer lock") = None;
    }
}

#[cfg(test)]
fn write_commit_wait_observer() -> &'static Mutex<Option<WriteCommitWaitObserver>> {
    static OBSERVER: std::sync::OnceLock<Mutex<Option<WriteCommitWaitObserver>>> =
        std::sync::OnceLock::new();
    OBSERVER.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn set_write_commit_wait_observer(
    expected_error_substring: impl Into<String>,
    tx: std::sync::mpsc::Sender<String>,
) -> WriteCommitWaitObserverGuard {
    let mut observer = write_commit_wait_observer()
        .lock()
        .expect("write commit wait observer lock");
    assert!(
        observer.is_none(),
        "write commit wait observer already registered"
    );
    *observer = Some(WriteCommitWaitObserver {
        expected_error_substring: expected_error_substring.into(),
        tx,
    });
    WriteCommitWaitObserverGuard
}

#[cfg(test)]
fn notify_write_commit_wait_observer(commit_error: &str) {
    let observer = write_commit_wait_observer()
        .lock()
        .expect("write commit wait observer lock");
    if let Some(observer) = observer.as_ref()
        && commit_error.contains(&observer.expected_error_substring)
    {
        let _ = observer.tx.send(commit_error.to_string());
    }
}

#[cfg(test)]
fn apply_scheduled_plan_test_drift(
    mut plan: SchedulingPlan,
    drift: Option<ScheduledPlanTestDrift>,
) -> SchedulingPlan {
    match drift {
        None => {}
        Some(ScheduledPlanTestDrift::Missing) => {
            plan.by_fragment.remove(&plan.root_fragment_id);
        }
        Some(ScheduledPlanTestDrift::Unknown) => {
            let mut placement = plan
                .by_fragment
                .values()
                .flat_map(|placements| placements.iter())
                .next()
                .cloned()
                .expect("real scheduler must produce an anchor placement");
            placement.fragment_id = 99;
            plan.by_fragment.insert(99, vec![placement]);
        }
    }
    plan
}

#[cfg(test)]
fn apply_post_install_assembly_test_drift(
    mut plan: SchedulingPlan,
    drift: Option<PostInstallAssemblyTestDrift>,
) -> SchedulingPlan {
    if matches!(
        drift,
        Some(PostInstallAssemblyTestDrift::MissingRootPlacement)
    ) {
        plan.by_fragment.remove(&plan.root_fragment_id);
    }
    plan
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod native_contract_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Array, Decimal128Array, Int32Array};

    use crate::coordinator::ports::CoordinatorObserver;
    use crate::proto::plan as native_plan;
    use crate::query_execution::write::FragmentExecStatusReport;
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    #[derive(Default)]
    struct CountingCoordinatorObserver(AtomicUsize);

    #[test]
    fn standalone_query_ids_do_not_repeat() {
        let first = next_standalone_query_id();
        let second = next_standalone_query_id();

        assert_ne!(first.hi, second.hi);
        assert_ne!(first.lo, second.lo);
    }

    impl CoordinatorObserver for CountingCoordinatorObserver {
        fn fragment_scheduled(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn real_execution_artifacts() -> (PreparedFragmentSet, NativeFragmentBundle) {
        let fragment = PlanFragment {
            fragment_id: 7,
            root: DistributedNode {
                node_id: 70,
                fragment_id: 7,
                tuple_ids: vec![70],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: PhysicalPlanStats {
                    output_row_count: 0.0,
                    row_count_confidence: PlannerConfidence::Fallback,
                    column_statistics: Default::default(),
                    cost_estimate: None,
                    broadcast_decision: None,
                },
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
            fragments: vec![fragment],
            root_fragment_id: 7,
            edges: Vec::new(),
            runtime_filter_graph: Default::default(),
        };
        let prepared = crate::query_execution::preparation::prepare_fragments(
            &plan,
            &crate::connector::ConnectorRegistry::new(),
            None,
        )
        .expect("prepare production execution artifact");
        let native_bundle =
            crate::protocol::native::encode::encode_native_fragment_bundle(&plan, &prepared)
                .expect("encode production execution artifact");
        (prepared, native_bundle)
    }

    fn coordinator_for_artifact_test(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        scheduler: Arc<FragmentScheduler>,
        scheduled_plan_test_drift: Option<ScheduledPlanTestDrift>,
        dispatcher: Arc<CapturingDispatcher>,
    ) -> ExecutionCoordinator {
        coordinator_for_artifact_test_with_query_options(
            prepared,
            native_bundle,
            scheduler,
            scheduled_plan_test_drift,
            dispatcher,
            None,
        )
    }

    fn coordinator_for_artifact_test_with_query_options(
        prepared: PreparedFragmentSet,
        native_bundle: NativeFragmentBundle,
        scheduler: Arc<FragmentScheduler>,
        scheduled_plan_test_drift: Option<ScheduledPlanTestDrift>,
        dispatcher: Arc<CapturingDispatcher>,
        query_options: Option<QueryOptions>,
    ) -> ExecutionCoordinator {
        let mut coordinator = ExecutionCoordinator::new(
            prepared,
            native_bundle,
            CoordinatorExecutionPorts::new(
                dispatcher,
                crate::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 9030)
                    .expect("report endpoint"),
                Arc::new(CountingCoordinatorObserver::default()),
                Arc::new(crate::coordinator::ports::RejectingTestRuntimeFilterDeploymentControl),
            ),
            scheduler,
            query_options,
            QueryCancellationView::never_cancelled(),
        );
        coordinator.scheduled_plan_test_drift = scheduled_plan_test_drift;
        coordinator
    }

    fn prepared(fragment_ids: &[FragmentId]) -> PreparedFragmentSet {
        crate::query_execution::preparation::prepared_fragment_set_for_test(
            fragment_ids
                .iter()
                .map(|&fragment_id| (fragment_id, PreparedFragmentRole::Result, Vec::new()))
                .collect(),
            fragment_ids.to_vec(),
            *fragment_ids.last().expect("prepared test fragment"),
            Vec::new(),
        )
    }

    fn placement(fragment_id: FragmentId, instance_lo: i64) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index: 0,
            finst_id: UniqueId {
                hi: 92_000,
                lo: instance_lo,
            },
            backend_idx: 0,
            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    fn router_edge(
        group_id: i32,
        branch_id: i32,
        branch_kind: crate::sql::common::ChangeStreamBranchKind,
        target_fragment_id: FragmentId,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id,
            target_exchange_node_id: 70 + target_fragment_id as i32,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id: group_id,
                branch_id,
                branch_kind,
            },
            output_slot_ids: vec![10],
        }
    }

    fn router_route(
        branch_id: i32,
        branch_kind: native_plan::ChangeStreamBranchKind,
    ) -> native_plan::IcebergChangeStreamBranchRoute {
        native_plan::IcebergChangeStreamBranchRoute {
            branch_id,
            branch_kind: branch_kind as i32,
            target_fragment_id: 0,
            target_exchange_node_id: -1,
            output_ordinals: vec![0],
            output_partition_ordinals: Vec::new(),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            destinations: None,
        }
    }

    fn router_fragment(
        group_id: i32,
        branches: Vec<native_plan::IcebergChangeStreamBranchRoute>,
    ) -> native_plan::PlanFragment {
        native_plan::PlanFragment {
            fragment_id: 1,
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(
                    native_plan::IcebergChangeStreamRouterSink {
                        group_id,
                        change_op_output_ordinal: 0,
                        data_route_output_ordinal: None,
                        branches,
                    },
                )),
            }),
            ..Default::default()
        }
    }

    fn router_branches_mut(
        fragment: &mut native_plan::PlanFragment,
    ) -> &mut Vec<native_plan::IcebergChangeStreamBranchRoute> {
        let Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(router)) =
            fragment.sink.as_mut().and_then(|sink| sink.kind.as_mut())
        else {
            panic!("router sink");
        };
        &mut router.branches
    }

    fn assert_router_rejected_without_mutation(
        mut fragment: native_plan::PlanFragment,
        edges: Vec<FragmentEdge>,
        expected_error: &str,
    ) {
        let before = fragment.clone();
        let edge_refs: Vec<&FragmentEdge> = edges.iter().collect();
        let placements = BTreeMap::from([(2, vec![placement(2, 2)]), (3, vec![placement(3, 3)])]);

        let err = patch_native_iceberg_change_stream_router_sink(
            &mut fragment,
            1,
            7,
            &edge_refs,
            &placements,
        )
        .expect_err("router contract drift must fail");

        assert!(err.contains(expected_error), "{err}");
        assert_eq!(fragment, before, "router validation must precede patching");
    }

    fn submission(
        fragment_id: FragmentId,
        query_id: UniqueId,
        fragment_instance_id: UniqueId,
    ) -> NativeFragmentEnvelope {
        NativeFragmentEnvelope::new(
            native_plan::PlanFragment {
                fragment_id,
                ..Default::default()
            },
            crate::proto::novarocks::InstanceParams {
                query_id: Some(crate::proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                fragment_instance_id: Some(crate::proto::common::UniqueId {
                    hi: fragment_instance_id.hi,
                    lo: fragment_instance_id.lo,
                }),
                ..Default::default()
            },
        )
    }

    fn submission_with_optional_query_id(
        fragment_id: FragmentId,
        query_id: Option<UniqueId>,
        fragment_instance_id: UniqueId,
    ) -> NativeFragmentEnvelope {
        NativeFragmentEnvelope::new(
            native_plan::PlanFragment {
                fragment_id,
                ..Default::default()
            },
            crate::proto::novarocks::InstanceParams {
                query_id: query_id.map(|id| crate::proto::common::UniqueId {
                    hi: id.hi,
                    lo: id.lo,
                }),
                fragment_instance_id: Some(crate::proto::common::UniqueId {
                    hi: fragment_instance_id.hi,
                    lo: fragment_instance_id.lo,
                }),
                ..Default::default()
            },
        )
    }

    struct CapturingDispatcher {
        submissions: Mutex<Vec<(usize, FragmentId, UniqueId)>>,
        query_options: Mutex<Vec<Option<crate::proto::novarocks::QueryOptions>>>,
        submit_count: AtomicUsize,
        fail_on_submit: Option<usize>,
        backend_loss_on_submit: Option<(usize, crate::coordinator::cluster::BeId)>,
        cancellations: Mutex<Vec<(usize, Vec<UniqueId>)>>,
        fetch_behavior: TestFetchBehavior,
        fetch_count: AtomicUsize,
        first_fetch: std::sync::atomic::AtomicBool,
    }

    enum TestFetchBehavior {
        Eof,
        Error(String),
        NotReady,
        QueryStateFailure(String),
        EofWithProfiles(Vec<crate::proto::novarocks::ExecStatusReport>),
    }

    impl CapturingDispatcher {
        fn new(fail_on_submit: Option<usize>) -> Arc<Self> {
            Self::with_fetch(fail_on_submit, TestFetchBehavior::Eof)
        }

        fn with_backend_loss_on_submit(
            call: usize,
            be_id: crate::coordinator::cluster::BeId,
        ) -> Arc<Self> {
            Arc::new(Self {
                submissions: Mutex::new(Vec::new()),
                query_options: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fail_on_submit: None,
                backend_loss_on_submit: Some((call, be_id)),
                cancellations: Mutex::new(Vec::new()),
                fetch_behavior: TestFetchBehavior::Eof,
                fetch_count: AtomicUsize::new(0),
                first_fetch: std::sync::atomic::AtomicBool::new(true),
            })
        }

        fn with_fetch(
            fail_on_submit: Option<usize>,
            fetch_behavior: TestFetchBehavior,
        ) -> Arc<Self> {
            Arc::new(Self {
                submissions: Mutex::new(Vec::new()),
                query_options: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fail_on_submit,
                backend_loss_on_submit: None,
                cancellations: Mutex::new(Vec::new()),
                fetch_behavior,
                fetch_count: AtomicUsize::new(0),
                first_fetch: std::sync::atomic::AtomicBool::new(true),
            })
        }
    }

    impl FragmentDispatcher for CapturingDispatcher {
        fn submit_fragment(
            &self,
            backend_idx: usize,
            submission: NativeFragmentEnvelope,
        ) -> Result<(), String> {
            let call = self.submit_count.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_on_submit == Some(call) {
                return Err(format!("native submit failed on call {call}"));
            }
            if let Some((failure_call, be_id)) = self.backend_loss_on_submit
                && failure_call == call
            {
                crate::coordinator::cluster::RegistryEventSink::on_event(
                    &crate::coordinator::cluster::QueryCleanupSink::new(),
                    crate::coordinator::cluster::RegistryEvent::BackendLost { be_id },
                );
            }
            let finst_id = submission.fragment_instance_id()?;
            let query_options = submission.instance_params_for_test().query_options.clone();
            self.submissions.lock().unwrap().push((
                backend_idx,
                submission.plan_for_test().fragment_id,
                finst_id,
            ));
            self.query_options.lock().unwrap().push(query_options);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_chunk_schema: Option<&ChunkSchemaRef>,
        ) -> Result<FetchOutcome, String> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            match &self.fetch_behavior {
                TestFetchBehavior::Eof => Ok(FetchOutcome::Eof),
                TestFetchBehavior::Error(message) => Ok(FetchOutcome::Err(message.clone())),
                TestFetchBehavior::NotReady => Ok(FetchOutcome::NotReady),
                TestFetchBehavior::QueryStateFailure(message) => {
                    if self.first_fetch.swap(false, Ordering::SeqCst) {
                        crate::runtime::query_state::in_flight_table()
                            .on_fragment_done(finst_id, Err(message.clone()));
                        Ok(FetchOutcome::NotReady)
                    } else {
                        panic!("query-state failure must be observed before another fetch")
                    }
                }
                TestFetchBehavior::EofWithProfiles(reports) => {
                    if self.first_fetch.swap(false, Ordering::SeqCst) {
                        for report in reports {
                            assert!(
                                record_native_standalone_query_profile_report(report)
                                    .expect("record native profile report")
                            );
                        }
                    }
                    Ok(FetchOutcome::Eof)
                }
            }
        }

        fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[UniqueId]) {
            self.cancellations
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            2
        }
    }

    fn writer_key(
        query_id: UniqueId,
        fragment_instance_id: UniqueId,
        backend_num: i32,
    ) -> WriterKey {
        WriterKey {
            query_id,
            fragment_instance_id,
            backend_num,
        }
    }

    fn write_report(
        writer: &WriterKey,
        status: crate::proto::common::Status,
        path: Option<&str>,
    ) -> FragmentExecStatusReport {
        let iceberg_commits = path
            .map(|path| {
                vec![crate::proto::novarocks::IcebergCommitInfo {
                    iceberg_data_file: Some(crate::proto::novarocks::IcebergDataFile {
                        path: Some(path.to_string()),
                        record_count: Some(7),
                        file_size_in_bytes: Some(70),
                        file_content: crate::proto::novarocks::IcebergFileContent::Data as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }]
            })
            .unwrap_or_default();
        FragmentExecStatusReport {
            query_id: writer.query_id,
            fragment_instance_id: writer.fragment_instance_id,
            backend_num: writer.backend_num,
            done: true,
            status,
            iceberg_commits,
            load_counters: BTreeMap::new(),
            loaded_rows: 7,
            loaded_bytes: 70,
            filtered_rows: 0,
        }
    }

    fn ok_status() -> crate::proto::common::Status {
        crate::proto::common::Status {
            code: 0,
            message: String::new(),
        }
    }

    fn err_status(message: &str) -> crate::proto::common::Status {
        crate::proto::common::Status {
            code: 1,
            message: message.to_string(),
        }
    }

    #[test]
    fn native_fragment_sink_support_allows_dynamic_stream_sink() {
        ensure_native_fragment_sink_supported(7, false, false, true, false, false)
            .expect("native execution carries a DATA_STREAM_SINK fragment payload");
    }

    #[test]
    fn native_fragment_sink_support_allows_router_and_cte_sinks() {
        ensure_native_fragment_sink_supported(8, false, false, false, true, false)
            .expect("native execution patches ICEBERG_CHANGE_STREAM_ROUTER_SINK fragments");

        ensure_native_fragment_sink_supported(9, false, false, false, false, true)
            .expect("native execution patches MULTI_CAST_DATA_STREAM_SINK fragments");
    }

    #[test]
    fn native_output_kind_validation_is_exhaustive() {
        validate_fragment_output_kind(1, true, false, false, PreparedFragmentRole::Result)
            .expect("result root");
        validate_fragment_output_kind(1, true, true, false, PreparedFragmentRole::TerminalWrite)
            .expect("write-only root");
        let err =
            validate_fragment_output_kind(1, true, false, false, PreparedFragmentRole::NonTerminal)
                .expect_err("root cannot be nonterminal");
        assert!(err.contains("root fragment 1"), "{err}");

        validate_fragment_output_kind(2, false, false, true, PreparedFragmentRole::NonTerminal)
            .expect("non-root producer");
        for output_kind in [
            PreparedFragmentRole::Result,
            PreparedFragmentRole::TerminalWrite,
        ] {
            let err = validate_fragment_output_kind(2, false, false, true, output_kind)
                .expect_err("producer must be nonterminal");
            assert!(err.contains("producer fragment 2"), "{err}");
        }

        validate_fragment_output_kind(3, false, true, false, PreparedFragmentRole::TerminalWrite)
            .expect("non-root terminal writer");
        for output_kind in [
            PreparedFragmentRole::Result,
            PreparedFragmentRole::NonTerminal,
        ] {
            let err = validate_fragment_output_kind(3, false, true, false, output_kind)
                .expect_err("terminal writer must use terminal output kind");
            assert!(err.contains("terminal write fragment 3"), "{err}");
        }
    }

    fn assert_native_bundle_drift_rejected(
        drift: crate::protocol::native::encode::NativeBundleTestDrift,
        expected_error: &str,
    ) {
        let (prepared, native_bundle) = real_execution_artifacts();
        let native_bundle =
            crate::protocol::native::encode::corrupt_native_fragment_bundle_for_execution_test(
                native_bundle,
                drift,
            );
        let inner = CapturingDispatcher::new(None);
        let scheduler = Arc::new(FragmentScheduler::new(vec![std::net::SocketAddr::from((
            [127, 0, 0, 1],
            9030,
        ))]));
        let error =
            coordinator_for_artifact_test(prepared, native_bundle, scheduler, None, inner.clone())
                .execute_with_write_outcome()
                .expect_err("native bundle drift must fail before dispatch");
        assert!(error.contains(expected_error), "{error}");
        assert_eq!(inner.submit_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn coordinator_rejects_missing_native_fragment_before_dispatch() {
        assert_native_bundle_drift_rejected(
            crate::protocol::native::encode::NativeBundleTestDrift::Missing(7),
            "missing prepared fragment id=7",
        );
    }

    #[test]
    fn coordinator_rejects_unknown_native_fragment_before_dispatch() {
        assert_native_bundle_drift_rejected(
            crate::protocol::native::encode::NativeBundleTestDrift::Unknown(99),
            "native fragment ids mismatch",
        );
    }

    #[test]
    fn normal_coordinator_encodes_positive_query_options_and_preserves_explicit_override() {
        let scheduler = || {
            Arc::new(FragmentScheduler::new(vec![std::net::SocketAddr::from((
                [127, 0, 0, 1],
                9030,
            ))]))
        };

        let (prepared, native_bundle) = real_execution_artifacts();
        let auto_dispatcher = CapturingDispatcher::new(None);
        coordinator_for_artifact_test_with_query_options(
            prepared,
            native_bundle,
            scheduler(),
            None,
            auto_dispatcher.clone(),
            None,
        )
        .execute_with_write_outcome()
        .expect("normal coordinator with auto DOP");
        let auto_options = auto_dispatcher.query_options.lock().unwrap();
        let auto_options = auto_options[0]
            .as_ref()
            .expect("normal coordinator must encode query options");
        assert_eq!(
            auto_options.pipeline_dop,
            crate::runtime::exec_env::calc_pipeline_dop(0)
        );
        assert!(auto_options.pipeline_dop > 0);

        let explicit = QueryOptions {
            pipeline_dop: Some(7),
            query_timeout: Some(41),
            enable_profile: true,
            group_concat_max_len: Some(65_535),
            ..Default::default()
        };
        let (prepared, native_bundle) = real_execution_artifacts();
        let explicit_dispatcher = CapturingDispatcher::new(None);
        coordinator_for_artifact_test_with_query_options(
            prepared,
            native_bundle,
            scheduler(),
            None,
            explicit_dispatcher.clone(),
            Some(explicit),
        )
        .execute_with_write_outcome()
        .expect("normal coordinator with explicit DOP");
        let explicit_options = explicit_dispatcher.query_options.lock().unwrap();
        let explicit_options = explicit_options[0]
            .as_ref()
            .expect("normal coordinator must encode explicit query options");
        assert_eq!(explicit_options.pipeline_dop, 7);
        assert_eq!(explicit_options.query_timeout, 41);
        assert!(explicit_options.enable_profile);
        assert_eq!(explicit_options.group_concat_max_len, Some(65_535));
    }

    fn assert_scheduled_drift_rejected(drift: ScheduledPlanTestDrift, expected_error: &str) {
        let (prepared, native_bundle) = real_execution_artifacts();
        let inner = CapturingDispatcher::new(None);
        let scheduler = Arc::new(FragmentScheduler::new(vec![std::net::SocketAddr::from((
            [127, 0, 0, 1],
            9030,
        ))]));
        let error = coordinator_for_artifact_test(
            prepared,
            native_bundle,
            scheduler,
            Some(drift),
            inner.clone(),
        )
        .execute_with_write_outcome()
        .expect_err("scheduled drift must fail before dispatch");
        assert!(error.contains(expected_error), "{error}");
        assert_eq!(inner.submit_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn coordinator_rejects_missing_scheduled_fragment_before_dispatch() {
        assert_scheduled_drift_rejected(ScheduledPlanTestDrift::Missing, "missing=[7]");
    }

    #[test]
    fn coordinator_rejects_unknown_scheduled_fragment_before_dispatch() {
        assert_scheduled_drift_rejected(ScheduledPlanTestDrift::Unknown, "unknown=[99]");
    }

    #[test]
    fn native_scheduling_plan_validation_rejects_empty_non_root_placements() {
        let plan = crate::coordinator::scheduler::SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(3, Vec::new()), (7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId { hi: 92_000, lo: 7 },
            root_backend_idx: 0,
        };
        let mut side_effects = 0;

        let err = validate_scheduling_placements(&plan)
            .map(|()| side_effects += 1)
            .expect_err("empty non-root placements must fail");

        assert!(err.contains("fragment 3 has no placements"), "{err}");
        assert_eq!(side_effects, 0);
    }

    #[test]
    fn native_scheduling_plan_validation_rejects_placement_fragment_id_drift() {
        let plan = crate::coordinator::scheduler::SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(8, 7)])]),
            root_finst_id: UniqueId { hi: 92_000, lo: 7 },
            root_backend_idx: 0,
        };
        let mut side_effects = 0;

        let err = validate_scheduling_placements(&plan)
            .map(|()| side_effects += 1)
            .expect_err("placement fragment id drift must fail");

        assert!(err.contains("map key 7"), "{err}");
        assert!(err.contains("fragment_id 8"), "{err}");
        assert_eq!(side_effects, 0);
    }

    #[test]
    fn submit_loop_uses_native_submission_in_default_and_compat() {
        let inner = CapturingDispatcher::new(None);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let observer = CountingCoordinatorObserver::default();
        let query_id = UniqueId {
            hi: 91_000,
            lo: 91_001,
        };
        let root_finst_id = UniqueId { hi: 91_000, lo: 2 };
        let mut tracker = InFlightTracker::default();

        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (1, submission(3, query_id, UniqueId { hi: 91_000, lo: 1 })),
                (0, submission(7, query_id, UniqueId { hi: 91_000, lo: 2 })),
            ],
            7,
            0,
            root_finst_id,
            &query_id,
            true,
            1_000,
            None,
            None,
            false,
            &observer,
            &QueryCancellationView::never_cancelled(),
        )
        .expect("native submissions execute");

        assert!(result.chunks.is_empty());
        assert_eq!(
            *inner.submissions.lock().unwrap(),
            vec![
                (1, 3, UniqueId { hi: 91_000, lo: 1 }),
                (0, 7, UniqueId { hi: 91_000, lo: 2 }),
            ]
        );
        assert_eq!(observer.0.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn submit_failure_cancels_accepted_and_unknown_outcome_native_instances() {
        let inner = CapturingDispatcher::new(Some(2));
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let observer = CountingCoordinatorObserver::default();
        let query_id = UniqueId {
            hi: 92_000,
            lo: 92_001,
        };
        let mut tracker = InFlightTracker::default();

        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (1, submission(3, query_id, UniqueId { hi: 92_000, lo: 1 })),
                (0, submission(7, query_id, UniqueId { hi: 92_000, lo: 2 })),
            ],
            7,
            0,
            UniqueId { hi: 92_000, lo: 2 },
            &query_id,
            true,
            1_000,
            None,
            None,
            false,
            &observer,
            &QueryCancellationView::never_cancelled(),
        )
        .expect_err("second native submit must fail");

        assert!(err.contains("native submit failed on call 2"), "{err}");
        assert_eq!(
            *inner.cancellations.lock().unwrap(),
            vec![
                (0, vec![UniqueId { hi: 92_000, lo: 2 }]),
                (1, vec![UniqueId { hi: 92_000, lo: 1 }]),
            ]
        );
        assert_eq!(observer.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn backend_loss_during_submit_observes_preregistered_query_mapping() {
        let inner = CapturingDispatcher::with_backend_loss_on_submit(1, 1);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let query_id = UniqueId {
            hi: 93_000,
            lo: 93_001,
        };
        let first_finst_id = UniqueId { hi: 93_000, lo: 1 };
        let root_finst_id = UniqueId { hi: 93_000, lo: 2 };
        let runtime_query_id = crate::runtime::query_context::QueryId {
            hi: query_id.hi,
            lo: query_id.lo,
        };
        let mut tracker = InFlightTracker::default();

        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (1, submission(3, query_id, first_finst_id.clone())),
                (0, submission(7, query_id, root_finst_id)),
            ],
            7,
            0,
            UniqueId { hi: 93_000, lo: 2 },
            &query_id,
            true,
            1_000,
            None,
            None,
            false,
            &CountingCoordinatorObserver::default(),
            &QueryCancellationView::never_cancelled(),
        )
        .expect_err("backend loss during submit must fail the mapped query");

        assert_eq!(err, "backend 1 lost");
        assert_eq!(inner.submit_count.load(Ordering::SeqCst), 1);
        assert_eq!(inner.fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(
            *inner.cancellations.lock().unwrap(),
            vec![(1, vec![first_finst_id])]
        );
        assert_eq!(
            crate::runtime::query_state::in_flight_table().state(runtime_query_id),
            None,
            "query registration guard must remove provisional mappings"
        );
    }

    #[test]
    fn submission_ids_are_prevalidated_before_any_dispatch() {
        let inner = CapturingDispatcher::new(None);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let query_id = UniqueId {
            hi: 94_000,
            lo: 94_001,
        };
        let malformed = NativeFragmentEnvelope::new(
            native_plan::PlanFragment {
                fragment_id: 7,
                ..Default::default()
            },
            crate::proto::novarocks::InstanceParams {
                query_id: Some(crate::proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                ..Default::default()
            },
        );
        let mut tracker = InFlightTracker::default();

        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (1, submission(3, query_id, UniqueId { hi: 94_000, lo: 1 })),
                (0, malformed),
            ],
            7,
            0,
            UniqueId { hi: 94_000, lo: 2 },
            &query_id,
            true,
            1_000,
            None,
            None,
            false,
            &CountingCoordinatorObserver::default(),
            &QueryCancellationView::never_cancelled(),
        )
        .expect_err("malformed later submission must fail before dispatch");

        assert!(err.contains("missing fragment_instance_id"), "{err}");
        assert!(inner.submissions.lock().unwrap().is_empty());
        assert!(inner.cancellations.lock().unwrap().is_empty());
        assert!(tracker.by_backend.is_empty());
    }

    #[test]
    fn duplicate_and_zero_submission_ids_fail_before_dispatch() {
        for (submissions, expected) in [
            (
                vec![
                    (
                        1,
                        submission(
                            3,
                            UniqueId {
                                hi: 95_000,
                                lo: 95_001,
                            },
                            UniqueId { hi: 95_000, lo: 1 },
                        ),
                    ),
                    (
                        0,
                        submission(
                            7,
                            UniqueId {
                                hi: 95_000,
                                lo: 95_001,
                            },
                            UniqueId { hi: 95_000, lo: 1 },
                        ),
                    ),
                ],
                "duplicate fragment_instance_id",
            ),
            (
                vec![(
                    0,
                    submission(
                        7,
                        UniqueId {
                            hi: 95_000,
                            lo: 95_001,
                        },
                        UniqueId { hi: 0, lo: 0 },
                    ),
                )],
                "zero fragment_instance_id",
            ),
        ] {
            let inner = CapturingDispatcher::new(None);
            let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
            let query_id = UniqueId {
                hi: 95_000,
                lo: 95_001,
            };
            let mut tracker = InFlightTracker::default();
            let err = submit_and_fetch_loop(
                &dispatcher,
                &mut tracker,
                submissions,
                7,
                0,
                UniqueId { hi: 95_000, lo: 99 },
                &query_id,
                true,
                1_000,
                None,
                None,
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
            )
            .expect_err("invalid submission ids must fail");
            assert!(err.contains(expected), "{err}");
            assert!(inner.submissions.lock().unwrap().is_empty());
            assert!(tracker.by_backend.is_empty());
        }
    }

    #[test]
    fn submission_query_ids_are_prevalidated_before_any_dispatch() {
        let expected_query_id = UniqueId {
            hi: 95_500,
            lo: 95_501,
        };
        let invalid_query_ids = [
            (None, "missing query_id"),
            (Some(UniqueId { hi: 0, lo: 0 }), "zero query_id"),
            (
                Some(UniqueId {
                    hi: 95_500,
                    lo: 95_599,
                }),
                "query_id mismatch",
            ),
        ];

        for (invalid_query_id, expected_error) in invalid_query_ids {
            for prepend_valid_submission in [false, true] {
                let invalid_finst_id = UniqueId { hi: 95_500, lo: 2 };
                let invalid =
                    submission_with_optional_query_id(7, invalid_query_id, invalid_finst_id);
                let mut submissions = Vec::new();
                if prepend_valid_submission {
                    submissions.push((
                        1,
                        submission(3, expected_query_id, UniqueId { hi: 95_500, lo: 1 }),
                    ));
                }
                submissions.push((0, invalid));

                let inner = CapturingDispatcher::new(None);
                let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
                let mut tracker = InFlightTracker::default();
                let err = submit_and_fetch_loop(
                    &dispatcher,
                    &mut tracker,
                    submissions,
                    7,
                    0,
                    invalid_finst_id,
                    &expected_query_id,
                    true,
                    1_000,
                    None,
                    None,
                    false,
                    &CountingCoordinatorObserver::default(),
                    &QueryCancellationView::never_cancelled(),
                )
                .expect_err("invalid submission query id must fail before dispatch");

                assert!(err.contains(expected_error), "{err}");
                let expected_index = usize::from(prepend_valid_submission);
                assert!(
                    err.contains(&format!("fragment submission {expected_index}")),
                    "{err}"
                );
                assert!(err.contains("fragment_id=7"), "{err}");
                assert!(err.contains("fragment_instance_id=95500/2"), "{err}");
                assert!(inner.submissions.lock().unwrap().is_empty());
                assert!(inner.cancellations.lock().unwrap().is_empty());
                assert!(tracker.by_backend.is_empty());
            }
        }
    }

    #[test]
    fn root_submission_fragment_and_backend_are_prevalidated_before_dispatch() {
        let query_id = UniqueId {
            hi: 95_600,
            lo: 95_601,
        };
        let root_finst_id = UniqueId { hi: 95_600, lo: 2 };
        let cases = [
            (
                vec![
                    (0, submission(7, query_id, UniqueId { hi: 95_600, lo: 1 })),
                    (0, submission(3, query_id, root_finst_id)),
                ],
                "got fragment_id=3 backend=0",
            ),
            (
                vec![
                    (1, submission(3, query_id, UniqueId { hi: 95_600, lo: 1 })),
                    (1, submission(7, query_id, root_finst_id)),
                ],
                "got fragment_id=7 backend=1",
            ),
        ];

        for (submissions, got_context) in cases {
            let inner = CapturingDispatcher::new(None);
            let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
            let mut tracker = InFlightTracker::default();
            let err = submit_and_fetch_loop(
                &dispatcher,
                &mut tracker,
                submissions,
                7,
                0,
                root_finst_id,
                &query_id,
                true,
                1_000,
                None,
                None,
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
            )
            .expect_err("root submission identity drift must fail before dispatch");

            assert!(err.contains("expected fragment_id=7 backend=0"), "{err}");
            assert!(err.contains(got_context), "{err}");
            assert!(err.contains("fragment_instance_id=95600/2"), "{err}");
            assert!(inner.submissions.lock().unwrap().is_empty());
            assert!(inner.cancellations.lock().unwrap().is_empty());
            assert!(tracker.by_backend.is_empty());
        }
    }

    #[test]
    fn fetch_error_timeout_query_failure_and_disconnect_cancel_all_native_submissions() {
        let cases = [
            (
                TestFetchBehavior::Error("native fetch failed".to_string()),
                1_000,
                "native fetch failed",
            ),
            (TestFetchBehavior::NotReady, 0, "query timed out"),
            (
                TestFetchBehavior::QueryStateFailure("remote fragment failed".to_string()),
                1_000,
                "remote fragment failed",
            ),
        ];
        for (index, (behavior, timeout_ms, expected)) in cases.into_iter().enumerate() {
            let hi = 96_000 + index as i64;
            let inner = CapturingDispatcher::with_fetch(None, behavior);
            let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
            let mut tracker = InFlightTracker::default();
            let err = submit_and_fetch_loop(
                &dispatcher,
                &mut tracker,
                vec![
                    (
                        1,
                        submission(3, UniqueId { hi, lo: 99 }, UniqueId { hi, lo: 1 }),
                    ),
                    (
                        0,
                        submission(7, UniqueId { hi, lo: 99 }, UniqueId { hi, lo: 2 }),
                    ),
                ],
                7,
                0,
                UniqueId { hi, lo: 2 },
                &UniqueId { hi, lo: 99 },
                true,
                timeout_ms,
                None,
                None,
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
            )
            .expect_err("native lifecycle failure must surface");
            assert!(err.contains(expected), "{err}");
            let mut canceled = inner.cancellations.lock().unwrap().clone();
            canceled.sort_by_key(|(backend_idx, _)| *backend_idx);
            assert_eq!(
                canceled,
                vec![
                    (0, vec![UniqueId { hi, lo: 2 }]),
                    (1, vec![UniqueId { hi, lo: 1 }]),
                ]
            );
        }

        let hi = 96_100;
        let inner = CapturingDispatcher::with_fetch(None, TestFetchBehavior::NotReady);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let cancellation =
            crate::query_execution::cancellation::QueryCancellationView::new(disconnected);
        let mut tracker = InFlightTracker::default();
        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (
                    1,
                    submission(3, UniqueId { hi, lo: 99 }, UniqueId { hi, lo: 1 }),
                ),
                (
                    0,
                    submission(7, UniqueId { hi, lo: 99 }, UniqueId { hi, lo: 2 }),
                ),
            ],
            7,
            0,
            UniqueId { hi, lo: 2 },
            &UniqueId { hi, lo: 99 },
            true,
            1_000,
            None,
            None,
            false,
            &CountingCoordinatorObserver::default(),
            &cancellation,
        )
        .expect_err("disconnect must surface");
        assert!(err.contains("client disconnected"), "{err}");
        let mut canceled = inner.cancellations.lock().unwrap().clone();
        canceled.sort_by_key(|(backend_idx, _)| *backend_idx);
        assert_eq!(
            canceled,
            vec![
                (0, vec![UniqueId { hi, lo: 2 }]),
                (1, vec![UniqueId { hi, lo: 1 }]),
            ]
        );
    }

    #[test]
    fn write_failure_before_root_eof_surfaces_abort_without_fetching() {
        let query_id = UniqueId { hi: 97_000, lo: 99 };
        let finished = writer_key(query_id, UniqueId { hi: 97_000, lo: 10 }, 0);
        let failed = writer_key(query_id, UniqueId { hi: 97_000, lo: 11 }, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![finished.clone(), failed.clone()]).unwrap(),
        ));
        write
            .lock()
            .unwrap()
            .apply_report(write_report(
                &finished,
                ok_status(),
                Some("s3://warehouse/finished.parquet"),
            ))
            .unwrap();
        write
            .lock()
            .unwrap()
            .apply_report(write_report(
                &failed,
                err_status("writer failed before EOF"),
                None,
            ))
            .unwrap();
        let inner = CapturingDispatcher::with_fetch(None, TestFetchBehavior::NotReady);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![
                (1, submission(3, query_id, UniqueId { hi: 97_000, lo: 1 })),
                (0, submission(7, query_id, UniqueId { hi: 97_000, lo: 2 })),
            ],
            7,
            0,
            UniqueId { hi: 97_000, lo: 2 },
            &query_id,
            true,
            1_000,
            None,
            Some(&write),
            false,
            &CountingCoordinatorObserver::default(),
            &QueryCancellationView::never_cancelled(),
        )
        .expect("writer failure returns structured abort");
        assert!(result.write_commit.is_none());
        let abort = result.write_abort.expect("write abort");
        assert!(abort.reason.contains("writer failed before EOF"));
        assert_eq!(abort.completed_writer_outputs.len(), 1);
        assert_eq!(abort.incomplete_writers, vec![failed]);
        assert_eq!(inner.fetch_count.load(Ordering::SeqCst), 0);
        assert_eq!(inner.cancellations.lock().unwrap().len(), 2);
    }

    #[test]
    fn write_commit_and_abort_after_root_eof_preserve_native_lifecycle() {
        for (index, failure) in [false, true].into_iter().enumerate() {
            let hi = 97_100 + index as i64;
            let query_id = UniqueId { hi, lo: 99 };
            let writer = writer_key(query_id, UniqueId { hi, lo: 10 }, 0);
            let write = Arc::new(Mutex::new(
                WriteCoordinator::new(query_id, vec![writer.clone()]).unwrap(),
            ));
            let inner = CapturingDispatcher::new(None);
            let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
            let (wait_tx, wait_rx) = std::sync::mpsc::channel();
            let _observer = set_write_commit_wait_observer("missing writer final report", wait_tx);
            let write_for_report = Arc::clone(&write);
            let writer_for_report = writer.clone();
            let report_thread = std::thread::spawn(move || {
                wait_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("post-EOF write wait signal");
                let status = if failure {
                    err_status("writer failed after EOF")
                } else {
                    ok_status()
                };
                write_for_report
                    .lock()
                    .unwrap()
                    .apply_report(write_report(
                        &writer_for_report,
                        status,
                        (!failure).then_some("s3://warehouse/committed.parquet"),
                    ))
                    .unwrap();
            });
            let mut tracker = InFlightTracker::default();
            let result = submit_and_fetch_loop(
                &dispatcher,
                &mut tracker,
                vec![(0, submission(7, query_id, UniqueId { hi, lo: 2 }))],
                7,
                0,
                UniqueId { hi, lo: 2 },
                &query_id,
                true,
                1_000,
                None,
                Some(&write),
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
            )
            .expect("post-EOF write outcome");
            report_thread.join().unwrap();
            if failure {
                assert!(result.write_commit.is_none());
                assert!(
                    result
                        .write_abort
                        .as_ref()
                        .is_some_and(|abort| abort.reason.contains("writer failed after EOF"))
                );
                assert_eq!(inner.cancellations.lock().unwrap().len(), 1);
            } else {
                assert!(result.write_commit.is_some());
                assert!(result.write_abort.is_none());
                assert!(inner.cancellations.lock().unwrap().is_empty());
            }
        }
    }

    #[test]
    fn write_only_root_skips_fetch_and_waits_for_commit() {
        let query_id = UniqueId { hi: 97_200, lo: 99 };
        let writer = writer_key(query_id, UniqueId { hi: 97_200, lo: 2 }, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer.clone()]).unwrap(),
        ));
        write
            .lock()
            .unwrap()
            .apply_report(write_report(
                &writer,
                ok_status(),
                Some("s3://warehouse/write-only.parquet"),
            ))
            .unwrap();
        let inner = CapturingDispatcher::with_fetch(None, TestFetchBehavior::NotReady);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![(0, submission(7, query_id, UniqueId { hi: 97_200, lo: 2 }))],
            7,
            0,
            UniqueId { hi: 97_200, lo: 2 },
            &query_id,
            false,
            1_000,
            None,
            Some(&write),
            false,
            &CountingCoordinatorObserver::default(),
            &QueryCancellationView::never_cancelled(),
        )
        .expect("write-only root commits");
        assert!(result.write_commit.is_some());
        assert!(result.write_abort.is_none());
        assert_eq!(inner.fetch_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn native_profile_collection_deduplicates_fragment_reports() {
        let query_id = UniqueId { hi: 97_300, lo: 99 };
        let finst_id = UniqueId { hi: 97_300, lo: 2 };
        let report = crate::proto::novarocks::ExecStatusReport {
            query_id: Some(crate::proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            fragment_instance_id: Some(crate::proto::common::UniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            status: Some(ok_status()),
            done: true,
            profile: Some(crate::proto::novarocks::RuntimeProfileTree {
                root: Some(crate::proto::novarocks::ProfileNode {
                    name: "root".to_string(),
                    node_id: 7,
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let inner = CapturingDispatcher::with_fetch(
            None,
            TestFetchBehavior::EofWithProfiles(vec![report.clone(), report]),
        );
        let dispatcher: Arc<dyn FragmentDispatcher> = inner;
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            vec![(0, submission(7, query_id, finst_id))],
            7,
            0,
            finst_id,
            &query_id,
            true,
            1_000,
            None,
            None,
            true,
            &CountingCoordinatorObserver::default(),
            &QueryCancellationView::never_cancelled(),
        )
        .expect("native profiles collected");
        assert_eq!(result.fragment_profiles.len(), 1);
        assert_eq!(
            result
                .fragment_profiles
                .get(&finst_id)
                .expect("profile remains keyed by finst_id")
                .root
                .node_id,
            7
        );
    }

    #[test]
    fn typed_root_alignment_renames_fields_and_rejects_decimal_drift() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_i",
            DataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(7)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let aligned = align_fetch_chunks_to_output_columns(
            vec![chunk],
            &[PreparedOutputColumn {
                name: "col1".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
        )
        .unwrap();
        assert_eq!(aligned[0].batch.schema().field(0).name(), "col1");
        assert!(aligned[0].batch.schema().field(0).is_nullable());
        assert!(
            aligned[0]
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .is_null(1)
        );

        let decimal = Decimal128Array::from(vec![Some(100_i128)])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let decimal_schema = Arc::new(Schema::new(vec![Field::new(
            "wire_price",
            DataType::Decimal128(38, 2),
            false,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&decimal_schema), vec![Arc::new(decimal)]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            decimal_schema.as_ref(),
            &[SlotId::new(8)],
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let err = align_fetch_chunks_to_output_columns(
            vec![chunk],
            &[PreparedOutputColumn {
                name: "price".to_string(),
                data_type: DataType::Decimal128(20, 2),
                nullable: false,
            }],
        )
        .expect_err("decimal precision drift must fail");
        assert!(err.contains("type mismatch"), "{err}");
    }

    #[test]
    fn native_cte_multicast_patch_uses_source_root_output_slots() {
        use crate::proto::{common, expr};

        let bigint = common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Bigint as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        };
        let mut fragment = native_plan::PlanFragment {
            fragment_id: 1,
            root: Some(native_plan::DistributedNode {
                node_id: 5,
                fragment_id: 1,
                tuple_ids: Vec::new(),
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                payload: Some(native_plan::distributed_node::Payload::Physical(
                    native_plan::PlanNode {
                        output_columns: Vec::new(),
                        kind: Some(native_plan::plan_node::Kind::Project(
                            native_plan::ProjectNode {
                                items: vec![native_plan::ProjectItem {
                                    expr: Some(expr::Expr {
                                        r#type: Some(bigint.clone()),
                                        nullable: true,
                                        kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                                            column_id: 20,
                                            qualifier: None,
                                            column: Some("sum(income)".to_string()),
                                        })),
                                    }),
                                    output_name: "total".to_string(),
                                    output_column_id: 10,
                                }],
                                output_qualifier: None,
                            },
                        )),
                    },
                )),
            }),
            data_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            output_partition: Some(native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            // The CTE producer is a `DataSink::Noop` fragment, so the planner seal
            // adopts its Project root's wire output into `output_columns` (CGO-9C
            // Task 2). The coordinator reads that sealed contract directly, so the
            // fixture carries it explicitly instead of relying on a re-walk of the
            // encoded root.
            output_columns: vec![common::OutputColumn {
                column_id: 10,
                name: "total".to_string(),
                r#type: Some(bigint),
                nullable: true,
                is_internal: false,
            }],
            cte_id: Some(3),
            ..Default::default()
        };
        let consumers = vec![(
            2,
            77,
            native_plan::DataPartition {
                kind: native_plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            },
            vec![13],
            vec![ColumnId::new_for_test(13)],
        )];
        let destination = crate::runtime::endpoint::FragmentDestination::new(
            UniqueId { hi: 98_000, lo: 1 },
            crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.20", 9010).unwrap(),
        );

        patch_native_cte_multicast_sink(
            &mut fragment,
            1,
            3,
            &consumers,
            &BTreeMap::from([(2, vec![destination])]),
        )
        .expect("patch native CTE sink");

        let Some(native_plan::data_sink::Kind::MultiCastDataStream(sink)) =
            fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
        else {
            panic!("native multicast sink");
        };
        assert_eq!(sink.sinks.len(), 1);
        assert_eq!(sink.sinks[0].output_columns, vec![10]);
        assert_eq!(sink.destinations[0].destinations.len(), 1);
        assert_eq!(
            sink.destinations[0].destinations[0].endpoint,
            "10.0.0.20:9010"
        );
    }

    #[test]
    fn router_patch_changes_only_the_placement_clone() {
        use crate::sql::common::ChangeStreamBranchKind;

        let edge = FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: 2,
            target_exchange_node_id: 77,
            output_partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
            stream_kind: crate::sql::planner::distributed::FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::IcebergChangeStreamRouter {
                router_group_id: 7,
                branch_id: 0,
                branch_kind: ChangeStreamBranchKind::DeleteDv,
            },
            output_slot_ids: vec![10],
        };
        let static_fragment = native_plan::PlanFragment {
            fragment_id: 1,
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(
                    native_plan::IcebergChangeStreamRouterSink {
                        group_id: 7,
                        change_op_output_ordinal: 0,
                        data_route_output_ordinal: None,
                        branches: vec![native_plan::IcebergChangeStreamBranchRoute {
                            branch_id: 0,
                            branch_kind: native_plan::ChangeStreamBranchKind::DeleteDv as i32,
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_ordinals: vec![0],
                            output_partition_ordinals: Vec::new(),
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            }),
                            destinations: None,
                        }],
                    },
                )),
            }),
            ..Default::default()
        };
        let mut placement_clone = static_fragment.clone();
        let placement = FragmentInstancePlacement {
            fragment_id: 2,
            instance_index: 0,
            finst_id: UniqueId { hi: 93_000, lo: 1 },
            backend_idx: 4,
            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        };

        patch_native_iceberg_change_stream_router_sink(
            &mut placement_clone,
            1,
            7,
            &[&edge],
            &BTreeMap::from([(2, vec![placement])]),
        )
        .expect("patch placement-local clone");

        fn route(
            fragment: &native_plan::PlanFragment,
        ) -> &native_plan::IcebergChangeStreamBranchRoute {
            let Some(native_plan::data_sink::Kind::IcebergChangeStreamRouter(router)) =
                fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
            else {
                panic!("router sink");
            };
            &router.branches[0]
        }
        assert!(route(&static_fragment).destinations.is_none());
        assert_eq!(
            route(&placement_clone)
                .destinations
                .as_ref()
                .unwrap()
                .destinations
                .len(),
            1
        );
        assert_eq!(route(&placement_clone).target_exchange_node_id, 77);
    }

    #[test]
    fn router_patch_rejects_extra_encoded_route_before_mutation() {
        use crate::sql::common::ChangeStreamBranchKind;

        assert_router_rejected_without_mutation(
            router_fragment(
                7,
                vec![
                    router_route(0, native_plan::ChangeStreamBranchKind::DeleteDv),
                    router_route(1, native_plan::ChangeStreamBranchKind::ReuseData),
                ],
            ),
            vec![router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2)],
            "route key set",
        );
    }

    #[test]
    fn router_patch_rejects_missing_encoded_route_before_mutation() {
        use crate::sql::common::ChangeStreamBranchKind;

        assert_router_rejected_without_mutation(
            router_fragment(
                7,
                vec![router_route(
                    0,
                    native_plan::ChangeStreamBranchKind::DeleteDv,
                )],
            ),
            vec![
                router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2),
                router_edge(7, 1, ChangeStreamBranchKind::ReuseData, 3),
            ],
            "route key set",
        );
    }

    #[test]
    fn router_patch_rejects_duplicate_encoded_route_key_before_mutation() {
        use crate::sql::common::ChangeStreamBranchKind;

        assert_router_rejected_without_mutation(
            router_fragment(
                7,
                vec![
                    router_route(0, native_plan::ChangeStreamBranchKind::DeleteDv),
                    router_route(0, native_plan::ChangeStreamBranchKind::DeleteDv),
                ],
            ),
            vec![router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2)],
            "duplicate encoded route key",
        );
    }

    #[test]
    fn router_patch_rejects_encoded_group_id_drift_before_mutation() {
        use crate::sql::common::ChangeStreamBranchKind;

        assert_router_rejected_without_mutation(
            router_fragment(
                8,
                vec![router_route(
                    0,
                    native_plan::ChangeStreamBranchKind::DeleteDv,
                )],
            ),
            vec![router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2)],
            "encoded group=8",
        );
    }

    #[test]
    fn router_patch_rejects_single_missing_partition_without_mutation() {
        use crate::sql::common::ChangeStreamBranchKind;

        let mut fragment = router_fragment(
            7,
            vec![router_route(
                0,
                native_plan::ChangeStreamBranchKind::DeleteDv,
            )],
        );
        router_branches_mut(&mut fragment)[0].output_partition = None;

        assert_router_rejected_without_mutation(
            fragment,
            vec![router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2)],
            "missing output_partition",
        );
    }

    #[test]
    fn router_patch_rejects_later_missing_partition_without_partial_patch() {
        use crate::sql::common::ChangeStreamBranchKind;

        let mut fragment = router_fragment(
            7,
            vec![
                router_route(0, native_plan::ChangeStreamBranchKind::DeleteDv),
                router_route(1, native_plan::ChangeStreamBranchKind::ReuseData),
            ],
        );
        router_branches_mut(&mut fragment)[1].output_partition = None;

        assert_router_rejected_without_mutation(
            fragment,
            vec![
                router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2),
                router_edge(7, 1, ChangeStreamBranchKind::ReuseData, 3),
            ],
            "missing output_partition",
        );
    }

    #[test]
    fn router_patch_rejects_later_missing_placements_without_partial_patch() {
        use crate::sql::common::ChangeStreamBranchKind;

        assert_router_rejected_without_mutation(
            router_fragment(
                7,
                vec![
                    router_route(0, native_plan::ChangeStreamBranchKind::DeleteDv),
                    router_route(1, native_plan::ChangeStreamBranchKind::ReuseData),
                ],
            ),
            vec![
                router_edge(7, 0, ChangeStreamBranchKind::DeleteDv, 2),
                router_edge(7, 1, ChangeStreamBranchKind::ReuseData, 4),
            ],
            "target fragment 4 has no placements",
        );
    }

    mod runtime_filter_deployment {
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::mpsc::{SyncSender, sync_channel};
        use std::time::Duration;

        use arrow::datatypes::DataType;

        use super::*;
        use crate::coordinator::ports::{
            RuntimeFilterDeploymentControlPort, RuntimeFilterDeploymentPolicyProvider,
        };
        use crate::coordinator::runtime_filter_deployment::RuntimeFilterInstallBarrier;
        use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
        use crate::runtime_filter::deployment::RuntimeFilterQueryDeploymentPolicy;
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, BindingId, ChannelId, CompletionFenceKind, CompletionRequirement,
            ContributionKind, CoverageWitnessId, LateApplyGranularity, NullSemantics,
            PlanFragmentId, PlanNodeId, ReductionRequirement, RuntimeFilterLifecycle,
            RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
        };
        use crate::runtime_filter::model::coverage::Coverage;
        use crate::runtime_filter::model::graph::{
            ApplyPoint, ConsumerBindingTarget, PlanLocation, ProducerRequirement,
            RuntimeFilterChannelSpec, RuntimeFilterGraph,
        };
        use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
        use crate::runtime_filter::port::install::{
            RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
        };
        use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;
        use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};

        type InstallGate = tokio::sync::oneshot::Receiver<Result<(), String>>;
        type AbortGate = tokio::sync::oneshot::Receiver<()>;

        #[derive(Default)]
        struct RecordingDeploymentControl {
            install_results: Mutex<BTreeMap<u32, Result<(), String>>>,
            abort_results: Mutex<BTreeMap<u32, Result<(), String>>>,
            install_gates: Mutex<BTreeMap<u32, InstallGate>>,
            abort_gates: Mutex<BTreeMap<u32, AbortGate>>,
            install_calls: Mutex<Vec<u32>>,
            abort_calls: Mutex<Vec<u32>>,
            live_installations: Mutex<BTreeSet<u32>>,
            tombstones: Mutex<BTreeSet<u32>>,
            install_side_effect_on_error: Mutex<BTreeSet<u32>>,
            events: Arc<Mutex<Vec<String>>>,
            install_started: Mutex<Option<SyncSender<u32>>>,
            abort_started: Mutex<Option<SyncSender<u32>>>,
        }

        impl RecordingDeploymentControl {
            fn with_install_result(
                self: Arc<Self>,
                participant: u32,
                result: Result<(), String>,
            ) -> Arc<Self> {
                self.install_results
                    .lock()
                    .unwrap()
                    .insert(participant, result);
                self
            }

            fn with_abort_result(
                self: Arc<Self>,
                participant: u32,
                result: Result<(), String>,
            ) -> Arc<Self> {
                self.abort_results
                    .lock()
                    .unwrap()
                    .insert(participant, result);
                self
            }

            fn with_install_side_effect_on_error(self: Arc<Self>, participant: u32) -> Arc<Self> {
                self.install_side_effect_on_error
                    .lock()
                    .unwrap()
                    .insert(participant);
                self
            }

            fn gate_install(
                &self,
                participant: u32,
            ) -> tokio::sync::oneshot::Sender<Result<(), String>> {
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.install_gates.lock().unwrap().insert(participant, rx);
                tx
            }

            fn gate_abort(&self, participant: u32) -> tokio::sync::oneshot::Sender<()> {
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.abort_gates.lock().unwrap().insert(participant, rx);
                tx
            }

            fn install_calls(&self) -> Vec<u32> {
                self.install_calls.lock().unwrap().clone()
            }

            fn abort_calls(&self) -> Vec<u32> {
                self.abort_calls.lock().unwrap().clone()
            }

            fn live_installations(&self) -> BTreeSet<u32> {
                self.live_installations.lock().unwrap().clone()
            }

            fn tombstones(&self) -> BTreeSet<u32> {
                self.tombstones.lock().unwrap().clone()
            }
        }

        #[async_trait::async_trait]
        impl RuntimeFilterDeploymentControlPort for RecordingDeploymentControl {
            async fn install(
                &self,
                _query_id: UniqueId,
                _lifecycle: RuntimeFilterQueryLifecycleOptions,
                _deadline: Duration,
                participant: RuntimeFilterParticipantId,
                _install: RuntimeFilterParticipantInstall,
            ) -> Result<(), String> {
                let participant = participant.get();
                self.events
                    .as_ref()
                    .lock()
                    .unwrap()
                    .push(format!("install:{participant}"));
                self.install_calls.lock().unwrap().push(participant);
                if let Some(started) = self.install_started.lock().unwrap().as_ref() {
                    started.send(participant).expect("bounded start receiver");
                }
                if self.tombstones.lock().unwrap().contains(&participant) {
                    return Err(format!(
                        "runtime filter deployment participant {participant} is tombstoned"
                    ));
                }
                let gate = self.install_gates.lock().unwrap().remove(&participant);
                let result = if let Some(gate) = gate {
                    gate.await.expect("install gate sender")
                } else {
                    self.install_results
                        .lock()
                        .unwrap()
                        .remove(&participant)
                        .unwrap_or(Ok(()))
                };
                if result.is_ok()
                    || self
                        .install_side_effect_on_error
                        .lock()
                        .unwrap()
                        .remove(&participant)
                {
                    self.live_installations.lock().unwrap().insert(participant);
                }
                result
            }

            async fn abort(
                &self,
                _query_id: UniqueId,
                _epoch: DeploymentEpoch,
                _deadline: Duration,
                participant: RuntimeFilterParticipantId,
            ) -> Result<(), String> {
                let participant = participant.get();
                self.events
                    .as_ref()
                    .lock()
                    .unwrap()
                    .push(format!("abort:{participant}"));
                self.abort_calls.lock().unwrap().push(participant);
                if let Some(started) = self.abort_started.lock().unwrap().as_ref() {
                    started.send(participant).expect("bounded start receiver");
                }
                let gate = self.abort_gates.lock().unwrap().remove(&participant);
                if let Some(gate) = gate {
                    gate.await.expect("abort gate sender");
                }
                self.tombstones.lock().unwrap().insert(participant);
                self.live_installations.lock().unwrap().remove(&participant);
                self.abort_results
                    .lock()
                    .unwrap()
                    .remove(&participant)
                    .unwrap_or(Ok(()))
            }
        }

        struct RecordingPolicyProvider {
            inner: crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider,
            calls: AtomicUsize,
            failure: Option<String>,
            events: Arc<Mutex<Vec<String>>>,
        }

        impl RuntimeFilterDeploymentPolicyProvider for RecordingPolicyProvider {
            fn policy_for(
                &self,
                graph: &RuntimeFilterGraph,
                backends: &crate::coordinator::cluster::LiveBackendSnapshot,
            ) -> Result<RuntimeFilterQueryDeploymentPolicy, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.events.lock().unwrap().push("compile".to_string());
                if let Some(failure) = &self.failure {
                    return Err(failure.clone());
                }
                self.inner.policy_for(graph, backends)
            }
        }

        fn lifecycle() -> RuntimeFilterQueryLifecycleOptions {
            RuntimeFilterQueryLifecycleOptions {
                delivery_expire: Duration::from_secs(5),
                query_expire: Duration::from_secs(30),
                transport_retry_interval: Duration::from_millis(200),
                transport_max_attempts: 3,
                transport_deadline: Duration::from_secs(2),
                transport_max_pending_entries: 1024,
                transport_max_pending_bytes: 1 << 20,
            }
        }

        fn participant_install(participant: u32) -> RuntimeFilterParticipantInstall {
            let epoch = DeploymentEpoch::new(17);
            let participant = RuntimeFilterParticipantId::new(participant);
            RuntimeFilterParticipantInstall::new(
                RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new()),
                RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new())
                    .expect("empty test routing shard"),
            )
        }

        fn installed_deployment(
            control: Arc<RecordingDeploymentControl>,
            query_id: UniqueId,
            participants: &[u32],
        ) -> InstalledRuntimeFilterDeployment {
            RuntimeFilterInstallBarrier::new(control)
                .install_all_or_rollback(
                    query_id,
                    DeploymentEpoch::new(17),
                    lifecycle(),
                    Duration::from_secs(1),
                    participants
                        .iter()
                        .map(|participant| {
                            (
                                RuntimeFilterParticipantId::new(*participant),
                                participant_install(*participant),
                            )
                        })
                        .collect(),
                )
                .expect("test deployment installs")
        }

        fn membership_graph() -> crate::sql::planner::distributed::DraftRuntimeFilterGraph {
            use crate::runtime_filter::model::graph::{
                ConsumerRequirementData, RuntimeFilterBindingRoleData, RuntimeFilterBindingSpecData,
            };
            use crate::sql::planner::distributed::{
                ActivationConstraint, DraftRuntimeFilterGraph, RequiredLiveReason,
            };

            let channel_id = ChannelId::new(1);
            let witness = CoverageWitnessId::new(1);
            let location = PlanLocation {
                fragment_id: PlanFragmentId::new(7),
                node_id: PlanNodeId::new(70),
            };
            let expression = || TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(1)),
                data_type: DataType::Int64,
                nullable: false,
            };
            let mut graph = DraftRuntimeFilterGraph::default();
            graph
                .insert_channel(RuntimeFilterChannelSpec {
                    channel_id,
                    logical_domain: RuntimeFilterLogicalDomain::Membership {
                        value_type: DataType::Int64,
                        null_semantics: NullSemantics::NullSafeEqual,
                    },
                    lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                    availability_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness)]),
                    terminal_coverage: Coverage::AllOf(vec![Coverage::Leaf(witness)]),
                    reduction_requirement: ReductionRequirement::SetUnion,
                    allowed_contribution_kinds: BTreeSet::from([
                        ContributionKind::FinalDomainShard,
                        ContributionKind::ProducerClosed,
                    ]),
                    required_consumer_capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    policy: RuntimeFilterPolicyRequirement {
                        max_contribution_bytes: 1024,
                        max_artifact_bytes: 4096,
                        deadline_ms: 2_000,
                        max_retries: 2,
                    },
                })
                .unwrap();
            graph
                .insert_binding(RuntimeFilterBindingSpecData {
                    binding_id: BindingId::new(1),
                    channel_id,
                    coverage_witness_id: Some(witness),
                    location,
                    expression: expression(),
                    apply_point: ApplyPoint::NodeOutput,
                    role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                        contribution_kinds: BTreeSet::from([
                            ContributionKind::FinalDomainShard,
                            ContributionKind::ProducerClosed,
                        ]),
                        completion_requirement: CompletionRequirement::FencedFinalDomain(
                            CompletionFenceKind::CommittedDomainFrozen,
                        ),
                        target: crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                            ordinal: 0,
                        },
                    }),
                })
                .unwrap();
            graph
                .insert_binding(RuntimeFilterBindingSpecData {
                    binding_id: BindingId::new(2),
                    channel_id,
                    coverage_witness_id: None,
                    location,
                    expression: expression(),
                    apply_point: ApplyPoint::NodeInput,
                    role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                        capabilities: BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        activation: ActivationConstraint::LiveOnly {
                            late_apply: LateApplyGranularity::Batch,
                            reason: RequiredLiveReason::FencedFinalDomainContract,
                        },
                        target: ConsumerBindingTarget::DirectInput { input_ordinal: 0 },
                    }),
                })
                .unwrap();
            graph
        }

        fn execution_artifacts(
            graph: crate::sql::planner::distributed::DraftRuntimeFilterGraph,
        ) -> (PreparedFragmentSet, NativeFragmentBundle) {
            let fragment = PlanFragment {
                fragment_id: 7,
                root: DistributedNode {
                    node_id: 70,
                    fragment_id: 7,
                    tuple_ids: vec![70],
                    nullable_tuple_ids: Vec::new(),
                    limit: -1,
                    runtime_filter_binding_ids: if graph.is_empty() {
                        Vec::new()
                    } else {
                        vec![BindingId::new(1), BindingId::new(2)]
                    },
                    children: Vec::new(),
                    stats: PhysicalPlanStats {
                        output_row_count: 0.0,
                        row_count_confidence: PlannerConfidence::Fallback,
                        column_statistics: Default::default(),
                        cost_estimate: None,
                        broadcast_decision: None,
                    },
                    payload: DistributedNodeKind::Values(PlanValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    }),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            };
            let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
                fragments: vec![fragment],
                root_fragment_id: 7,
                edges: Vec::new(),
                runtime_filter_graph: graph,
            };
            let prepared = crate::query_execution::preparation::prepare_fragments(
                &plan,
                &crate::connector::ConnectorRegistry::new(),
                None,
            )
            .expect("prepare runtime-filter execution artifact");
            let native_bundle =
                crate::protocol::native::encode::encode_native_fragment_bundle(&plan, &prepared)
                    .expect("encode runtime-filter execution artifact");
            (prepared, native_bundle)
        }

        fn scheduler() -> Arc<FragmentScheduler> {
            Arc::new(FragmentScheduler::new_with_backend_ids(vec![(
                0,
                "127.0.0.1:19031".parse().unwrap(),
            )]))
        }

        fn coordinator(
            graph: crate::sql::planner::distributed::DraftRuntimeFilterGraph,
            dispatcher: Arc<CapturingDispatcher>,
            control: Arc<dyn RuntimeFilterDeploymentControlPort>,
            policy_provider: Arc<dyn RuntimeFilterDeploymentPolicyProvider>,
        ) -> ExecutionCoordinator {
            let (prepared, native_bundle) = execution_artifacts(graph);
            let mut ports = CoordinatorExecutionPorts::new(
                dispatcher,
                crate::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                Arc::new(CountingCoordinatorObserver::default()),
                control,
            );
            ports.runtime_filter_policy_provider = policy_provider;
            ExecutionCoordinator::new(
                prepared,
                native_bundle,
                ports,
                scheduler(),
                None,
                QueryCancellationView::never_cancelled(),
            )
        }

        fn native_policy(events: Arc<Mutex<Vec<String>>>) -> Arc<RecordingPolicyProvider> {
            Arc::new(RecordingPolicyProvider {
                inner: crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider::new(2),
                calls: AtomicUsize::new(0),
                failure: None,
                events,
            })
        }

        #[test]
        fn nonempty_graph_compiles_after_schedule_before_install() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let control = Arc::new(RecordingDeploymentControl {
                events: events.clone(),
                ..Default::default()
            });
            let dispatcher = CapturingDispatcher::new(None);
            coordinator(
                membership_graph(),
                dispatcher.clone(),
                control.clone(),
                native_policy(events.clone()),
            )
            .execute_with_write_outcome()
            .expect("compiled deployment installs before fragment submission");

            assert_eq!(events.lock().unwrap().as_slice(), ["compile", "install:1"]);
            assert_eq!(control.install_calls(), vec![1]);
            assert!(control.abort_calls().is_empty());
            assert_eq!(dispatcher.submit_count.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn post_install_assembly_failure_aborts_every_acknowledged_participant() {
            let control = Arc::new(RecordingDeploymentControl::default())
                .with_abort_result(1, Err("rollback failed at backend-zero".to_string()));
            let dispatcher = CapturingDispatcher::new(None);
            let mut coordinator = coordinator(
                membership_graph(),
                dispatcher.clone(),
                control.clone(),
                native_policy(Arc::new(Mutex::new(Vec::new()))),
            );
            coordinator.post_install_assembly_test_drift =
                Some(PostInstallAssemblyTestDrift::MissingRootPlacement);

            let error = coordinator
                .execute_with_write_outcome()
                .expect_err("post-install assembly failure must roll back the ACKed deployment");

            assert!(
                error.starts_with("native fragments remained after submission assembly"),
                "{error}"
            );
            assert!(error.contains("rollback failures"), "{error}");
            assert!(error.contains("participant 1"), "{error}");
            assert!(error.contains("rollback failed at backend-zero"), "{error}");
            assert_eq!(control.install_calls(), vec![1]);
            assert_eq!(control.abort_calls(), vec![1]);
            assert_eq!(dispatcher.submit_count.load(Ordering::SeqCst), 0);
        }

        #[test]
        fn fragment_submit_waits_for_every_install_ack() {
            let (started_tx, started_rx) = sync_channel(2);
            let control = Arc::new(RecordingDeploymentControl {
                install_started: Mutex::new(Some(started_tx)),
                ..Default::default()
            });
            let first = control.gate_install(1);
            let second = control.gate_install(2);
            let (submitted_tx, submitted_rx) = sync_channel(1);
            let barrier_control = control.clone();
            std::thread::spawn(move || {
                let result = RuntimeFilterInstallBarrier::new(barrier_control)
                    .install_all_or_rollback(
                        UniqueId { hi: 1, lo: 2 },
                        DeploymentEpoch::new(17),
                        lifecycle(),
                        Duration::from_secs(1),
                        vec![
                            (RuntimeFilterParticipantId::new(1), participant_install(1)),
                            (RuntimeFilterParticipantId::new(2), participant_install(2)),
                        ],
                    )
                    .map(InstalledRuntimeFilterDeployment::release);
                submitted_tx.send(result).unwrap();
            });

            let mut started = BTreeSet::new();
            started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
            started.insert(started_rx.recv_timeout(Duration::from_secs(1)).unwrap());
            assert_eq!(started, BTreeSet::from([1, 2]));
            first.send(Ok(())).unwrap();
            assert!(submitted_rx.try_recv().is_err());
            second.send(Ok(())).unwrap();
            assert!(
                submitted_rx
                    .recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .is_ok()
            );
        }

        #[test]
        fn unreleased_deployment_drop_aborts_every_participant() {
            let control = Arc::new(RecordingDeploymentControl::default());
            let deployment = installed_deployment(
                control.clone(),
                UniqueId {
                    hi: 95_000,
                    lo: 95_001,
                },
                &[1, 2],
            );

            drop(deployment);

            assert_eq!(control.abort_calls(), vec![1, 2]);
            assert!(control.live_installations().is_empty());
            assert_eq!(control.tombstones(), BTreeSet::from([1, 2]));
        }

        #[test]
        fn install_unknown_outcome_submits_zero_fragments_and_aborts_every_attempted_participant() {
            let integrated_control = Arc::new(RecordingDeploymentControl::default())
                .with_install_result(1, Err("coordinator install refused".to_string()));
            let dispatcher = CapturingDispatcher::new(None);
            let integrated_error = coordinator(
                membership_graph(),
                dispatcher.clone(),
                integrated_control,
                native_policy(Arc::new(Mutex::new(Vec::new()))),
            )
            .execute_with_write_outcome()
            .expect_err("install failure must stop the real coordinator before submission");
            assert!(
                integrated_error.contains("coordinator install refused"),
                "{integrated_error}"
            );
            assert_eq!(dispatcher.submit_count.load(Ordering::SeqCst), 0);

            let control = Arc::new(RecordingDeploymentControl::default())
                .with_install_result(2, Err("install ACK was lost".to_string()))
                .with_install_side_effect_on_error(2);
            let submitted = AtomicUsize::new(0);
            let error = RuntimeFilterInstallBarrier::new(control.clone())
                .install_all_or_rollback(
                    UniqueId { hi: 3, lo: 4 },
                    DeploymentEpoch::new(17),
                    lifecycle(),
                    Duration::from_secs(1),
                    vec![
                        (RuntimeFilterParticipantId::new(1), participant_install(1)),
                        (RuntimeFilterParticipantId::new(2), participant_install(2)),
                    ],
                )
                .map(|deployment| {
                    deployment.release();
                    submitted.fetch_add(1, Ordering::SeqCst)
                })
                .err()
                .expect("install failure must trigger rollback");

            assert!(error.contains("participant 2"), "{error}");
            assert!(error.contains("install ACK was lost"), "{error}");
            assert_eq!(submitted.load(Ordering::SeqCst), 0);
            assert_eq!(control.abort_calls(), vec![1, 2]);
            assert!(control.live_installations().is_empty());
            assert_eq!(control.tombstones(), BTreeSet::from([1, 2]));

            let late_install = crate::runtime::global_async_runtime::data_block_on(async {
                control
                    .install(
                        UniqueId { hi: 3, lo: 4 },
                        lifecycle(),
                        Duration::from_secs(1),
                        RuntimeFilterParticipantId::new(2),
                        participant_install(2),
                    )
                    .await
            })
            .expect("data runtime remains available")
            .expect_err("rollback tombstone must reject a late install");
            assert!(late_install.contains("tombstoned"), "{late_install}");
            assert!(control.live_installations().is_empty());
        }

        #[test]
        fn rollback_fans_out_to_every_attempted_participant_in_parallel() {
            let (started_tx, started_rx) = sync_channel(2);
            let control = Arc::new(RecordingDeploymentControl {
                abort_started: Mutex::new(Some(started_tx)),
                ..Default::default()
            })
            .with_install_result(2, Err("install outcome unknown".to_string()));
            let first_abort = control.gate_abort(1);
            let second_abort = control.gate_abort(2);
            let (result_tx, result_rx) = sync_channel(1);
            let barrier_control = control.clone();
            std::thread::spawn(move || {
                let result = RuntimeFilterInstallBarrier::new(barrier_control)
                    .install_all_or_rollback(
                        UniqueId { hi: 8, lo: 9 },
                        DeploymentEpoch::new(17),
                        lifecycle(),
                        Duration::from_secs(1),
                        vec![
                            (RuntimeFilterParticipantId::new(1), participant_install(1)),
                            (RuntimeFilterParticipantId::new(2), participant_install(2)),
                        ],
                    );
                result_tx.send(result).expect("bounded result receiver");
            });

            let mut started = BTreeSet::new();
            started.insert(
                started_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("first rollback starts"),
            );
            started.insert(
                started_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("second rollback starts"),
            );
            assert_eq!(started, BTreeSet::from([1, 2]));
            assert!(
                result_rx.try_recv().is_err(),
                "rollback must wait for every participant"
            );
            first_abort.send(()).expect("release first rollback");
            second_abort.send(()).expect("release second rollback");

            let error = result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("barrier returns after rollback fanout")
                .err()
                .expect("unknown install outcome remains the primary error");
            assert!(error.contains("install outcome unknown"), "{error}");
            assert_eq!(control.abort_calls(), vec![1, 2]);
        }

        #[test]
        fn first_submit_failure_cancels_in_flight_fragment_and_aborts_entire_deployment() {
            let query_id = UniqueId {
                hi: 96_000,
                lo: 96_001,
            };
            let first_finst_id = UniqueId { hi: 96_000, lo: 1 };
            let root_finst_id = UniqueId { hi: 96_000, lo: 2 };
            let control = Arc::new(RecordingDeploymentControl::default());
            let deployment = installed_deployment(control.clone(), query_id, &[1, 2]);
            let dispatcher = CapturingDispatcher::new(Some(1));
            let dispatcher_port: Arc<dyn FragmentDispatcher> = dispatcher.clone();
            let mut tracker = InFlightTracker::default();

            let error = submit_and_fetch_loop_with_deployment_lease(
                &dispatcher_port,
                &mut tracker,
                vec![
                    (1, submission(3, query_id, first_finst_id)),
                    (0, submission(7, query_id, root_finst_id)),
                ],
                7,
                0,
                root_finst_id,
                &query_id,
                true,
                1_000,
                None,
                None,
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
                Some(deployment),
            )
            .expect_err("first submit failure must abort the installed deployment");

            assert!(
                error.starts_with("native submit failed on call 1"),
                "{error}"
            );
            assert_eq!(
                *dispatcher.cancellations.lock().unwrap(),
                vec![(1, vec![first_finst_id])]
            );
            assert_eq!(control.abort_calls(), vec![1, 2]);
            assert!(control.live_installations().is_empty());
            assert_eq!(control.tombstones(), BTreeSet::from([1, 2]));
            assert_eq!(
                crate::runtime::query_state::in_flight_table().state(
                    crate::runtime::query_context::QueryId {
                        hi: query_id.hi,
                        lo: query_id.lo,
                    }
                ),
                None,
                "failed submission must not leak a query-state registration"
            );
        }

        #[test]
        fn middle_submit_failure_cancels_every_attempt_and_preserves_abort_context() {
            let query_id = UniqueId {
                hi: 97_000,
                lo: 97_001,
            };
            let first_finst_id = UniqueId { hi: 97_000, lo: 1 };
            let root_finst_id = UniqueId { hi: 97_000, lo: 2 };
            let control = Arc::new(RecordingDeploymentControl::default())
                .with_abort_result(2, Err("participant-two abort failed".to_string()));
            let deployment = installed_deployment(control.clone(), query_id, &[1, 2]);
            let dispatcher = CapturingDispatcher::new(Some(2));
            let dispatcher_port: Arc<dyn FragmentDispatcher> = dispatcher.clone();
            let mut tracker = InFlightTracker::default();

            let error = submit_and_fetch_loop_with_deployment_lease(
                &dispatcher_port,
                &mut tracker,
                vec![
                    (1, submission(3, query_id, first_finst_id)),
                    (0, submission(7, query_id, root_finst_id)),
                ],
                7,
                0,
                root_finst_id,
                &query_id,
                true,
                1_000,
                None,
                None,
                false,
                &CountingCoordinatorObserver::default(),
                &QueryCancellationView::never_cancelled(),
                Some(deployment),
            )
            .expect_err("middle submit failure must abort the installed deployment");

            assert!(
                error.starts_with("native submit failed on call 2"),
                "{error}"
            );
            assert!(error.contains("rollback failures"), "{error}");
            assert!(error.contains("participant-two abort failed"), "{error}");
            assert_eq!(
                *dispatcher.cancellations.lock().unwrap(),
                vec![(0, vec![root_finst_id]), (1, vec![first_finst_id]),]
            );
            assert_eq!(control.abort_calls(), vec![1, 2]);
            assert!(control.live_installations().is_empty());
            assert_eq!(control.tombstones(), BTreeSet::from([1, 2]));
        }

        #[test]
        fn rollback_failure_preserves_primary_install_error_with_context() {
            let control = Arc::new(RecordingDeploymentControl::default())
                .with_install_result(2, Err("primary install failure at endpoint-b".to_string()))
                .with_abort_result(1, Err("rollback failure at endpoint-a".to_string()));
            let error = RuntimeFilterInstallBarrier::new(control)
                .install_all_or_rollback(
                    UniqueId { hi: 5, lo: 6 },
                    DeploymentEpoch::new(17),
                    lifecycle(),
                    Duration::from_secs(1),
                    vec![
                        (RuntimeFilterParticipantId::new(1), participant_install(1)),
                        (RuntimeFilterParticipantId::new(2), participant_install(2)),
                    ],
                )
                .err()
                .expect("install failure must preserve rollback context");

            assert!(error.contains("participant 2"), "{error}");
            assert!(
                error.contains("primary install failure at endpoint-b"),
                "{error}"
            );
            assert!(error.contains("rollback failures"), "{error}");
            assert!(error.contains("participant 1"), "{error}");
            assert!(error.contains("rollback failure at endpoint-a"), "{error}");
        }

        #[test]
        fn empty_graph_sends_zero_install_rpc() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let policy = native_policy(events);
            let control = Arc::new(RecordingDeploymentControl::default());
            let dispatcher = CapturingDispatcher::new(None);
            coordinator(
                Default::default(),
                dispatcher.clone(),
                control.clone(),
                policy.clone(),
            )
            .execute_with_write_outcome()
            .expect("empty graph bypasses deployment");

            assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
            assert!(control.install_calls().is_empty());
            assert_eq!(dispatcher.submit_count.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn compile_failure_sends_zero_install_and_zero_submit() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let policy = Arc::new(RecordingPolicyProvider {
                inner: crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider::new(2),
                calls: AtomicUsize::new(0),
                failure: Some("compile phase rejected policy".to_string()),
                events,
            });
            let control = Arc::new(RecordingDeploymentControl::default());
            let dispatcher = CapturingDispatcher::new(None);
            let error = coordinator(
                membership_graph(),
                dispatcher.clone(),
                control.clone(),
                policy,
            )
            .execute_with_write_outcome()
            .expect_err("compile phase failure must stop deployment");

            assert!(error.contains("compile phase rejected policy"), "{error}");
            assert!(control.install_calls().is_empty());
            assert_eq!(dispatcher.submit_count.load(Ordering::SeqCst), 0);
        }
    }
}
