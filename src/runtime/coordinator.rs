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
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::novarocks_logging::debug;
use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher, FragmentSubmission};
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_state::QueryState;
use crate::runtime::scheduler::{
    FragmentInstancePlacement, FragmentScheduler, topological_sort_bottom_up,
};
use crate::runtime::write_coordinator::{
    WriteAbortInput, WriteCommitInput, WriteCoordinator, WriterKey, register_query,
    unregister_query,
};
use crate::sql::analysis::cte::CteId;
use crate::sql::codegen::{FragmentOutputKind, MultiFragmentBuildResult, RuntimeFilterPlanResult};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{FragmentEdge, FragmentEdgeKind, FragmentId};

use crate::runtime::query_result::{QueryResult, QueryResultColumn};

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
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn FragmentDispatcher>,
    scheduler: Arc<FragmentScheduler>,
    query_options: Option<QueryOptions>,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        build_result: MultiFragmentBuildResult,
        dispatcher: Arc<dyn FragmentDispatcher>,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<QueryOptions>,
    ) -> Self {
        Self {
            build_result,
            dispatcher,
            scheduler,
            query_options,
        }
    }

    pub(crate) fn execute_with_write_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(false)
    }

    pub(crate) fn execute_with_profile_outcome(self) -> Result<CoordinatedQueryResult, String> {
        self.execute_with_profile_collection(true)
    }

    fn execute_with_profile_collection(
        self,
        collect_profiles: bool,
    ) -> Result<CoordinatedQueryResult, String> {
        let MultiFragmentBuildResult {
            fragment_schedules,
            native_fragments,
            root_fragment_id,
            edges,
            boundary_schemas,
            rf_plan,
            ..
        } = self.build_result;
        let query_options = self.query_options;
        let dispatcher = self.dispatcher;
        let scheduler = self.scheduler;
        let native_fragments_by_id = native_fragments;
        // ---------------------------------------------------------------
        // 1. Allocate query id and run the scheduler.
        // ---------------------------------------------------------------
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_QUERY_BASE: AtomicI64 = AtomicI64::new(100);
        let query_base = NEXT_QUERY_BASE.fetch_add(1000, Ordering::Relaxed);
        // Use query_base for both hi and lo so the scheduler's
        // `root_backend_idx = query_id.lo % n` scatters across backends per
        // query instead of always landing on backend 1 % n.
        let query_id = UniqueId {
            hi: query_base,
            lo: query_base,
        };

        debug!(
            "coordinator topology: fragments={} edges={} root={} backends={}",
            native_fragments_by_id.len(),
            edges.len(),
            root_fragment_id,
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

        validate_fragment_schedule_payloads(
            &fragment_schedules,
            &native_fragments_by_id,
            root_fragment_id,
            &boundary_schemas,
        )?;
        let live = scheduler.live_backend_entries().to_vec();
        let mut plan =
            scheduler.assign_with_live(&fragment_schedules, &edges, query_id.clone(), &live)?;
        scheduler.fill_destinations_with_live(&mut plan, &edges, &live)?;
        if let Some(rf) = rf_plan.as_ref() {
            scheduler.fill_runtime_filter_params_with_live(&mut plan, rf, &live)?;
        }
        scheduler.fill_per_exch_num_senders(&mut plan, &edges);
        validate_native_scheduling_plan(&fragment_schedules, &native_fragments_by_id, &plan)?;
        let execution_root_fragment_id = plan.root_fragment_id;

        // ---------------------------------------------------------------
        // 2. Build per-edge / CTE consumer indices used for sink wiring.
        // ---------------------------------------------------------------
        // Stream producer fragment id -> its single outgoing plain stream edge.
        let stream_edge_by_source = build_stream_edge_by_source(&edges)?;
        let router_edge_groups = group_router_edges_by_source(&edges)?;
        let mut router_edges_by_source: BTreeMap<FragmentId, (i32, Vec<&FragmentEdge>)> =
            BTreeMap::new();
        for ((source_fragment_id, router_group_id), branch_edges) in router_edge_groups {
            if router_edges_by_source
                .insert(source_fragment_id, (router_group_id, branch_edges))
                .is_some()
            {
                return Err(format!(
                    "fragment {source_fragment_id} has multiple Iceberg change-stream router groups; \
                     one source fragment can only use one router sink template"
                ));
            }
        }
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
                        crate::sql::codegen::proto_encode::plan::encode_data_partition(
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
        for schedule in &fragment_schedules {
            for (cte_id, exchange_node_id, receive_producer_column_ids) in
                &schedule.cte_exchange_nodes
            {
                let consumers = cte_consumers.entry(*cte_id).or_default();
                if !consumers.iter().any(|(fid, nid, _, _, _)| {
                    *fid == schedule.fragment_id && *nid == *exchange_node_id
                }) {
                    consumers.push((
                        schedule.fragment_id,
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
        let mut novarocks_report_endpoint: Option<crate::runtime::endpoint::RuntimeEndpoint> = None;

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

        let schedule_by_id: BTreeMap<FragmentId, &crate::sql::codegen::FragmentSchedulingMetadata> =
            fragment_schedules
                .iter()
                .map(|fr| (fr.fragment_id, fr))
                .collect();

        // Build a fragment-id -> instance count map from the scheduling plan.
        // Builder numbers must equal the number of build-side instances, not a
        // hardcoded 1.
        let instance_counts: BTreeMap<FragmentId, usize> = plan
            .by_fragment
            .iter()
            .map(|(&fid, insts)| (fid, insts.len()))
            .collect();

        let mut tracker = InFlightTracker::default();
        // Collect submissions by fragment, then submit consumers before
        // producers. This ensures downstream exchange receivers/result buffers
        // are registered before an upstream producer can fail or send data.
        let mut submissions_by_fragment: BTreeMap<FragmentId, Vec<(usize, FragmentSubmission)>> =
            BTreeMap::new();
        let mut expected_writers = Vec::new();

        for (&fragment_id, placements) in &plan.by_fragment {
            let schedule = *schedule_by_id
                .get(&fragment_id)
                .ok_or_else(|| format!("fragment {fragment_id} missing from native schedules"))?;
            let is_root = fragment_id == execution_root_fragment_id;
            let stream_edge = stream_edge_by_source.get(&fragment_id).copied();
            let router_edges = router_edges_by_source.get(&fragment_id);
            let is_terminal_write = stream_edge.is_none()
                && router_edges.is_none()
                && schedule.cte_id.is_none()
                && schedule.output_kind.is_terminal_write();
            let is_producer =
                stream_edge.is_some() || router_edges.is_some() || schedule.cte_id.is_some();
            validate_fragment_output_kind(
                fragment_id,
                is_root,
                is_terminal_write,
                is_producer,
                schedule.output_kind,
            )?;

            // Classify the fragment once.
            if !is_root
                && !is_terminal_write
                && schedule.cte_id.is_none()
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
                schedule.cte_id.is_some(),
            )?;

            for placement in placements {
                let fragment_has_write_sink = is_terminal_write;
                let fragment_report_endpoint =
                    if fragment_has_write_sink || needs_fragment_status_report {
                        if novarocks_report_endpoint.is_none() {
                            novarocks_report_endpoint = Some(local_coordinator_report_endpoint()?);
                        }
                        novarocks_report_endpoint.clone()
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

                let mut native_fragment = native_fragments_by_id
                    .get(&fragment_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!("native fragment build missing fragment {fragment_id}")
                    })?;
                if !is_root && !is_terminal_write && stream_edge.is_none() {
                    if let Some((router_group_id, branch_edges)) = router_edges {
                        patch_native_iceberg_change_stream_router_sink(
                            &mut native_fragment,
                            fragment_id,
                            *router_group_id,
                            branch_edges,
                            &plan.by_fragment,
                        )?;
                    } else if let Some(cte_id) = schedule.cte_id {
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
                let native_rf_builder_number =
                    runtime_filter_builder_number_for_instance(rf_plan.as_ref(), &instance_counts);
                let native_rf_max_size = if rf_plan.is_some() {
                    16_i64 * 1024 * 1024
                } else {
                    0
                };
                let native_instance_params =
                    crate::sql::codegen::proto_encode::instance::encode_instance_params(
                        &query_id,
                        placement,
                        query_options.as_ref(),
                        &placement.runtime_filter_prober_params,
                        &native_rf_builder_number,
                        native_rf_max_size,
                        placement.instance_index as i32,
                        fragment_report_endpoint.as_ref(),
                        typed_result_sink,
                    )?;
                let submission = FragmentSubmission::new(native_fragment, native_instance_params);

                submissions_by_fragment
                    .entry(fragment_id)
                    .or_default()
                    .push((placement.backend_idx, submission));
            }
        }

        if !submissions_by_fragment.contains_key(&execution_root_fragment_id) {
            return Err("root fragment produced no placement".to_string());
        }
        let mut submissions: Vec<(usize, FragmentSubmission)> = Vec::new();
        for fragment_id in topological_sort_bottom_up(&fragment_schedules, &edges)?
            .into_iter()
            .rev()
        {
            if let Some(mut fragment_submissions) = submissions_by_fragment.remove(&fragment_id) {
                submissions.append(&mut fragment_submissions);
            }
        }
        if !submissions_by_fragment.is_empty() {
            return Err(format!(
                "submissions remained for unknown fragments: {:?}",
                submissions_by_fragment.keys().collect::<Vec<_>>()
            ));
        }

        let (write_coordinator, _write_registration) = if expected_writers.is_empty() {
            (None, None)
        } else {
            let write = register_query(query_id, expected_writers)?;
            (Some(write), Some(RegisteredWriteCoordinator::new(query_id)))
        };

        let timeout_ms = query_options
            .as_ref()
            .and_then(|q| q.query_timeout)
            .map(|t| t as i64 * 1000)
            .unwrap_or(300_000); // 5 minute default
        let root_schedule = schedule_by_id
            .get(&execution_root_fragment_id)
            .ok_or_else(|| "root fragment not found in native schedules".to_string())?;
        let root_uses_result_buffer = !root_schedule.output_kind.is_terminal_write();
        let expected_root_chunk_schema = if root_uses_result_buffer {
            Some(build_root_expected_chunk_schema(root_schedule)?)
        } else {
            None
        };

        let fetch_result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            submissions,
            execution_root_fragment_id,
            plan.root_backend_idx,
            plan.root_finst_id.clone(),
            &query_id,
            root_uses_result_buffer,
            timeout_ms,
            expected_root_chunk_schema.as_ref(),
            write_coordinator.as_ref(),
            collect_profiles,
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

        let chunks = align_fetch_chunks_to_output_columns(
            fetch_result.chunks,
            &root_schedule.output_columns,
        )?;
        let query_result = QueryResult {
            columns: root_schedule
                .output_columns
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
            fragment_profiles: fetch_result.fragment_profiles,
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
    root_fragment: &crate::sql::codegen::FragmentSchedulingMetadata,
) -> Result<ChunkSchemaRef, String> {
    let output_columns = &root_fragment.output_columns;
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
    output_columns: &[crate::sql::codegen::OutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[crate::sql::codegen::OutputColumn],
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

fn build_stream_edge_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> Result<BTreeMap<FragmentId, &'a FragmentEdge>, String> {
    let router_sources: BTreeSet<FragmentId> = edges
        .iter()
        .filter_map(|edge| {
            matches!(
                edge.edge_kind,
                FragmentEdgeKind::IcebergChangeStreamRouter { .. }
            )
            .then_some(edge.source_fragment_id)
        })
        .collect();
    let mut stream_edge_by_source = BTreeMap::new();
    for edge in edges {
        if !matches!(edge.edge_kind, FragmentEdgeKind::Stream) {
            continue;
        }
        if router_sources.contains(&edge.source_fragment_id) {
            return Err(format!(
                "fragment {} has both plain Stream and Iceberg change-stream router edges",
                edge.source_fragment_id
            ));
        }
        if stream_edge_by_source
            .insert(edge.source_fragment_id, edge)
            .is_some()
        {
            return Err(format!(
                "fragment {} has multiple outgoing stream edges; stream fan-out is not supported",
                edge.source_fragment_id
            ));
        }
    }
    Ok(stream_edge_by_source)
}

fn group_router_edges_by_source<'a>(
    edges: &'a [FragmentEdge],
) -> Result<BTreeMap<(FragmentId, i32), Vec<&'a FragmentEdge>>, String> {
    let stream_sources: BTreeSet<FragmentId> = edges
        .iter()
        .filter_map(|edge| {
            matches!(edge.edge_kind, FragmentEdgeKind::Stream).then_some(edge.source_fragment_id)
        })
        .collect();
    let mut grouped: BTreeMap<(FragmentId, i32), Vec<&FragmentEdge>> = BTreeMap::new();
    let mut branch_ids_by_group: BTreeMap<(FragmentId, i32), BTreeSet<i32>> = BTreeMap::new();
    let mut branch_kinds_by_group = BTreeMap::new();
    let mut target_exchanges_by_group = BTreeMap::new();

    for edge in edges {
        let FragmentEdgeKind::IcebergChangeStreamRouter {
            router_group_id,
            branch_id,
            branch_kind,
        } = edge.edge_kind
        else {
            continue;
        };
        if stream_sources.contains(&edge.source_fragment_id) {
            return Err(format!(
                "fragment {} has both plain Stream and Iceberg change-stream router edges",
                edge.source_fragment_id
            ));
        }
        let key = (edge.source_fragment_id, router_group_id);
        if !branch_ids_by_group
            .entry(key)
            .or_default()
            .insert(branch_id)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats branch_id {}",
                edge.source_fragment_id, router_group_id, branch_id
            ));
        }
        if !branch_kinds_by_group
            .entry(key)
            .or_insert_with(BTreeSet::new)
            .insert(branch_kind)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats branch_kind {:?}",
                edge.source_fragment_id, router_group_id, branch_kind
            ));
        }
        let target_exchange = (edge.target_fragment_id, edge.target_exchange_node_id);
        if !target_exchanges_by_group
            .entry(key)
            .or_insert_with(BTreeSet::new)
            .insert(target_exchange)
        {
            return Err(format!(
                "Iceberg change-stream router group source={} group={} repeats target exchange \
                 fragment={} node={}",
                edge.source_fragment_id,
                router_group_id,
                edge.target_fragment_id,
                edge.target_exchange_node_id
            ));
        }
        grouped.entry(key).or_default().push(edge);
    }

    Ok(grouped)
}

struct RegisteredWriteCoordinator {
    query_id: UniqueId,
}

impl RegisteredWriteCoordinator {
    fn new(query_id: UniqueId) -> Self {
        Self { query_id }
    }
}

impl Drop for RegisteredWriteCoordinator {
    fn drop(&mut self) {
        unregister_query(&self.query_id);
    }
}

fn validate_write_commit_ready(
    write: &Arc<Mutex<WriteCoordinator>>,
) -> Result<WriteCommitInput, String> {
    write.lock().expect("write coordinator lock").commit_input()
}

fn local_coordinator_report_endpoint() -> Result<crate::runtime::endpoint::RuntimeEndpoint, String>
{
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    let host = crate::common::network::advertise_host().unwrap_or_else(|_| cfg.server.host.clone());
    let port =
        crate::service::grpc_server::grpc_server_bound_port().unwrap_or(cfg.server.grpc_port);
    crate::runtime::endpoint::RuntimeEndpoint::new(host, port as i32)
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
    output_kind: FragmentOutputKind,
) -> Result<(), String> {
    if is_root {
        return match output_kind {
            FragmentOutputKind::Result | FragmentOutputKind::TerminalWrite => Ok(()),
            FragmentOutputKind::NonTerminal => Err(format!(
                "root fragment {fragment_id} must have Result or TerminalWrite output kind"
            )),
        };
    }
    if is_terminal_write {
        return (output_kind == FragmentOutputKind::TerminalWrite)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "terminal write fragment {fragment_id} must have TerminalWrite output kind, got {output_kind:?}"
                )
            });
    }
    if is_producer {
        return (output_kind == FragmentOutputKind::NonTerminal)
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "producer fragment {fragment_id} must have NonTerminal output kind, got {output_kind:?}"
                )
            });
    }
    Ok(())
}

fn validate_fragment_schedule_payloads(
    fragment_schedules: &[crate::sql::codegen::FragmentSchedulingMetadata],
    native_fragments: &BTreeMap<FragmentId, crate::proto::plan::PlanFragment>,
    root_fragment_id: FragmentId,
    boundary_schemas: &[crate::sql::codegen::boundary_schema::BoundarySchemaReport],
) -> Result<(), String> {
    let schedule_ids: BTreeSet<FragmentId> =
        fragment_schedules.iter().map(|fr| fr.fragment_id).collect();
    let native_ids: BTreeSet<FragmentId> = native_fragments.keys().copied().collect();
    if schedule_ids.len() != fragment_schedules.len() {
        return Err("native fragment_schedules contain duplicate fragment ids".to_string());
    }
    if !native_ids.contains(&root_fragment_id) {
        return Err(format!(
            "native fragment build is missing root fragment id={root_fragment_id}"
        ));
    }
    if native_ids != schedule_ids {
        return Err(format!(
            "native fragment ids {:?} do not match fragment_schedules ids {:?}",
            native_ids, schedule_ids
        ));
    }
    for (&fragment_id, fragment) in native_fragments {
        if fragment.fragment_id != fragment_id {
            return Err(format!(
                "native fragment map key {fragment_id} does not match encoded fragment id {}",
                fragment.fragment_id
            ));
        }
    }
    for (index, boundary) in boundary_schemas.iter().enumerate() {
        if let Some(fragment_id) = boundary.fragment_id {
            let fragment_id = FragmentId::try_from(fragment_id).map_err(|_| {
                format!("boundary schema {index} has negative fragment id={fragment_id}")
            })?;
            if !schedule_ids.contains(&fragment_id) {
                return Err(format!(
                    "boundary schema {index} references missing fragment id={fragment_id}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_native_scheduling_plan(
    fragment_schedules: &[crate::sql::codegen::FragmentSchedulingMetadata],
    native_fragments: &BTreeMap<FragmentId, crate::proto::plan::PlanFragment>,
    plan: &crate::runtime::scheduler::SchedulingPlan,
) -> Result<(), String> {
    let schedule_ids: BTreeSet<FragmentId> =
        fragment_schedules.iter().map(|fr| fr.fragment_id).collect();
    let native_ids: BTreeSet<FragmentId> = native_fragments.keys().copied().collect();
    let placement_ids: BTreeSet<FragmentId> = plan.by_fragment.keys().copied().collect();
    if native_ids != schedule_ids || native_ids != placement_ids {
        return Err(format!(
            "native scheduling plan fragment id set mismatch: native={native_ids:?}, \
             schedules={schedule_ids:?}, placements={placement_ids:?}"
        ));
    }
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

    let root_columns =
        crate::sql::codegen::proto_encode::plan::encoded_fragment_root_output_columns(fragment)?;
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

fn runtime_filter_builder_number_for_instance(
    rf_plan: Option<&RuntimeFilterPlanResult>,
    instance_counts: &BTreeMap<FragmentId, usize>,
) -> BTreeMap<i32, i32> {
    let mut builder_number = BTreeMap::new();
    if let Some(rf_plan) = rf_plan {
        for (build_frag_id, filter_ids) in &rf_plan.build_side_filters {
            let n_builders = instance_counts
                .get(build_frag_id)
                .map(|&n| n as i32)
                .unwrap_or(1);
            for filter_id in filter_ids {
                builder_number.insert(*filter_id, n_builders);
            }
        }
    }
    builder_number
}

// ---------------------------------------------------------------------------
// In-flight instance tracking (per-backend cancellation)
// ---------------------------------------------------------------------------

/// Tracks submitted fragment instances grouped by backend so that, on any
/// failure, cancellation can fan out to every backend that accepted work.
#[derive(Default)]
pub(crate) struct InFlightTracker {
    pub(crate) by_backend: BTreeMap<usize, Vec<UniqueId>>,
}

impl InFlightTracker {
    /// Record that `finst_id` was submitted to `backend_idx`.
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

#[derive(Default)]
struct StandaloneQueryFailureRegistry {
    active: BTreeSet<(i64, i64)>,
    failures: BTreeMap<(i64, i64), String>,
}

fn standalone_query_failures() -> &'static Mutex<StandaloneQueryFailureRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryFailureRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryFailureRegistry::default()))
}

fn query_failure_key(query_id: &UniqueId) -> (i64, i64) {
    (query_id.hi, query_id.lo)
}

pub(crate) fn record_standalone_query_failure(
    query_id: crate::runtime::query_context::QueryId,
    error: String,
) {
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock");
    if guard.active.contains(&key) {
        guard.failures.entry(key).or_insert(error);
    }
}

fn take_standalone_query_failure(query_id: &UniqueId) -> Option<String> {
    standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock")
        .failures
        .remove(&query_failure_key(query_id))
}

struct StandaloneQueryFailureGuard {
    key: (i64, i64),
}

impl StandaloneQueryFailureGuard {
    fn register(query_id: &UniqueId) -> Self {
        let key = query_failure_key(query_id);
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.failures.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryFailureGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.active.remove(&self.key);
        guard.failures.remove(&self.key);
    }
}

#[derive(Default)]
struct StandaloneQueryProfileRegistry {
    active: BTreeSet<(i64, i64)>,
    profiles: BTreeMap<(i64, i64), BTreeMap<(i64, i64), RuntimeProfileTree>>,
}

fn standalone_query_profiles() -> &'static Mutex<StandaloneQueryProfileRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryProfileRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryProfileRegistry::default()))
}

pub(crate) fn record_native_standalone_query_profile_report(
    report: &crate::proto::novarocks::ExecStatusReport,
) -> Result<bool, String> {
    let Some(query_id) = report.query_id.as_ref() else {
        return Ok(false);
    };
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock");
    if !guard.active.contains(&key) {
        return Ok(false);
    }

    let Some(status) = report.status.as_ref() else {
        return Err("ExecStatusReport missing status".to_string());
    };
    if report.done
        && status.code == 0
        && let Some(profile) = report.profile.as_ref()
    {
        let Some(finst_id) = report.fragment_instance_id.as_ref() else {
            return Err("ExecStatusReport missing fragment_instance_id".to_string());
        };
        let native = RuntimeProfileTree::from_proto(profile)?;
        guard
            .profiles
            .entry(key)
            .or_default()
            .insert((finst_id.hi, finst_id.lo), native);
    }
    Ok(true)
}

fn standalone_query_profile_count(query_id: &UniqueId) -> usize {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .get(&query_failure_key(query_id))
        .map(BTreeMap::len)
        .unwrap_or(0)
}

fn take_standalone_query_profiles(query_id: &UniqueId) -> Vec<RuntimeProfileTree> {
    standalone_query_profiles()
        .lock()
        .expect("standalone query profile registry lock")
        .profiles
        .remove(&query_failure_key(query_id))
        .map(|profiles| profiles.into_values().collect())
        .unwrap_or_default()
}

struct StandaloneQueryProfileGuard {
    key: (i64, i64),
}

impl StandaloneQueryProfileGuard {
    fn register(query_id: &UniqueId) -> Self {
        let key = query_failure_key(query_id);
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.profiles.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryProfileGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_profiles()
            .lock()
            .expect("standalone query profile registry lock");
        guard.active.remove(&self.key);
        guard.profiles.remove(&self.key);
    }
}

#[derive(Debug)]
pub(crate) struct SubmitAndFetchResult {
    pub(crate) chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    pub(crate) fragment_profiles: Vec<RuntimeProfileTree>,
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
    submissions: &[(usize, FragmentSubmission)],
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
/// On any submit failure or fetch error, all already-submitted instances are
/// cancelled (fanned out per backend) before the error is returned.
pub(crate) fn submit_and_fetch_loop(
    dispatcher: &Arc<dyn FragmentDispatcher>,
    tracker: &mut InFlightTracker,
    submissions: Vec<(usize, FragmentSubmission)>,
    execution_root_fragment_id: FragmentId,
    root_backend_idx: usize,
    root_finst_id: UniqueId,
    query_id: &UniqueId,
    root_uses_result_buffer: bool,
    timeout_ms: i64,
    expected_root_chunk_schema: Option<&ChunkSchemaRef>,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
    collect_profiles: bool,
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
    let validated_finst_ids = prevalidate_fragment_submissions(
        &submissions,
        *query_id,
        execution_root_fragment_id,
        root_backend_idx,
        root_finst_id,
    )?;

    for ((backend_idx, submission), finst_id) in submissions.into_iter().zip(validated_finst_ids) {
        if let Err(e) = dispatcher.submit_fragment(backend_idx, submission) {
            tracker.cancel_all(dispatcher.as_ref());
            return Err(e);
        }
        crate::service::metrics_http::observe_fragment_scheduled();
        if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
            registry
                .record_scheduled_fragment(backend_idx as crate::runtime::backend_registry::BeId);
        }
        tracker.record_submitted(backend_idx, finst_id.clone());
        crate::runtime::query_state::in_flight_table().register(
            crate::runtime::query_context::QueryId {
                hi: query_id.hi,
                lo: query_id.lo,
            },
            finst_id,
            backend_idx,
        );
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
                    fragment_profiles: Vec::new(),
                });
            }
            if let Some(err) = take_standalone_query_failure(query_id) {
                tracker.cancel_all(dispatcher.as_ref());
                return Err(err);
            }
            if crate::runtime::query_cancel::client_disconnected() {
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
    } else if crate::runtime::query_cancel::client_disconnected() {
        tracker.cancel_all(dispatcher.as_ref());
        return Err("client disconnected".to_string());
    } else if std::time::Instant::now() >= deadline {
        tracker.cancel_all(dispatcher.as_ref());
        return Err(format!("query timed out after {timeout_ms} ms"));
    }

    let (write_commit, write_abort) = if let Some(write) = write_coordinator {
        match wait_for_write_commit_ready(write, tracker, dispatcher.as_ref(), deadline, timeout_ms)
        {
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
        )?
    } else {
        Vec::new()
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
) -> Result<Vec<RuntimeProfileTree>, String> {
    const PROFILE_REPORT_POLL_INTERVAL_MS: i64 = 10;

    if expected_reports == 0 {
        return Ok(Vec::new());
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
        if crate::runtime::query_cancel::client_disconnected() {
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
) -> Result<WriteCommitInput, String> {
    const WRITE_COMMIT_POLL_INTERVAL_MS: i64 = 10;

    loop {
        poll_write_failure_and_cancel(write, tracker, dispatcher)?;

        if crate::runtime::query_cancel::client_disconnected() {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod native_contract_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Array, Decimal128Array, Int32Array};

    use crate::proto::plan as native_plan;
    use crate::runtime::write_coordinator::FragmentExecStatusReport;

    fn schedule(fragment_id: FragmentId) -> crate::sql::codegen::FragmentSchedulingMetadata {
        crate::sql::codegen::FragmentSchedulingMetadata {
            fragment_id,
            has_scan_nodes: false,
            output_kind: crate::sql::codegen::FragmentOutputKind::Result,
            native_scan_ranges: BTreeMap::new(),
            output_columns: Vec::new(),
            boundary_schemas: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
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
            runtime_filter_prober_params: BTreeMap::new(),
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
    ) -> FragmentSubmission {
        FragmentSubmission::new(
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
    ) -> FragmentSubmission {
        FragmentSubmission::new(
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
        submit_count: AtomicUsize,
        fail_on_submit: Option<usize>,
        cancellations: Mutex<Vec<UniqueId>>,
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

        fn with_fetch(
            fail_on_submit: Option<usize>,
            fetch_behavior: TestFetchBehavior,
        ) -> Arc<Self> {
            Arc::new(Self {
                submissions: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fail_on_submit,
                cancellations: Mutex::new(Vec::new()),
                fetch_behavior,
                fetch_count: AtomicUsize::new(0),
                first_fetch: std::sync::atomic::AtomicBool::new(true),
            })
        }
    }

    impl FragmentDispatcher for CapturingDispatcher {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn submit_fragment(
            &self,
            backend_idx: usize,
            submission: FragmentSubmission,
        ) -> Result<(), String> {
            let call = self.submit_count.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_on_submit == Some(call) {
                return Err(format!("native submit failed on call {call}"));
            }
            let finst_id = submission.fragment_instance_id()?;
            self.submissions.lock().unwrap().push((
                backend_idx,
                submission.plan_for_test().fragment_id,
                finst_id,
            ));
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

        fn cancel_fragments(&self, _backend_idx: usize, finst_ids: &[UniqueId]) {
            self.cancellations
                .lock()
                .unwrap()
                .extend_from_slice(finst_ids);
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
        use crate::sql::codegen::FragmentOutputKind;

        validate_fragment_output_kind(1, true, false, false, FragmentOutputKind::Result)
            .expect("result root");
        validate_fragment_output_kind(1, true, true, false, FragmentOutputKind::TerminalWrite)
            .expect("write-only root");
        let err =
            validate_fragment_output_kind(1, true, false, false, FragmentOutputKind::NonTerminal)
                .expect_err("root cannot be nonterminal");
        assert!(err.contains("root fragment 1"), "{err}");

        validate_fragment_output_kind(2, false, false, true, FragmentOutputKind::NonTerminal)
            .expect("non-root producer");
        for output_kind in [
            FragmentOutputKind::Result,
            FragmentOutputKind::TerminalWrite,
        ] {
            let err = validate_fragment_output_kind(2, false, false, true, output_kind)
                .expect_err("producer must be nonterminal");
            assert!(err.contains("producer fragment 2"), "{err}");
        }

        validate_fragment_output_kind(3, false, true, false, FragmentOutputKind::TerminalWrite)
            .expect("non-root terminal writer");
        for output_kind in [FragmentOutputKind::Result, FragmentOutputKind::NonTerminal] {
            let err = validate_fragment_output_kind(3, false, true, false, output_kind)
                .expect_err("terminal writer must use terminal output kind");
            assert!(err.contains("terminal write fragment 3"), "{err}");
        }
    }

    #[test]
    fn native_payload_validation_rejects_schedule_and_encoded_id_drift() {
        let schedules = vec![schedule(7)];
        let err = validate_fragment_schedule_payloads(
            &schedules,
            &BTreeMap::from([(
                8,
                native_plan::PlanFragment {
                    fragment_id: 8,
                    ..Default::default()
                },
            )]),
            8,
            &[],
        )
        .expect_err("schedule/native id drift must fail");
        assert!(err.contains("do not match"), "{err}");

        let err = validate_fragment_schedule_payloads(
            &schedules,
            &BTreeMap::from([(
                7,
                native_plan::PlanFragment {
                    fragment_id: 9,
                    ..Default::default()
                },
            )]),
            7,
            &[],
        )
        .expect_err("map/encoded fragment id drift must fail");
        assert!(err.contains("map key 7"), "{err}");
    }

    #[test]
    fn native_scheduling_plan_validation_rejects_fragment_set_drift_before_side_effects() {
        let schedules = vec![schedule(3), schedule(7)];
        let fragments = BTreeMap::from([
            (
                3,
                native_plan::PlanFragment {
                    fragment_id: 3,
                    ..Default::default()
                },
            ),
            (
                7,
                native_plan::PlanFragment {
                    fragment_id: 7,
                    ..Default::default()
                },
            ),
        ]);
        let plan = crate::runtime::scheduler::SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId { hi: 92_000, lo: 7 },
            root_backend_idx: 0,
        };
        let mut side_effects = 0;

        let err = validate_native_scheduling_plan(&schedules, &fragments, &plan)
            .map(|()| side_effects += 1)
            .expect_err("scheduling-plan fragment set drift must fail");

        assert!(err.contains("fragment id set"), "{err}");
        assert_eq!(side_effects, 0);
    }

    #[test]
    fn native_scheduling_plan_validation_rejects_empty_non_root_placements() {
        let schedules = vec![schedule(3), schedule(7)];
        let fragments = BTreeMap::from([
            (
                3,
                native_plan::PlanFragment {
                    fragment_id: 3,
                    ..Default::default()
                },
            ),
            (
                7,
                native_plan::PlanFragment {
                    fragment_id: 7,
                    ..Default::default()
                },
            ),
        ]);
        let plan = crate::runtime::scheduler::SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(3, Vec::new()), (7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId { hi: 92_000, lo: 7 },
            root_backend_idx: 0,
        };
        let mut side_effects = 0;

        let err = validate_native_scheduling_plan(&schedules, &fragments, &plan)
            .map(|()| side_effects += 1)
            .expect_err("empty non-root placements must fail");

        assert!(err.contains("fragment 3 has no placements"), "{err}");
        assert_eq!(side_effects, 0);
    }

    #[test]
    fn native_scheduling_plan_validation_rejects_placement_fragment_id_drift() {
        let schedules = vec![schedule(7)];
        let fragments = BTreeMap::from([(
            7,
            native_plan::PlanFragment {
                fragment_id: 7,
                ..Default::default()
            },
        )]);
        let plan = crate::runtime::scheduler::SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(8, 7)])]),
            root_finst_id: UniqueId { hi: 92_000, lo: 7 },
            root_backend_idx: 0,
        };
        let mut side_effects = 0;

        let err = validate_native_scheduling_plan(&schedules, &fragments, &plan)
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
    }

    #[test]
    fn submit_failure_cancels_only_native_instances_already_accepted() {
        let inner = CapturingDispatcher::new(Some(2));
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
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
        )
        .expect_err("second native submit must fail");

        assert!(err.contains("native submit failed on call 2"), "{err}");
        assert_eq!(
            *inner.cancellations.lock().unwrap(),
            vec![UniqueId { hi: 92_000, lo: 1 }]
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
        let malformed = FragmentSubmission::new(
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
            )
            .expect_err("native lifecycle failure must surface");
            assert!(err.contains(expected), "{err}");
            let mut canceled = inner.cancellations.lock().unwrap().clone();
            canceled.sort();
            assert_eq!(
                canceled,
                vec![UniqueId { hi, lo: 1 }, UniqueId { hi, lo: 2 }]
            );
        }

        let hi = 96_100;
        let inner = CapturingDispatcher::with_fetch(None, TestFetchBehavior::NotReady);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let err = crate::runtime::query_cancel::with_client_disconnect_signal(disconnected, || {
            let mut tracker = InFlightTracker::default();
            submit_and_fetch_loop(
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
            )
        })
        .expect_err("disconnect must surface");
        assert!(err.contains("client disconnected"), "{err}");
        let mut canceled = inner.cancellations.lock().unwrap().clone();
        canceled.sort();
        assert_eq!(
            canceled,
            vec![UniqueId { hi, lo: 1 }, UniqueId { hi, lo: 2 }]
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
        )
        .expect("native profiles collected");
        assert_eq!(result.fragment_profiles.len(), 1);
        assert_eq!(result.fragment_profiles[0].root.node_id, 7);
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
            &[crate::sql::codegen::OutputColumn {
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
            &[crate::sql::codegen::OutputColumn {
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
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
                children: Vec::new(),
                payload: Some(native_plan::distributed_node::Payload::Physical(
                    native_plan::PlanNode {
                        output_columns: Vec::new(),
                        kind: Some(native_plan::plan_node::Kind::Project(
                            native_plan::ProjectNode {
                                items: vec![native_plan::ProjectItem {
                                    expr: Some(expr::Expr {
                                        r#type: Some(bigint),
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
            runtime_filter_prober_params: BTreeMap::new(),
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

    #[test]
    fn runtime_filter_builder_number_uses_native_instance_counts() {
        let rf = RuntimeFilterPlanResult {
            all_filters: Default::default(),
            build_side_filters: std::collections::HashMap::from([(3, vec![11, 12])]),
            probe_side_filters: Default::default(),
        };
        let numbers =
            runtime_filter_builder_number_for_instance(Some(&rf), &BTreeMap::from([(3, 4_usize)]));
        assert_eq!(numbers, BTreeMap::from([(11, 4), (12, 4)]));
    }
}
