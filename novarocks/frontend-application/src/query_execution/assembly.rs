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

//! Role-neutral native submission assembly helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[cfg(test)]
use arrow::array::{ArrayRef, RecordBatchOptions};
#[cfg(test)]
use arrow::datatypes::{DataType, Schema};
#[cfg(test)]
use arrow::record_batch::RecordBatch;

use crate::query_execution::artifact::FragmentId;
#[cfg(test)]
use crate::query_execution::attempt_plan_facts::PlanOutputColumn;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::schedule::SchedulingPlan;
#[cfg(test)]
use novarocks_execution::exec::chunk::Chunk;
use tracing::debug;

#[cfg(test)]
pub(crate) fn align_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[PlanOutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| align_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

#[cfg(test)]
fn align_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[PlanOutputColumn],
) -> Result<Chunk, String> {
    let row_count = chunk.record_batch().num_rows();
    if chunk.record_batch().num_columns() != output_columns.len() {
        return Err(format!(
            "typed root result column count mismatch: chunk has {}, output metadata has {}",
            chunk.record_batch().num_columns(),
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
        let slot = &chunk.chunk_schema().slots()[idx];
        let source = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
            slot.typed_binding().clone(),
        )
        .map_err(|error| error.to_string())?;
        if !source.same_semantics(&output.ty) {
            return Err(
                "typed root result source complete value type differs from output declaration"
                    .into(),
            );
        }
        let array = align_typed_root_array(
            idx,
            chunk.record_batch().column(idx).clone(),
            output.data_type(),
        )?;
        let dictionary_carrier =
            result_string_dictionary_carrier(output.data_type(), array.data_type());
        if matches!(output.data_type(), DataType::Utf8 | DataType::LargeUtf8)
            && matches!(array.data_type(), DataType::Dictionary(_, _))
            && !dictionary_carrier
        {
            return Err("typed root result string dictionary carrier is unsupported".into());
        }
        if !dictionary_carrier
            && let Err(mismatch) = novarocks_execution::exec::chunk::type_compatibility::check_exact(
                output.data_type(),
                array.data_type(),
            )
        {
            return Err(format!(
                "typed root result column {idx} type mismatch: output={:?} chunk={:?} ({:?})",
                output.data_type(),
                array.data_type(),
                mismatch.kind
            ));
        }
        let canonical = arrow::datatypes::Field::new(
            &output.name,
            output.data_type().clone(),
            output.nullable(),
        );
        let canonical = if dictionary_carrier {
            canonical.with_data_type(array.data_type().clone())
        } else {
            canonical
        };
        fields.push(if slot.field() == &canonical {
            Arc::clone(slot.field_ref())
        } else {
            Arc::new(canonical)
        });
        arrays.push(array);
    }

    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| format!("align typed root result batch failed: {e}"))?;
    // Retain the signed value binding while projecting the visible alias.
    let slots = chunk
        .chunk_schema()
        .slots()
        .iter()
        .zip(batch.schema().fields())
        .zip(output_columns)
        .map(|((slot, field), output)| {
            novarocks_execution::exec::chunk::ChunkSlotSchema::try_new_typed(
                slot.slot_id(),
                Arc::clone(field),
                output.ty.typed_binding().clone(),
                slot.unique_id(),
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema = Arc::new(novarocks_execution::exec::chunk::ChunkSchema::try_new(
        slots,
    )?);
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

#[cfg(test)]
fn align_typed_root_array(
    idx: usize,
    array: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    if result_string_dictionary_carrier(output_type, array.data_type()) {
        return Ok(array);
    }
    if novarocks_execution::exec::chunk::type_compatibility::check_exact(
        output_type,
        array.data_type(),
    )
    .is_ok()
    {
        if array.data_type() != output_type
            && matches!(
                output_type,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::Map(_, _)
                    | DataType::Struct(_)
            )
        {
            // All original Field/cache domains were validated before this
            // metadata-only completion. Existing buffers remain unchanged.
            return novarocks_execution::exec::chunk::type_compatibility::retag_column(
                &array,
                output_type,
            )
            .map_err(|_| "typed root result nested metadata completion failed".into());
        }
        return Ok(array);
    }
    if !same_unit_timestamp_metadata_mismatch(output_type, array.data_type()) {
        return Ok(array);
    }
    novarocks_execution::exec::chunk::type_compatibility::retag_column(&array, output_type).map_err(|mismatch| {
        format!(
            "typed root result column {idx} timestamp metadata retag failed: output={:?} chunk={:?} ({:?})",
            output_type,
            array.data_type(),
            mismatch.kind
        )
    })
}

/// ResultBufferSink::accepts_encoded_column explicitly retains Int32 string
/// dictionaries (Utf8 or LargeUtf8). This is a result-boundary carrier rule;
/// it neither broadens generic type compatibility nor hydrates the column.
#[cfg(test)]
fn result_string_dictionary_carrier(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::Utf8, DataType::Dictionary(key, value)) => {
            key.as_ref() == &DataType::Int32
                && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
        }
        (DataType::LargeUtf8, DataType::Dictionary(key, value)) => {
            key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::LargeUtf8
        }
        _ => false,
    }
}

#[cfg(test)]
fn same_unit_timestamp_metadata_mismatch(expected: &DataType, actual: &DataType) -> bool {
    matches!(
        (expected, actual),
        (DataType::Timestamp(expected_unit, _), DataType::Timestamp(actual_unit, _))
            if expected_unit == actual_unit
    )
}

// Index each plain `Stream` producer fragment to its single outgoing stream
// edge. This is an infallible projection of the sealed edge set: the planner
/// The exact routing fields submission needs from an encoded completed plan.
/// Partition expressions and slot layouts stay in the
/// native template and are never reconstructed during placement.
#[derive(Clone)]
pub(crate) struct RouterSubmissionEdge {
    pub(crate) source_fragment_id: FragmentId,
    pub(crate) target_fragment_id: FragmentId,
    pub(crate) target_exchange_node_id: i32,
    pub(crate) router_group_id: i32,
    pub(crate) route_id: [u8; 32],
}

// This is an infallible projection of the sealed edge set: the planner seal
// (`validate_source_edge_shape`) already owns plain/router mix rejection and the
// per-(source, group) branch_id / branch_kind / target-exchange uniqueness that
// this used to re-check, so grouping here only collects the sealed branches. Re-
// adding a shape check here would duplicate a planner-owned decision (guarded by
// `planner_topology_contract`).
pub fn group_router_edges_by_source(
    edges: &[RouterSubmissionEdge],
) -> BTreeMap<(FragmentId, i32), Vec<&RouterSubmissionEdge>> {
    let mut grouped: BTreeMap<(FragmentId, i32), Vec<&RouterSubmissionEdge>> = BTreeMap::new();
    for edge in edges {
        grouped
            .entry((edge.source_fragment_id, edge.router_group_id))
            .or_default()
            .push(edge);
    }
    grouped
}
pub fn ensure_native_fragment_sink_supported(
    fragment_id: FragmentId,
    is_root: bool,
    has_stream_edge: bool,
    has_router_edges: bool,
    has_cte_id: bool,
) -> Result<(), String> {
    if is_root || has_stream_edge || has_router_edges || has_cte_id {
        return Ok(());
    }

    let dynamic_sink = "unowned fragment sink";
    Err(format!(
        "native submission cannot encode {dynamic_sink} for fragment {fragment_id}; \
         the completed plan must declare a supported static sink"
    ))
}

pub(crate) fn validate_artifact_fragment_sets(
    expected: &BTreeSet<FragmentId>,
    native_bundle: &NativeFragmentAttachment,
    scheduling: &SchedulingPlan,
) -> Result<(), String> {
    let native = native_bundle.fragment_ids().collect::<BTreeSet<_>>();
    if &native != expected {
        return Err(fragment_set_mismatch("native", expected, &native));
    }
    let scheduled = scheduling.fragment_ids().collect::<BTreeSet<_>>();
    if &scheduled != expected {
        return Err(fragment_set_mismatch("scheduled", expected, &scheduled));
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

pub(crate) fn validate_scheduling_placements(plan: &SchedulingPlan) -> Result<(), String> {
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

/// Applies only placement to an already-encoded provider-neutral connector
/// source. The traversal deliberately has no provider branch and never
pub fn patch_native_change_stream_router_sink(
    fragment: &mut novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&RouterSubmissionEdge],
) -> Result<(), String> {
    let mut patched_fragment = fragment.clone();
    patch_native_change_stream_router_sink_in_place(
        &mut patched_fragment,
        fragment_id,
        router_group_id,
        branch_edges,
    )?;
    *fragment = patched_fragment;
    Ok(())
}

fn patch_native_change_stream_router_sink_in_place(
    fragment: &mut novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    router_group_id: i32,
    branch_edges: &[&RouterSubmissionEdge],
) -> Result<(), String> {
    if branch_edges.is_empty() {
        return Err("native Iceberg change-stream router sink has no branch edges".to_string());
    }
    let router = match fragment.sink.as_mut().and_then(|sink| sink.kind.as_mut()) {
        Some(novarocks_proto_models::plan::data_sink::Kind::ChangeStreamRouter(router)) => router,
        _ => {
            return Err(format!(
                "fragment {fragment_id} is router source for group {router_group_id} but native \
                 fragment payload is missing CHANGE_STREAM_ROUTER_SINK"
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

    let mut edge_route_ids = BTreeSet::new();
    for edge in branch_edges {
        if edge.router_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge.router_group_id
            ));
        }
        if !edge_route_ids.insert(edge.route_id) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate opaque route id"
            ));
        }
    }

    let mut encoded_route_ids = BTreeSet::new();
    for route in &router.routes {
        let route_id: [u8; 32] = route.route_id.as_slice().try_into().map_err(|_| {
            format!("native row-mutation router source={fragment_id} group={router_group_id} has non-32-byte route id")
        })?;
        if !encoded_route_ids.insert(route_id) {
            return Err(format!(
                "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
                 has duplicate encoded opaque route id"
            ));
        }
    }

    if encoded_route_ids != edge_route_ids {
        return Err(format!(
            "native Iceberg change-stream router source={fragment_id} group={router_group_id} \
             route id set mismatch: encoded={encoded_route_ids:?}, branch_edges={edge_route_ids:?}"
        ));
    }

    for edge in branch_edges {
        if edge.router_group_id != router_group_id {
            return Err(format!(
                "native Iceberg change-stream router source={} expected group={} but edge uses group={}",
                fragment_id, router_group_id, edge.router_group_id
            ));
        }

        let route = router
            .routes
            .iter_mut()
            .find(|route| route.route_id.as_slice() == edge.route_id)
            .ok_or_else(|| {
                format!(
                    "native row-mutation router source={} group={} has no matching route",
                    fragment_id, router_group_id
                )
            })?;
        route.target_fragment_id = edge.target_fragment_id;
        route.target_exchange_node_id = edge.target_exchange_node_id;

        if route.output_partition.is_none() {
            return Err(format!(
                "native row-mutation router source={} group={} missing output_partition from native encoder",
                fragment_id, router_group_id
            ));
        }
    }

    debug!(
        "patched native Iceberg change-stream router sink: fragment={} group={} branches={}",
        fragment_id,
        router_group_id,
        branch_edges.len()
    );
    Ok(())
}

/// Route identities borrowed from the completed native plan's CTE edges.
/// Partition expressions and producer slots remain inside that exact fragment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CteMulticastConsumer {
    pub(crate) fragment_id: FragmentId,
    pub(crate) exchange_node_id: i32,
}

pub(crate) fn validate_native_cte_multicast_sink(
    fragment: &novarocks_proto_models::plan::PlanFragment,
    fragment_id: FragmentId,
    cte_id: u32,
    consumers: &[CteMulticastConsumer],
) -> Result<(), String> {
    if fragment.fragment_id != fragment_id || fragment.cte_id != Some(cte_id) {
        return Err("CTE fragment identity differs from its completed plan".into());
    }
    if fragment.execution_types.is_none() {
        return Err("CTE fragment has no execution type owner".into());
    }
    let Some(novarocks_proto_models::plan::data_sink::Kind::MultiCastDataStream(multicast)) =
        fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref())
    else {
        return Err("CTE fragment has no encoded multicast sink".into());
    };
    if consumers.is_empty() || consumers.len() != multicast.sinks.len() {
        return Err(format!(
            "CTE fragment {cte_id} has inconsistent consumer count"
        ));
    }
    let mut remaining = consumers.iter().copied().collect::<BTreeSet<_>>();
    if remaining.len() != consumers.len() {
        return Err("CTE fragment has duplicate completed consumer routes".into());
    }
    for sink in &multicast.sinks {
        let consumer = CteMulticastConsumer {
            fragment_id: sink.target_fragment_id,
            exchange_node_id: sink.dest_node_id,
        };
        if sink.output_partition.is_none() || !remaining.remove(&consumer) {
            return Err("CTE encoded sink differs from its completed consumer route".into());
        }
    }
    // No partition, slot, or type is regenerated here. The codec validated the
    // exact edge mapping while borrowing this fragment's own execution table.
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    #[cfg(test)]
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::ConnectorWriteRouteId;

    use super::*;
    use crate::query_execution::schedule::FragmentInstancePlacement;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_proto_models::plan as native_plan;
    use novarocks_types::SlotId;
    use novarocks_types::UniqueId;

    fn placement(fragment_id: FragmentId, instance_lo: i64) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index: 0,
            finst_id: UniqueId::new(92_000, instance_lo),
            backend_idx: 0,
            endpoint: RuntimeEndpoint::new("10.0.0.2", 9030).unwrap(),
            scan_ranges: BTreeMap::new(),
        }
    }

    fn router_edge(target_fragment_id: FragmentId) -> RouterSubmissionEdge {
        RouterSubmissionEdge {
            source_fragment_id: 1,
            target_fragment_id,
            target_exchange_node_id: 77,
            router_group_id: 7,
            route_id: ConnectorWriteRouteId::from_bytes([7; 32]).to_bytes(),
        }
    }

    fn router_fragment() -> native_plan::PlanFragment {
        native_plan::PlanFragment {
            fragment_id: 1,
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::ChangeStreamRouter(
                    native_plan::ChangeStreamRouterSink {
                        group_id: 7,
                        effect_output_ordinal: 0,
                        routes: vec![native_plan::ChangeStreamBranchRoute {
                            target_fragment_id: 0,
                            target_exchange_node_id: -1,
                            output_partition_ordinals: Vec::new(),
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            }),
                            route_id: vec![7; 32],
                            write_target_ordinal: 0,
                            accepted_effects: vec![native_plan::RowMutationEffect::Delete as i32],
                            input_ordinals: vec![0],
                        }],
                    },
                )),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn validates_native_sink_and_output_roles() {
        ensure_native_fragment_sink_supported(7, false, true, false, false)
            .expect("stream sink is supported");
        ensure_native_fragment_sink_supported(8, false, false, true, false)
            .expect("router sink is supported");
        ensure_native_fragment_sink_supported(9, false, false, false, true)
            .expect("CTE multicast sink is supported");
        assert!(
            ensure_native_fragment_sink_supported(10, false, false, false, false)
                .expect_err("unowned dynamic sink must be rejected")
                .contains("unowned fragment sink")
        );
    }

    #[test]
    fn rejects_invalid_scheduling_placements_before_assembly() {
        let empty = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(3, Vec::new()), (7, vec![placement(7, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        assert!(
            validate_scheduling_placements(&empty)
                .expect_err("empty placements must fail")
                .contains("fragment 3 has no placements")
        );

        let drift = SchedulingPlan {
            root_fragment_id: 7,
            by_fragment: BTreeMap::from([(7, vec![placement(8, 7)])]),
            root_finst_id: UniqueId::new(92_000, 7),
            root_backend_idx: 0,
        };
        let error =
            validate_scheduling_placements(&drift).expect_err("placement id drift must fail");
        assert!(error.contains("map key 7"), "{error}");
        assert!(error.contains("fragment_id 8"), "{error}");
    }

    #[test]
    fn aligns_typed_root_fields_and_rejects_decimal_drift() {
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
            &[PlanOutputColumn {
                name: "col1".to_string(),
                ty: crate::query_execution::attempt_plan_facts::declared_test_value(
                    DataType::Int32,
                    true,
                    novarocks_physical_plan::ResultValueDomain::Plain,
                )
                .unwrap(),
            }],
        )
        .unwrap();
        assert_eq!(aligned[0].record_batch().schema().field(0).name(), "col1");
        assert!(aligned[0].record_batch().schema().field(0).is_nullable());

        let decimal = Decimal128Array::from(vec![Some(100_i128)])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wire_price",
            DataType::Decimal128(38, 2),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(decimal)]).unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(8)])
                .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        assert!(
            align_fetch_chunks_to_output_columns(
                vec![chunk],
                &[PlanOutputColumn {
                    name: "price".to_string(),
                    ty: crate::query_execution::attempt_plan_facts::declared_test_value(
                        DataType::Decimal128(20, 2),
                        false,
                        novarocks_physical_plan::ResultValueDomain::Plain
                    )
                    .unwrap(),
                }],
            )
            .expect_err("decimal precision drift must fail")
            .contains("complete value type differs")
        );
    }

    #[test]
    fn cte_freeze_keeps_the_encoded_owner_and_rejects_missing_routes() {
        let fragment = native_plan::PlanFragment {
            fragment_id: 1,
            cte_id: Some(1),
            execution_types: Some(Default::default()),
            sink: Some(native_plan::DataSink {
                kind: Some(native_plan::data_sink::Kind::MultiCastDataStream(
                    native_plan::MultiCastDataStreamSink {
                        sinks: vec![native_plan::DataStreamSink {
                            dest_node_id: 77,
                            target_fragment_id: 2,
                            output_partition: Some(native_plan::DataPartition {
                                kind: native_plan::PartitionKind::Unpartitioned as i32,
                                exprs: vec![],
                            }),
                            output_columns: vec![10, 10],
                            limit: None,
                        }],
                    },
                )),
            }),
            ..Default::default()
        };
        let consumer = CteMulticastConsumer {
            fragment_id: 2,
            exchange_node_id: 77,
        };
        let before = fragment.clone();
        validate_native_cte_multicast_sink(&fragment, 1, 1, &[consumer]).unwrap();
        assert_eq!(fragment, before);
        assert!(
            validate_native_cte_multicast_sink(
                &fragment,
                1,
                1,
                &[CteMulticastConsumer {
                    fragment_id: 3,
                    ..consumer
                }]
            )
            .is_err()
        );
        let mut missing_owner = fragment;
        missing_owner.execution_types = None;
        assert!(validate_native_cte_multicast_sink(&missing_owner, 1, 1, &[consumer]).is_err());
    }

    #[test]
    fn rejects_router_validation_drift_without_mutating_the_fragment() {
        let mut fragment = router_fragment();
        let before = fragment.clone();
        let edge = router_edge(2);
        let error = patch_native_change_stream_router_sink(&mut fragment, 1, 8, &[&edge])
            .expect_err("router group drift must fail before patching");
        assert!(error.contains("expected group=8"), "{error}");
        assert_eq!(fragment, before, "router patch must be atomic");
    }

    #[test]
    fn root_alignment_checks_explicit_domain_and_ignores_source_attributes() {
        use arrow::array::BinaryArray;
        use novarocks_execution::exec::chunk::ChunkSlotSchema;
        use novarocks_physical_plan::ResultValueDomain as D;
        use novarocks_type_contract::ValueTypeStorageLifetime as S;
        let ty = super::super::attempt_plan_facts::declared_test_value(
            DataType::Binary,
            false,
            D::Bitmap,
        )
        .unwrap();
        let field =
            Arc::new(Field::new("wire", DataType::Binary, false).with_metadata(
                [("nr_logical_type".into(), "unknown-source-attribute".into())].into(),
            ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field.clone()])),
            vec![Arc::new(BinaryArray::from(vec![b"payload".as_slice()]))],
        )
        .unwrap();
        let slot = ChunkSlotSchema::try_new_typed(
            SlotId::new(9),
            field,
            ty.typed_binding().clone(),
            None,
            S::pure(),
        )
        .unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::new(ChunkSchema::try_new(vec![slot]).unwrap()),
        )
        .unwrap();
        for domain in [D::Hll, D::Object, D::Percentile, D::Plain] {
            let output = PlanOutputColumn {
                name: "declared".into(),
                ty: super::super::attempt_plan_facts::declared_test_value(
                    DataType::Binary,
                    false,
                    domain,
                )
                .unwrap(),
            };
            assert!(
                align_fetch_chunks_to_output_columns(vec![chunk.clone()], &[output])
                    .unwrap_err()
                    .contains("complete value type differs")
            );
        }
        let output = PlanOutputColumn {
            name: "declared".into(),
            ty: ty.clone(),
        };
        let aligned = align_fetch_chunks_to_output_columns(vec![chunk], &[output]).unwrap();
        assert_eq!(
            aligned[0].chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        assert!(
            aligned[0]
                .record_batch()
                .schema()
                .field(0)
                .metadata()
                .is_empty()
        );
    }
}
