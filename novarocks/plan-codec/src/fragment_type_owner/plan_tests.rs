// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::execution_type_table::decode_execution_type_table_observed;
use crate::value_type_table::LogicalTypeTableLimits;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ValueTypeStorageLifetime,
};
use std::convert::Infallible;

fn limits() -> ExecutionTypeTableLimits {
    ExecutionTypeTableLimits {
        logical: LogicalTypeTableLimits {
            max_definitions: 4096,
            max_expanded_nodes: 4096 * 4096,
            max_string_bytes: 1024 * 1024,
        },
        max_bindings: 1024,
        max_layout_nodes: 4096 * 1024,
        max_layout_string_bytes: 1024 * 1024,
    }
}

#[test]
fn typed_plan_roundtrip_references_original_complete_fragment_owner() {
    let physical = empty_typed_values_plan(ValueType::new(DataType::Int64, false));
    let (catalog, _) = exact_scalar_catalog();
    let mut request = |_: std::alloc::Layout| Ok::<_, AdmissionRefusal<Infallible>>(());
    let encoded = encode_physical_plan_typed_with_host(
        &physical,
        &catalog,
        &NoPhysicalV1PrivateFacts,
        false,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &CodecTestControl,
    )
    .unwrap();
    let bytes = encoded.encode_to_vec();
    assert_eq!(
        plan::DistributedPlan::decode(bytes.as_slice()).unwrap(),
        encoded
    );
    let fragment = &encoded.fragments[0];
    let table = fragment.execution_types.as_ref().unwrap();
    assert!(!table.logical_types.is_empty());
    assert!(
        fragment.output_columns[0]
            .r#type
            .as_ref()
            .unwrap()
            .kind
            .is_none()
    );
    let mut work = CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        table,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    )
    .unwrap();
    let expected = &physical.result_port().unwrap().fields[0].ty;
    let id = fragment.output_columns[0]
        .r#type
        .as_ref()
        .unwrap()
        .value_binding_id
        .unwrap();
    assert_eq!(
        decoded.get_observed(id, &mut work).unwrap().unwrap(),
        expected
    );
}

#[test]
fn typed_plan_every_host_refusal_returns_original_payload_before_later_request() {
    #[derive(Debug, PartialEq)]
    struct Refusal {
        ordinal: usize,
        payload: [u8; 17],
    }
    let physical = empty_typed_values_plan(ValueType::new(DataType::Int64, false));
    let (catalog, _) = exact_scalar_catalog();
    let mut baseline = Vec::new();
    encode_physical_plan_typed_with_host(
        &physical,
        &catalog,
        &NoPhysicalV1PrivateFacts,
        false,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut |layout| {
            baseline.push((layout.size(), layout.align()));
            Ok::<_, AdmissionRefusal<Refusal>>(())
        },
        &CodecTestControl,
    )
    .unwrap();
    assert!(baseline.len() > 8);
    for stop in 0..baseline.len() {
        let mut seen = Vec::new();
        let result = encode_physical_plan_typed_with_host(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            limits(),
            ValueTypeStorageLifetime::pure(),
            &mut |layout| {
                let ordinal = seen.len();
                seen.push((layout.size(), layout.align()));
                if ordinal == stop {
                    Err(AdmissionRefusal::Host(Refusal {
                        ordinal,
                        payload: [0xa7; 17],
                    }))
                } else {
                    Ok(())
                }
            },
            &CodecTestControl,
        );
        assert!(
            matches!(result,Err(ProjectionFailure::Host(Refusal{ordinal,payload})) if ordinal==stop&&payload==[0xa7;17])
        );
        assert_eq!(seen, baseline[..=stop]);
    }
}

#[test]
fn typed_plan_control_refusals_keep_actual_first_prefix() {
    let physical = empty_typed_values_plan(ValueType::new(DataType::Int64, false));
    let (catalog, _) = exact_scalar_catalog();
    let good = UnpivotProjectionControl::default();
    let run = |control: &UnpivotProjectionControl| {
        encode_physical_plan_typed_with_host(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            limits(),
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
            control,
        )
    };
    run(&good).unwrap();
    let baseline = good.calls.lock().unwrap().clone();
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = UnpivotProjectionControl {
                calls: Default::default(),
                reject: Some((stop, cause)),
            };
            assert!(
                matches!(run(&control),Err(ProjectionFailure::Codec(PhysicalEncodeError::Control(actual))) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), baseline[..=stop]);
        }
    }
}

fn empty_typed_values_plan(ty: ValueType) -> PhysicalPlan {
    let fragment_id = FragmentId::new(41);
    let mut builder = FragmentBuilder::new(fragment_id);
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([41; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: OutputPort {
            node,
            columns: Box::from([value]),
        },
        fields: Box::from([ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "payload".into(),
            alias: None,
            value,
            ty,
        }]),
    })
    .unwrap();
    plan.finish_observed(&CodecTestControl).unwrap()
}

#[test]
fn typed_plan_nested_nominal_semantics_survive_without_arrow_metadata() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    };
    let json = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new(
                "payload",
                json,
                ValueTypeStorageLifetime::pure(),
            )]
            .into_boxed_slice(),
        ),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::Struct(
            vec![arrow::datatypes::Field::new(
                "payload",
                DataType::Utf8,
                true,
            )]
            .into(),
        ),
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let expected = ValueType::try_from_value(semantic, layout).unwrap();
    let physical = empty_typed_values_plan(expected.clone());
    let (catalog, _) = exact_scalar_catalog();
    let mut request = |_| Ok::<_, AdmissionRefusal<Infallible>>(());
    let encoded = encode_physical_plan_typed_with_host(
        &physical,
        &catalog,
        &NoPhysicalV1PrivateFacts,
        false,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &CodecTestControl,
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let mut work = CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        fragment.execution_types.as_ref().unwrap(),
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    )
    .unwrap();
    let actual = decoded
        .type_reference_observed(
            fragment.output_columns[0].r#type.as_ref().unwrap(),
            &mut work,
        )
        .unwrap();
    assert_eq!(actual, &expected);
    let ValueKind::Struct(fields) = actual.value_type().kind() else {
        panic!("Struct source")
    };
    assert!(matches!(
        fields[0].value_type().kind(),
        ValueKind::Nominal(ValueLogicalType::Json)
    ));
    let Some(plan::arrow_physical_type::Kind::StructType(structure)) =
        fragment.execution_types.as_ref().unwrap().value_bindings[0]
            .value_layout
            .as_ref()
            .unwrap()
            .kind
            .as_ref()
    else {
        panic!("Struct layout")
    };
    assert!(structure.fields[0].logical_type_id.is_some());
}

#[test]
fn typed_cte_view_borrows_the_actual_producer_partition_and_type_owner() {
    let partition = plan::DataPartition {
        kind: plan::PartitionKind::Hash as i32,
        exprs: Vec::new(),
    };
    let encoded = plan::DistributedPlan {
        root_fragment_id: 9,
        fragments: vec![plan::PlanFragment {
            fragment_id: 3,
            cte_id: Some(3),
            execution_types: Some(plan::ExecutionTypeTable::default()),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                    plan::MultiCastDataStreamSink {
                        sinks: vec![plan::DataStreamSink {
                            dest_node_id: 7,
                            target_fragment_id: 9,
                            output_partition: Some(partition),
                            output_columns: vec![17, 19],
                            limit: None,
                        }],
                    },
                )),
            }),
            ..Default::default()
        }],
        edges: vec![plan::FragmentEdge {
            source_fragment_id: 3,
            target_fragment_id: 9,
            target_exchange_node_id: 7,
            output_partition: plan::PartitionKind::Hash as i32,
            output_slot_ids: vec![17, 19],
            edge_kind: Some(plan::FragmentEdgeKind {
                kind: Some(plan::fragment_edge_kind::Kind::CteMulticast(
                    plan::CteMulticastEdge {
                        cte_id: 3,
                        receive_producer_column_ids: vec![17, 19],
                    },
                )),
            }),
            ..Default::default()
        }],
    };
    let view = physical_typed_cte_consumer_view(&encoded, 0, &CodecTestControl).unwrap();
    assert!(std::ptr::eq(
        view.execution_types,
        encoded.fragments[0].execution_types.as_ref().unwrap()
    ));
    let Some(plan::data_sink::Kind::MultiCastDataStream(sink)) =
        encoded.fragments[0].sink.as_ref().unwrap().kind.as_ref()
    else {
        panic!("multicast")
    };
    assert!(std::ptr::eq(
        view.output_partition,
        sink.sinks[0].output_partition.as_ref().unwrap()
    ));
    assert!(std::ptr::eq(
        view.output_slot_ids,
        encoded.edges[0].output_slot_ids.as_slice()
    ));
    assert!(physical_typed_cte_consumer_view(&encoded, 1, &CodecTestControl).is_err());
    let mut drift = encoded;
    drift.edges[0].output_slot_ids[0] = 23;
    assert!(physical_typed_cte_consumer_view(&drift, 0, &CodecTestControl).is_err());
}

fn typed_cte_fanout_fixture(count: u32) -> plan::DistributedPlan {
    let sinks = (0..count)
        .rev()
        .map(|ordinal| plan::DataStreamSink {
            dest_node_id: ordinal as i32,
            target_fragment_id: ordinal + 100,
            output_partition: Some(plan::DataPartition {
                kind: plan::PartitionKind::Hash as i32,
                exprs: Vec::new(),
            }),
            output_columns: vec![17, 19],
            limit: None,
        })
        .collect();
    plan::DistributedPlan {
        root_fragment_id: 100,
        fragments: vec![plan::PlanFragment {
            fragment_id: 3,
            cte_id: Some(3),
            execution_types: Some(plan::ExecutionTypeTable::default()),
            sink: Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                    plan::MultiCastDataStreamSink { sinks },
                )),
            }),
            ..Default::default()
        }],
        edges: (0..count)
            .map(|ordinal| plan::FragmentEdge {
                source_fragment_id: 3,
                target_fragment_id: ordinal + 100,
                target_exchange_node_id: ordinal as i32,
                output_partition: plan::PartitionType::Hash as i32,
                output_slot_ids: vec![17, 19],
                edge_kind: Some(plan::FragmentEdgeKind {
                    kind: Some(plan::fragment_edge_kind::Kind::CteMulticast(
                        plan::CteMulticastEdge {
                            cte_id: 3,
                            receive_producer_column_ids: vec![17, 19],
                        },
                    )),
                }),
                ..Default::default()
            })
            .collect(),
    }
}

#[test]
fn typed_cte_batch_preserves_edge_order_actual_owners_and_all_control_prefixes() {
    let mut encoded = typed_cte_fanout_fixture(7);
    encoded.edges.insert(2, plan::FragmentEdge::default());
    let baseline = UnpivotProjectionControl::default();
    let views = physical_typed_cte_consumer_views(&encoded, &baseline).unwrap();
    assert_eq!(views.len(), 7);
    let ordinals: Vec<_> = views.iter().map(|(ordinal, _)| *ordinal).collect();
    assert_eq!(ordinals, vec![0, 1, 3, 4, 5, 6, 7]);
    for (ordinal, actual) in views {
        let expected =
            physical_typed_cte_consumer_view(&encoded, ordinal, &CodecTestControl).unwrap();
        assert_eq!(actual.cte_id, expected.cte_id);
        assert_eq!(actual.target_fragment_id, expected.target_fragment_id);
        assert_eq!(
            actual.target_exchange_node_id,
            expected.target_exchange_node_id
        );
        assert!(std::ptr::eq(
            actual.execution_types,
            expected.execution_types
        ));
        assert!(std::ptr::eq(
            actual.output_partition,
            expected.output_partition
        ));
        assert!(std::ptr::eq(
            actual.output_slot_ids,
            expected.output_slot_ids
        ));
        assert!(std::ptr::eq(
            actual.receive_producer_column_ids,
            expected.receive_producer_column_ids
        ));
    }
    let trace = baseline.calls.lock().unwrap().clone();
    for reason in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..trace.len() {
            let control = UnpivotProjectionControl {
                reject: Some((stop, reason)),
                ..Default::default()
            };
            let result = physical_typed_cte_consumer_views(&encoded, &control);
            assert!(
                matches!(result, Err(PhysicalEncodeError::Control(actual)) if actual == reason)
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn typed_cte_batch_observes_subquadratic_fanout_and_preserves_point_error_scope() {
    let work = |count| {
        let control = UnpivotProjectionControl::default();
        let encoded = typed_cte_fanout_fixture(count);
        assert_eq!(
            physical_typed_cte_consumer_views(&encoded, &control)
                .unwrap()
                .len(),
            count as usize
        );
        control
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>()
    };
    let small = work(64);
    let large = work(256);
    // Four times the actual fanout cannot perform a quadratic sixteenfold scan.
    assert!(small > 0 && large < small * 7, "{small} -> {large}");
    let mut encoded = typed_cte_fanout_fixture(2);
    encoded.fragments.extend([
        plan::PlanFragment {
            fragment_id: 91,
            ..Default::default()
        },
        plan::PlanFragment {
            fragment_id: 91,
            ..Default::default()
        },
    ]);
    physical_typed_cte_consumer_view(&encoded, 0, &CodecTestControl).unwrap();
    assert!(physical_typed_cte_consumer_views(&encoded, &CodecTestControl).is_err());
    encoded.edges.clear();
    assert!(
        physical_typed_cte_consumer_views(&encoded, &CodecTestControl)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn opaque_scan_source_copy_preserves_exact_bytes_and_all_request_refusals() {
    use novarocks_proto_models::connector_read as cv;
    let source = plan::ScanSource {
        kind: Some(plan::scan_source::Kind::TypedConnectorRead(
            cv::ConnectorTableScanSource {
                assignments: vec![cv::ScanAssignment {
                    variable: "λ雪".repeat(130),
                    column: None,
                    value_type: Some(cv::ValueType {
                        kind: 4,
                        ..Default::default()
                    }),
                }],
                dynamic_filters: vec![cv::DynamicFilterBinding {
                    filter_id: 17,
                    variable: "λ雪".repeat(130),
                }],
                max_batch_rows: 1024,
                max_batch_bytes: 8192,
                work_source: cv::ScanWorkSource::RuntimeSplits as i32,
                ..Default::default()
            },
        )),
    };
    let control = UnpivotProjectionControl::default();
    let mut request = |_: std::alloc::Layout| Ok::<_, AdmissionRefusal<usize>>(());
    let mut resources = TypeEncodingResources {
        request: &mut request,
        work: CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap(),
        type_policy:
            crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                limits(),
            ),
        storage: ValueTypeStorageLifetime::pure(),
    };
    let copied = source.copy_source_observed(&mut resources).unwrap();
    resources.work.finish().unwrap();
    assert_eq!(copied, source);
    assert!(
        control
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, units)| u64::from(*units))
            .sum::<u64>()
            >= 130 * 5 * 2
    );
    let mut baseline = Vec::new();
    let mut observe = |layout: std::alloc::Layout| {
        baseline.push((layout.size(), layout.align()));
        Ok::<_, AdmissionRefusal<usize>>(())
    };
    let mut resources = TypeEncodingResources {
        request: &mut observe,
        work: CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Encode).unwrap(),
        type_policy:
            crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                limits(),
            ),
        storage: ValueTypeStorageLifetime::pure(),
    };
    source.copy_source_observed(&mut resources).unwrap();
    for stop in 0..baseline.len() {
        let mut seen = Vec::new();
        let mut observe = |layout: std::alloc::Layout| {
            let ordinal = seen.len();
            seen.push((layout.size(), layout.align()));
            if ordinal == stop {
                Err(AdmissionRefusal::Host(ordinal))
            } else {
                Ok(())
            }
        };
        let mut resources = TypeEncodingResources {
            request: &mut observe,
            work: CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Encode).unwrap(),
            type_policy:
                crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                    limits(),
                ),
            storage: ValueTypeStorageLifetime::pure(),
        };
        assert!(
            matches!(source.copy_source_observed(&mut resources),Err(ProjectionFailure::Host(actual)) if actual==stop)
        );
        assert_eq!(seen, baseline[..=stop]);
    }
}

#[test]
fn typed_schema_owners_keep_independent_semantics_even_with_the_same_layout() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef};
    let make = |kind| {
        ValueType::try_from_value(
            ValueTypeRef::try_new(kind, true, ValueTypeStorageLifetime::pure()).unwrap(),
            ValueLayout::try_new(&DataType::Utf8, ValueTypeStorageLifetime::pure()).unwrap(),
        )
        .unwrap()
    };
    let json = make(ValueKind::Nominal(ValueLogicalType::Json));
    let text = make(ValueKind::String);
    let mut request = |_| Ok::<_, AdmissionRefusal<Infallible>>(());
    let mut resources = TypeEncodingResources {
        request: &mut request,
        work: CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Encode).unwrap(),
        type_policy:
            crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                limits(),
            ),
        storage: ValueTypeStorageLifetime::pure(),
    };
    let left = fragment_type_owner::encode_schema(
        [(17, "payload", &json)].into_iter(),
        true,
        &mut resources,
    )
    .unwrap();
    let right = fragment_type_owner::encode_schema(
        [(19, "payload", &text)].into_iter(),
        true,
        &mut resources,
    )
    .unwrap();
    assert_eq!(
        left.columns[0].field.as_ref().unwrap().logical_type_id,
        right.columns[0].field.as_ref().unwrap().logical_type_id
    );
    assert_ne!(left.logical_types, right.logical_types);
    let mut work = CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Decode).unwrap();
    let left_types = crate::value_type_table::decode_logical_type_table_observed(
        &left.logical_types,
        limits().logical,
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        left_types
            .get_observed(
                left.columns[0]
                    .field
                    .as_ref()
                    .unwrap()
                    .logical_type_id
                    .unwrap(),
                &mut work
            )
            .unwrap()
            .unwrap(),
        json.value_type()
    );
    assert_eq!(left.columns[0].slot_id, 17);
    assert_eq!(right.columns[0].slot_id, 19);
}

#[test]
fn runtime_filter_visitor_keeps_single_author_numbering_and_every_refusal() {
    use novarocks_physical_plan::visit_runtime_filter_bindings_observed;
    #[derive(Debug, PartialEq)]
    enum Failure {
        Original(novarocks_physical_plan::RuntimeFilterBindingError),
        Refused(usize),
    }
    impl From<novarocks_physical_plan::RuntimeFilterBindingError> for Failure {
        fn from(error: novarocks_physical_plan::RuntimeFilterBindingError) -> Self {
            Self::Original(error)
        }
    }
    let physical = finish_ordered_join_build_filter_plan();
    let expected = novarocks_physical_plan::runtime_filter_bindings(&physical).unwrap();
    let mut actual = Vec::new();
    let mut visits = 0usize;
    visit_runtime_filter_bindings_observed(
        &physical,
        &mut |binding| {
            actual.push(binding);
            Ok::<_, Failure>(())
        },
        &mut || {
            visits += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(actual, expected);
    for stop in 0..visits {
        let mut count = 0usize;
        let mut emitted = Vec::new();
        let result = visit_runtime_filter_bindings_observed(
            &physical,
            &mut |binding| {
                emitted.push(binding);
                Ok::<_, Failure>(())
            },
            &mut || {
                let current = count;
                count += 1;
                if current == stop {
                    Err(Failure::Refused(current))
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err(Failure::Refused(stop)));
        assert_eq!(count, stop + 1);
        assert_eq!(emitted, expected[..emitted.len()]);
    }
}

#[test]
fn runtime_filter_contract_owns_its_domain_independently_of_fragment_bindings() {
    use novarocks_physical_plan::RuntimeFilterDomain;
    let physical = finish_ordered_join_build_filter_plan();
    let fragment = &physical.fragments()[&FragmentId::new(42)];
    let filter = physical.runtime_filters().values().next().unwrap();
    let RuntimeFilterDomain::Ordered { key, .. } = &filter.domain else {
        panic!("ordered source")
    };
    let mut request = |_| Ok::<_, AdmissionRefusal<Infallible>>(());
    let mut resources = TypeEncodingResources {
        request: &mut request,
        work: CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Encode).unwrap(),
        type_policy:
            crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                limits(),
            ),
        storage: ValueTypeStorageLifetime::pure(),
    };
    let synthetic = PreparedSyntheticTypes::prepare(fragment, &mut resources).unwrap();
    let original=crate::physical_encode::fragment_type_owner::original_requests::PreparedOriginalRequests::prepare(fragment,physical.constants(),&mut resources).unwrap();
    let owner =
        PreparedFragmentTypes::prepare(
            fragment,
            &synthetic,
            &original,
            physical.result_port(),
            &NoPhysicalV1PrivateFacts,
            crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::Explicit(
                limits(),
            ),
            &mut |layout| (resources.request)(layout),
            &mut resources.work,
        )
        .unwrap();
    let context = ExpressionEncodingContext {
        writer_calls: None,
        constants: physical.constants(),
        control: &CodecTestControl,
        types: Some(&owner),
    };
    // This is the standalone genuine domain codec; the separate existing
    // operator preflight still refuses ordered JoinBuildKey capability.
    let contract = encode_runtime_filter_contract(filter, context, &mut resources).unwrap();
    let domain = contract.logical_domain.unwrap();
    let table = domain.execution_types.unwrap();
    let mut work = CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        &table,
        limits(),
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        decoded
            .type_reference_observed(domain.value_type.as_ref().unwrap(), &mut work)
            .unwrap(),
        &key.ty
    );
    let Some(plan::runtime_filter_contract::Kind::Ordered(ordered)) = contract.kind else {
        panic!("ordered contract")
    };
    assert_eq!(ordered.keys[0].r#type, domain.value_type);
}

#[test]
fn pure_fe_typed_encoder_matches_explicit_owner_tables_and_original_control_prefixes() {
    let (catalog, _) = exact_scalar_catalog();
    for physical in [
        empty_typed_values_plan(ValueType::new(DataType::Int64, false)),
        empty_typed_values_plan(ValueType::new(DataType::Int64, true)),
    ] {
        let pure = encode_physical_plan_typed_pure(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            &CodecTestControl,
        )
        .unwrap();
        let explicit = encode_physical_plan_typed_with_host(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            limits(),
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
            &CodecTestControl,
        )
        .unwrap();
        assert_eq!(pure, explicit);
        assert!(
            pure.fragments
                .iter()
                .all(|fragment| fragment.execution_types.is_some())
        );
    }
    // The production FE is pure. Its genuine constant plans must keep their
    // pools and complete tables, while the unused expansion Host API states
    // its unsupported IPC boundary rather than claiming a construction port.
    for physical in [finish_limit_chain_plan(3), finish_split_topn_plan(false)] {
        let pure = encode_physical_plan_typed_pure(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            &CodecTestControl,
        )
        .unwrap();
        assert!(
            pure.fragments
                .iter()
                .any(|fragment| !fragment.constants.is_empty())
        );
        assert_eq!(
            plan::DistributedPlan::decode(pure.encode_to_vec().as_slice()).unwrap(),
            pure
        );
        let explicit = encode_physical_plan_typed_with_host(
            &physical,
            &catalog,
            &NoPhysicalV1PrivateFacts,
            false,
            limits(),
            ValueTypeStorageLifetime::pure(),
            &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
            &CodecTestControl,
        );
        assert!(matches!(
            explicit,
            Err(ProjectionFailure::Codec(PhysicalEncodeError::Invalid(message)))
                if message == "constant IPC encoding requires its explicit construction observer"
        ));
    }
    let physical = finish_limit_chain_plan(2);
    let baseline = UnpivotProjectionControl::default();
    encode_physical_plan_typed_pure(
        &physical,
        &catalog,
        &NoPhysicalV1PrivateFacts,
        false,
        &baseline,
    )
    .unwrap();
    let trace = baseline.calls.lock().unwrap().clone();
    for reason in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..trace.len() {
            let control = UnpivotProjectionControl {
                reject: Some((stop, reason)),
                ..Default::default()
            };
            let result = encode_physical_plan_typed_pure(
                &physical,
                &catalog,
                &NoPhysicalV1PrivateFacts,
                false,
                &control,
            );
            assert!(
                matches!(result, Err(PhysicalEncodeError::Control(actual)) if actual == reason)
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn genuine_original_request_preserves_constant_pool_and_optional_constraint_in_its_owner() {
    use novarocks_physical_plan::{
        ConstantPoolId, ConstantReference, PhysicalCallDefinition, PhysicalCallRequest,
        StaticFunctionArgument,
    };
    let (_, function) = exact_scalar_catalog();
    let ty = function.result_type.clone();
    let constants = fixture_i64_constants();
    let pool_id = ConstantPoolId::new(0);
    let reference = ConstantReference {
        pool: pool_id,
        ordinal: 2,
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(41));
    let node = builder.reserve_node_id().unwrap();
    let input = builder
        .add_expression(node, ty.clone(), ExprKind::Constant(reference))
        .unwrap();
    let call = builder
        .add_expression(
            node,
            ty.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([input]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([call])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    // These are the actual source request facts, authored independently of
                    // the selected result and preserving a supplied literal ordinal.
                    arguments: Box::from([StaticFunctionArgument::Value {
                        value_type: ty.clone(),
                        constant: Some(reference),
                    }]),
                    logical_argument_count: 1,
                    expected_result_type: None,
                    constant_policy: codec_constant_policy(),
                },
            )],
            &CodecTestControl,
        )
        .unwrap();
    let mut request = |_: std::alloc::Layout| Ok::<_, AdmissionRefusal<Infallible>>(());
    let mut resources = TypeEncodingResources {
        request: &mut request,
        work: CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Encode).unwrap(),
        type_policy: fragment_type_owner::ExecutionTypeEncodingPolicy::PureGrammar,
        storage: ValueTypeStorageLifetime::pure(),
    };
    let original = fragment_type_owner::original_requests::PreparedOriginalRequests::prepare(
        &fragment,
        &constants,
        &mut resources,
    )
    .unwrap();
    let synthetic = PreparedSyntheticTypes::prepare(&fragment, &mut resources).unwrap();
    let mut owner = PreparedFragmentTypes::prepare(
        &fragment,
        &synthetic,
        &original,
        None,
        &NoPhysicalV1PrivateFacts,
        resources.type_policy,
        &mut |layout| (resources.request)(layout),
        &mut resources.work,
    )
    .unwrap();
    let requests = owner.emit_original_requests(&mut resources).unwrap();
    assert_eq!(requests.entries.len(), 1);
    let entry = &requests.entries[0];
    assert_eq!(entry.request_id, Some(0));
    assert_eq!(entry.expected_result_value_type_id, None);
    assert_eq!(entry.logical_argument_count, Some(1));
    let Some(novarocks_proto_models::call_request::original_function_argument::Kind::Value(
        argument,
    )) = &entry.arguments[0].kind
    else {
        panic!("value source")
    };
    assert_eq!(argument.constant.as_ref().unwrap().pool_id, Some(0));
    assert_eq!(argument.constant.as_ref().unwrap().row_ordinal, 2);
    let (records, source) = owner.emit_pure_constants(&mut resources).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, 0);
    assert_eq!(records[0].source_field_id, Some(0));
    let shape = source.shape.as_ref().expect("genuine source shape");
    assert!(shape.value_types.is_empty() && shape.logical_types.is_empty());
    assert_eq!(source.field_facts.len(), shape.fields.len());
    assert!(source.field_facts.iter().all(|facts| {
        shape
            .fields
            .iter()
            .any(|field| facts.field_id == Some(field.id))
    }));
    assert!(
        shape
            .fields
            .iter()
            .all(|field| field.logical_type_id.is_none() && field.value_binding_id.is_none())
    );
    let table = owner.take_wire();
    let mut work = CompileCheckpoints::try_new(&CodecTestControl, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        &table,
        ExecutionTypeTableLimits::for_validated_root_count(table.value_bindings.len()).unwrap(),
        ValueTypeStorageLifetime::pure(),
        &mut |_| Ok::<_, AdmissionRefusal<Infallible>>(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(
        decoded
            .get_observed(records[0].value_type_id.unwrap(), &mut work)
            .unwrap()
            .unwrap(),
        constants.entries().get(&pool_id).unwrap().value_type()
    );
    let mut reader = arrow::ipc::reader::StreamReader::try_new(
        std::io::Cursor::new(&records[0].arrow_ipc),
        None,
    )
    .unwrap();
    assert_eq!(
        reader.next().unwrap().unwrap().column(0).as_ref(),
        constants.entries().get(&pool_id).unwrap().array().as_ref()
    );
}
