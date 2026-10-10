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
use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    late: Mutex<Option<CompileControlError>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() > 1
            && let Some(cause) = *self.late.lock().unwrap()
        {
            assert_eq!(trace.len(), 2, "callback after originating refusal");
            return Err(cause);
        }
        Ok(())
    }
}
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: usize::MAX,
        max_value_references: usize::MAX,
        max_list_items: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        },
    }
}
fn model() -> Model {
    let mut model = Model {
        inputs: 2,
        refs: 3,
        items: 4,
        ..Model::default()
    };
    model
        .layout_request(Layout::array::<u64>(2).unwrap(), 2)
        .unwrap();
    model
}
fn axes(facts: &NodeProjectionFacts) -> [usize; 7] {
    [
        facts.input_node_count,
        facts.value_reference_count,
        facts.list_item_count,
        facts.allocation_requests_upper_bound,
        facts.allocation_request_bytes_upper_bound,
        facts.coexisting_source_and_request_bytes_upper_bound,
        facts.cumulative_work_upper_bound,
    ]
}
#[test]
fn containing_parent_refuses_known_axes_before_pending_observation() {
    let model = model();
    // Independent Layout and hand arithmetic, without calling the facts author.
    let expected = [2, 3, 4, 2, 32, 96, 256 + 6 * 32 + 3 * 34 + 32 * 4];
    for pending in [0, 254, 255] {
        for late in CAUSES {
            for axis in 0..7 {
                let control = Control::default();
                *control.late.lock().unwrap() = Some(late);
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let mut calls = 0;
                let result = model.facts_in(
                    64,
                    1,
                    limits(),
                    &mut |facts| {
                        calls += 1;
                        assert_eq!(axes(facts), expected);
                        let cap = expected[axis] - 1;
                        if axes(facts)[axis] > cap {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    },
                    &mut work,
                );
                assert!(matches!(
                    finish(work, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
                assert_eq!(calls, 1);
                assert_eq!(*control.trace.lock().unwrap(), [0]);
            }
        }
    }
}
#[test]
fn parent_snapshot_replaces_contribution_without_an_observer_or_scope() {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for _ in 0..255 {
        work.step().unwrap();
    }
    let mut model = model();
    let mut latest = None;
    let mut parent = |facts: &NodeProjectionFacts| {
        latest = Some(*facts);
        Ok(())
    };
    let first = model.admit_in(64, 1, limits(), &mut parent).unwrap();
    assert_eq!(first.allocation_request_bytes_upper_bound, 32);
    model.request::<u8>(5, 1).unwrap();
    let second = model.admit_in(64, 1, limits(), &mut parent).unwrap();
    assert_eq!(second.allocation_requests_upper_bound, 3);
    assert_eq!(second.allocation_request_bytes_upper_bound, 37);
    assert_eq!(second.coexisting_source_and_request_bytes_upper_bound, 101);
    assert_eq!(latest, Some(second));
    assert_eq!(*control.trace.lock().unwrap(), [0]);
    work.step().unwrap();
    work.finish().unwrap();
    assert_eq!(*control.trace.lock().unwrap(), [0, 256, 0]);
}
#[test]
fn accepted_parent_preserves_plain_completed_trace_and_first_refusal() {
    let model = model();
    let plain = Control::default();
    let mut work = CompileCheckpoints::try_new(&plain, CompilePhase::Decode).unwrap();
    let expected = model.facts(64, 1, limits(), &mut work).unwrap();
    work.finish().unwrap();
    let parent = Control::default();
    let mut work = CompileCheckpoints::try_new(&parent, CompilePhase::Decode).unwrap();
    let actual = model
        .facts_in(64, 1, limits(), &mut |_| Ok(()), &mut work)
        .unwrap();
    work.finish().unwrap();
    assert_eq!(actual, expected);
    assert_eq!(*parent.trace.lock().unwrap(), *plain.trace.lock().unwrap());
    assert_eq!(*parent.trace.lock().unwrap(), [0, 7]);
    for cause in CAUSES {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = model.facts_in(64, 1, limits(), &mut |_| Err(cause), &mut work);
        assert!(matches!(finish(work, result), Err(Error::Control(actual)) if actual == cause));
        assert_eq!(*control.trace.lock().unwrap(), [0]);
    }
}
#[test]
fn delegated_dictionary_prefix_uses_the_same_containing_parent_before_flush() {
    use crate::physical_binding_v2::{
        BindingCodecError, BindingProjectionLimits, MaterializationModel,
    };
    use arrow::datatypes::DataType;
    let ty = FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
    );
    let requested = 2 * Layout::new::<DataType>().size();
    assert!(requested > 1);
    let binding_limits = BindingProjectionLimits {
        max_definitions: usize::MAX,
        max_type_references: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    for pending in [0, 254, 255] {
        for late in CAUSES {
            let control = Control::default();
            *control.late.lock().unwrap() = Some(late);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut seen = Vec::new();
            let mut parent = |facts: &NodeProjectionFacts| {
                seen.push(*facts);
                if facts.allocation_request_bytes_upper_bound > requested - 1 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            {
                let mut child = MaterializationModel::for_composition(1, 0, 4096, 0);
                child
                    .compose_in_node_in(Model::default(), 0, limits(), binding_limits, &mut parent)
                    .unwrap();
                // Updating the original base must retain the parent loan.
                child
                    .compose_in_node(
                        Model {
                            inputs: 1,
                            ..Model::default()
                        },
                        0,
                        limits(),
                        binding_limits,
                    )
                    .unwrap();
                let result = child.count_owned_type_clone_in(
                    &ty,
                    binding_limits,
                    &mut |_| Ok(()),
                    &mut work,
                );
                assert!(matches!(
                    result,
                    Err(BindingCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
            }
            let last = seen.last().unwrap();
            assert_eq!(last.input_node_count, 1);
            assert_eq!(last.allocation_requests_upper_bound, 2);
            assert_eq!(last.allocation_request_bytes_upper_bound, requested);
            assert_eq!(
                last.coexisting_source_and_request_bytes_upper_bound,
                4096 + requested
            );
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
}

fn with_values(
    run: impl FnOnce(&EncodedValues<'_, '_, '_>, &DecodedValues<'_, '_, '_>, &Control, &[p::ValueDef]),
) {
    use crate::{
        physical_connector_payload_v2::{
            ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
        },
        physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
        physical_value_origin_v2::ValueOriginProjectionLimits,
        physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
    };
    use arrow::datatypes::DataType;
    let control = Control::default();
    let definitions: Vec<_> = [0, u32::MAX]
        .into_iter()
        .map(|id| p::ValueDef {
            id: p::ValueId::new(id),
            ty: FunctionValueType::new(DataType::Int64, id == u32::MAX),
            origin: p::ValueOrigin::NodeOutput {
                node: p::NodeId::new(7),
                output_ordinal: u32::from(id != 0),
            },
        })
        .collect();
    let roots: Vec<_> = definitions
        .iter()
        .map(|value| (value.id.get(), value.ty.clone()))
        .collect();
    let type_limits = TypeProjectionLimits {
        max_definitions: 16,
        max_expanded_nodes: 128,
        max_string_bytes: 4096,
    };
    let types = encode_type_table_sources(&roots, &[], type_limits, &control).unwrap();
    let read_types = decode_type_table(types.as_wire(), type_limits, &control).unwrap();
    let payload_limits = ConnectorPayloadProjectionLimits {
        max_definitions: 16,
        max_payload_bytes: 4096,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 65536,
        max_coexisting_source_and_request_bytes: 1048576,
        max_work: 1048576,
    };
    let payloads = encode_connector_payloads(&[], 4096, payload_limits, &control).unwrap();
    let read_payloads =
        decode_connector_payloads(payloads.as_wire(), 4096, payload_limits, &control).unwrap();
    let inputs: Vec<_> = definitions
        .iter()
        .map(|source| ValueSource {
            source,
            value_type_id: source.id.get(),
        })
        .collect();
    let value_limits = ValueProjectionLimits {
        max_definitions: 16,
        max_origin_references: 128,
        max_allocation_requests: 128,
        max_allocation_request_bytes: 65536,
        max_coexisting_source_and_request_bytes: 1048576,
        max_work: 1048576,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 64,
            max_allocation_request_bytes: 65536,
            max_coexisting_source_and_request_bytes: 1048576,
            max_work: 1048576,
        },
    };
    let encoded = encode_values(&inputs, &payloads, &types, 65536, value_limits).unwrap();
    let decoded = decode_values(
        encoded.as_wire(),
        &read_payloads,
        &read_types,
        65536,
        value_limits,
    )
    .unwrap();
    control.trace.lock().unwrap().clear();
    run(&encoded, &decoded, &control, &definitions);
}
fn check_value_capture(values: &impl Values, control: &Control, expected: Option<&p::ValueDef>) {
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            control.trace.lock().unwrap().clear();
            *control.late.lock().unwrap() = Some(cause);
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut captured = false;
            let result = values.value_captured(u32::MAX, &mut |value, _| {
                assert_eq!(value.id.get(), u32::MAX);
                assert_eq!(value.ty, FunctionValueType::new(arrow::datatypes::DataType::Int64, true));
                assert!(matches!(value.origin, p::ValueOrigin::NodeOutput { node, output_ordinal: 1 } if node.get() == 7));
                if let Some(expected) = expected { assert!(std::ptr::eq(value, expected)); }
                captured = true;
                Err(CompileControlError::ResourceExhausted.into())
            }, &mut work);
            assert!(matches!(
                finish(work, result),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert!(captured);
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
    *control.late.lock().unwrap() = None;
    control.trace.lock().unwrap().clear();
    let known = values.retained_floor_header().unwrap();
    assert!(known >= 65536);
    assert!(control.trace.lock().unwrap().is_empty());
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
    assert_eq!(values.retained_floor(&mut work).unwrap(), known);
    work.finish().unwrap();
    assert_eq!(*control.trace.lock().unwrap(), [0, 1]);
    control.trace.lock().unwrap().clear();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
    assert!(!values.contains(7, &mut work).unwrap());
    work.finish().unwrap();
    let plain = control.trace.lock().unwrap().clone();
    control.trace.lock().unwrap().clear();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
    assert!(
        values
            .value_captured(
                7,
                &mut |_, _| panic!("unknown ID captured a value"),
                &mut work
            )
            .unwrap()
            .is_none()
    );
    work.finish().unwrap();
    assert_eq!(*control.trace.lock().unwrap(), plain);
}
#[test]
fn shared_value_capture_borrows_actual_sparse_source_before_lookup_completion() {
    with_values(|encoded, decoded, control, source| {
        check_value_capture(encoded, control, Some(&source[1]));
        check_value_capture(decoded, control, None);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode).unwrap();
        let actual = decoded
            .value_observed(u32::MAX, &mut work)
            .unwrap()
            .unwrap();
        work.finish().unwrap();
        check_value_capture(decoded, control, Some(actual));
    });
}
