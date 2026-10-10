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

use super::*;
use novarocks_proto_models::physical_package_v2 as raw;
use std::{collections::BTreeMap, sync::Mutex};
use wire::carrier_type_definition::Kind;

const SOURCE: usize = 1 << 25;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        events.push(units);
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(c: &Control) -> Vec<u32> {
    c.events.lock().unwrap().clone()
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100,
        max_expanded_nodes: 10000,
        max_string_bytes: 10000,
    }
}
fn carrier(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn field(id: u32, carrier: u32) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        carrier_type_id: Some(carrier),
        ..Default::default()
    }
}
fn binding(id: u32) -> raw::ConnectorWriteFieldBinding {
    raw::ConnectorWriteFieldBinding {
        field_id: Some(id),
        field_token: vec![],
    }
}
fn fixture() -> raw::FragmentPackage {
    raw::FragmentPackage {
        types: Some(wire::TypeTable {
            carriers: vec![
                carrier(
                    u32::MAX,
                    Kind::StructType(wire::StructFields {
                        field_ids: vec![u32::MAX, 0, u32::MAX],
                    }),
                ),
                carrier(12, Kind::Primitive(5)),
                carrier(0, Kind::Primitive(5)),
                carrier(9, Kind::ListFieldId(u32::MAX)),
            ],
            fields: vec![
                field(u32::MAX, 0),
                field(7, u32::MAX),
                field(12, 12),
                field(0, 0),
                field(9, 12),
            ],
            value_types: vec![wire::ValueTypeDefinition {
                id: u32::MAX,
                carrier_type_id: Some(0),
                nullable: false,
                logical_type: 1,
            }],
        }),
        schemas: vec![raw::SchemaDefinition {
            id: 0,
            field_ids: vec![0, 0],
            metadata: vec![],
        }],
        constants: vec![raw::IpcConstantPool {
            id: u32::MAX,
            field_id: Some(12),
            ..Default::default()
        }],
        writes: vec![raw::FrozenWriterRecipe {
            input: Some(raw::ConnectorWriteInputShape {
                kind: Some(raw::connector_write_input_shape::Kind::Data(
                    raw::ConnectorWriteDataInput {
                        fields: vec![binding(7), binding(7), binding(u32::MAX)],
                    },
                )),
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}
fn axes(f: PackageTypeGraphFacts) -> [usize; 7] {
    [
        f.definition_count,
        f.graph_edges,
        f.root_occurrences,
        f.allocation_requests_upper_bound,
        f.request_bytes_upper_bound,
        f.coexistence_bytes_upper_bound,
        f.cumulative_work_upper_bound,
    ]
}
fn invoke(
    package: &raw::FragmentPackage,
    control: &Control,
    caps: Option<[usize; 7]>,
    pending: usize,
) -> Result<PackageTypeGraphFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    // This private seam models already completed containing-caller work; it
    // does not claim that the package component performed these operations.
    for _ in 0..pending {
        work.step()?;
    }
    let result = (|| {
        let token = prepare_package_type_graph(
            package,
            limits(),
            SOURCE,
            &mut |f| {
                if let Some(caps) = caps
                    && axes(*f).iter().zip(caps).any(|(known, cap)| *known > cap)
                {
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            },
            &mut work,
        )?;
        token.visit::<TypeCodecError>(&mut |_, _| Ok(()), &mut work)?;
        Ok(token.facts())
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn domain(domain: PackageTypeRootDomain) -> u8 {
    match domain {
        PackageTypeRootDomain::Strict => 1,
        PackageTypeRootDomain::Writer => 2,
        PackageTypeRootDomain::Intersection => 3,
    }
}

#[test]
fn package_graph_sparse_sources_repetitions_and_unused_strict_intersections_have_hand_oracle() {
    let package = fixture();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let token =
        prepare_package_type_graph(&package, limits(), SOURCE, &mut |_| Ok(()), &mut work).unwrap();
    let table = package.types.as_ref().unwrap();
    assert!(std::ptr::eq(token.table_for(&package).unwrap(), table));
    let mut seen = BTreeMap::new();
    let mut ordered = Vec::new();
    token
        .visit::<TypeCodecError>(
            &mut |definition, _| {
                let (key, owner) = match definition {
                    PackageTypeGraphDefinition::Carrier {
                        definition,
                        domain: classification,
                    } => {
                        let original = table
                            .carriers
                            .iter()
                            .find(|d| d.id == definition.id)
                            .unwrap();
                        assert!(std::ptr::eq(definition, original));
                        ((false, definition.id), domain(classification))
                    }
                    PackageTypeGraphDefinition::Field {
                        definition,
                        domain: classification,
                    } => {
                        let original = table.fields.iter().find(|d| d.id == definition.id).unwrap();
                        assert!(std::ptr::eq(definition, original));
                        ((true, definition.id), domain(classification))
                    }
                };
                ordered.push(key);
                assert!(
                    seen.insert(key, owner).is_none(),
                    "each real definition is visited once"
                );
                Ok(())
            },
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(
        ordered,
        [
            (false, 0),
            (false, 9),
            (false, 12),
            (false, u32::MAX),
            (true, 0),
            (true, 7),
            (true, 9),
            (true, 12),
            (true, u32::MAX)
        ]
    );
    assert_eq!(
        seen,
        BTreeMap::from([
            ((false, 0), 3),
            ((false, 9), 1),
            ((false, 12), 1),
            ((false, u32::MAX), 2),
            ((true, 0), 3),
            ((true, 7), 2),
            ((true, 9), 1),
            ((true, 12), 1),
            ((true, u32::MAX), 3),
        ])
    );
    let f = token.facts();
    assert_eq!(f.definition_count, 10); // 4 carriers + 5 fields + one Value.
    assert_eq!(f.graph_edges, 9); // Struct3 + unusedList1 + five Field edges.
    assert_eq!(f.root_occurrences, 7); // Value1 + Schema2 + IPC1 + Writer3.
    // Original Index9 + domain flags9 + reusable stack1, not actual allocator
    // request measurements and not max sparse identity sized storage.
    assert_eq!(f.allocation_requests_upper_bound, 19);
    assert_eq!(
        f.coexistence_bytes_upper_bound,
        SOURCE + f.request_bytes_upper_bound + size_of::<PreparedPackageTypeGraph<'_>>()
    );
}

#[test]
fn package_graph_foreign_equal_owner_is_refused_and_raw_representation_is_not_a_law_grant() {
    let mut package = fixture();
    // Legal graph representation with a carrier parameter the separate Arrow
    // law must reject. The graph token must not claim whole type validation.
    package.types.as_mut().unwrap().carriers[1].kind = Some(Kind::FixedSizeBinary(-1));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let token =
        prepare_package_type_graph(&package, limits(), SOURCE, &mut |_| Ok(()), &mut work).unwrap();
    let foreign = package.clone();
    assert!(matches!(
        token.table_for(&foreign),
        Err(TypeCodecError::InvalidShape(_))
    ));
    assert!(std::ptr::eq(
        token.table_for(&package).unwrap(),
        package.types.as_ref().unwrap()
    ));
    work.finish().unwrap();
}

#[derive(Debug)]
enum VisitError {
    Codec(TypeCodecError),
    Visitor(&'static str),
}
impl From<TypeCodecError> for VisitError {
    fn from(error: TypeCodecError) -> Self {
        Self::Codec(error)
    }
}
#[test]
fn package_graph_generic_visitor_error_is_original_without_second_event_or_own_footer() {
    let package = fixture();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let token =
        prepare_package_type_graph(&package, limits(), SOURCE, &mut |_| Ok(()), &mut work).unwrap();
    let mut calls = 0;
    let mut callback_trace_len = 0;
    let result = token.visit(
        &mut |_, _| {
            calls += 1;
            callback_trace_len = trace(&control).len();
            Err(VisitError::Visitor("original visitor failure"))
        },
        &mut work,
    );
    assert!(matches!(
        result,
        Err(VisitError::Visitor("original visitor failure"))
    ));
    assert_eq!(calls, 1);
    assert_eq!(trace(&control).len(), callback_trace_len);
    // Only the caller observes any completed ordinary tail. The visit cannot
    // insert a second visitor event or its own finish after the error.
    let before_footer = trace(&control);
    work.finish().unwrap();
    assert_eq!(trace(&control).len(), before_footer.len() + 1);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let token =
        prepare_package_type_graph(&package, limits(), SOURCE, &mut |_| Ok(()), &mut work).unwrap();
    let mut calls = 0;
    let mut callback_trace_len = 0;
    let result = token.visit(
        &mut |_, _| {
            calls += 1;
            callback_trace_len = trace(&control).len();
            Err(VisitError::Codec(TypeCodecError::Control(
                CompileControlError::Cancelled,
            )))
        },
        &mut work,
    );
    assert!(matches!(
        result,
        Err(VisitError::Codec(TypeCodecError::Control(
            CompileControlError::Cancelled
        )))
    ));
    assert_eq!(calls, 1);
    assert_eq!(trace(&control).len(), callback_trace_len);
    // No caller footer after a typed primary control refusal.
}

#[test]
fn package_graph_all_cumulative_axes_exact_and_one_under_keep_pending_resource_primary() {
    let package = fixture();
    let expected = axes(invoke(&package, &Control::default(), None, 0).unwrap());
    invoke(&package, &Control::default(), Some(expected), 0).unwrap();
    for pending in [254, 255] {
        for axis in 0..expected.len() {
            assert!(expected[axis] > 0);
            let mut caps = expected;
            caps[axis] -= 1;
            let control = Control::default();
            assert!(matches!(
                invoke(&package, &control, Some(caps), pending),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            let prefix = trace(&control);
            // Later discovered edges/roots retain their actual early callbacks.
            // Known initial axes may refuse immediately at the caller entry.
            for cause in CAUSES {
                let control = Control {
                    stop: Some((prefix.len(), cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    invoke(&package, &control, Some(caps), pending),
                    Err(TypeCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(trace(&control), prefix);
            }
        }
    }
}

#[test]
fn package_graph_every_actual_small_callback_success_and_ordinary_prefix_keeps_three_causes() {
    for case in 0..3 {
        let mut package = fixture();
        match case {
            1 => package.types = None,
            2 => package.writes[0].input = None,
            _ => {}
        }
        let control = Control::default();
        assert_eq!(invoke(&package, &control, None, 0).is_err(), case != 0);
        let baseline = trace(&control);
        for at in 0..baseline.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                assert!(
                    matches!(invoke(&package, &control, None, 0), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), baseline[..=at]);
            }
        }
    }
}

#[test]
fn package_graph_single_carrier_known_requests_refuse_before_first_root_header_quantum() {
    let package = raw::FragmentPackage {
        types: Some(wire::TypeTable {
            carriers: vec![carrier(u32::MAX, Kind::Primitive(5))],
            fields: vec![],
            value_types: vec![],
        }),
        ..Default::default()
    };
    let exact = invoke(&package, &Control::default(), None, 0).unwrap();
    // One original sparse Index insertion, one domain flag and one reusable
    // stack reservation are all scalar-known before the first root-header step.
    assert_eq!(exact.allocation_requests_upper_bound, 3);
    assert_eq!(exact.definition_count, 1);
    assert_eq!(exact.root_occurrences, 0);
    for pending in [254, 255] {
        for request_cap in [0, 2] {
            let mut caps = axes(exact);
            caps[3] = request_cap;
            for cause in CAUSES {
                let control = Control {
                    stop: Some((1, cause)),
                    ..Control::default()
                };
                assert!(matches!(
                    invoke(&package, &control, Some(caps), pending),
                    Err(TypeCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(trace(&control), [0]);
            }
        }
    }
}
