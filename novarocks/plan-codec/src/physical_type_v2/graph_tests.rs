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

use super::graph::{Index, IndexProjectionFacts, Node};
use super::*;
use std::{alloc::Layout, sync::Mutex};
use wire::carrier_type_definition::Kind;

const SOURCE: usize = 16 * 1024 * 1024;
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
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(control: &Control) -> Vec<u32> {
    control.events.lock().unwrap().clone()
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 10000,
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
fn field(id: u32, ty: Option<u32>) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: format!("field{id}"),
        nullable: false,
        carrier_type_id: ty,
        metadata: vec![],
        dictionary_id: None,
        dictionary_is_ordered: None,
    }
}
fn small() -> wire::TypeTable {
    wire::TypeTable {
        carriers: vec![
            carrier(
                u32::MAX,
                Kind::StructType(wire::StructFields {
                    field_ids: vec![u32::MAX, 0, u32::MAX],
                }),
            ),
            carrier(0, Kind::Primitive(5)),
            carrier(9, Kind::ListFieldId(0)),
        ],
        fields: vec![field(u32::MAX, Some(0)), field(0, Some(0))],
        value_types: vec![wire::ValueTypeDefinition {
            id: u32::MAX,
            carrier_type_id: Some(u32::MAX),
            nullable: true,
            logical_type: 1,
        }],
    }
}
fn floor(table: &wire::TypeTable) -> usize {
    size_of::<wire::TypeTable>()
        + table.carriers.capacity() * size_of::<wire::CarrierTypeDefinition>()
        + table.fields.capacity() * size_of::<wire::FieldDefinition>()
        + table.value_types.capacity() * size_of::<wire::ValueTypeDefinition>()
}
fn prepare<'a>(
    table: &'a wire::TypeTable,
    source: usize,
    control: &Control,
) -> Result<Index<'a>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = Index::prepare_observed(table, limits(), source, &mut |_| Ok(()), &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn facts(table: &wire::TypeTable) -> IndexProjectionFacts {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut last = None;
    Index::prepare_observed(
        table,
        limits(),
        SOURCE,
        &mut |facts| {
            last = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    last.unwrap()
}
fn ordinary(result: Result<Index<'_>, TypeCodecError>, message: &'static str) {
    assert!(matches!(result, Err(TypeCodecError::InvalidShape(actual)) if actual == message));
}

#[test]
fn graph_sparse_namespaces_borrow_original_definitions_and_preserve_all_child_ordinals() {
    let mut table = small();
    table.carriers.extend([
        carrier(10, Kind::ListViewFieldId(u32::MAX)),
        carrier(
            11,
            Kind::FixedSizeList(wire::FixedSizeList {
                item_field_id: Some(0),
                length: 3,
            }),
        ),
        carrier(12, Kind::LargeListFieldId(0)),
        carrier(13, Kind::LargeListViewFieldId(u32::MAX)),
        carrier(
            14,
            Kind::UnionType(wire::UnionFields {
                mode: 1,
                fields: vec![
                    wire::UnionField {
                        type_id: 127,
                        field_id: Some(u32::MAX),
                    },
                    wire::UnionField {
                        type_id: 0,
                        field_id: Some(0),
                    },
                ],
            }),
        ),
        carrier(
            15,
            Kind::Dictionary(wire::DictionaryTypes {
                key_type_id: Some(0),
                value_type_id: Some(u32::MAX),
            }),
        ),
        carrier(
            16,
            Kind::Map(wire::MapField {
                entries_field_id: Some(0),
                ordered: true,
            }),
        ),
        carrier(
            17,
            Kind::RunEndEncoded(wire::RunEndEncodedFields {
                run_ends_field_id: Some(0),
                values_field_id: Some(u32::MAX),
            }),
        ),
    ]);
    let index = prepare(&table, SOURCE, &Control::default()).unwrap();
    assert!(std::ptr::eq(index.table, &table));
    for original in &table.carriers {
        assert!(std::ptr::eq(index.carrier(original.id).unwrap(), original));
    }
    for original in &table.fields {
        assert!(std::ptr::eq(index.field(original.id).unwrap(), original));
    }
    assert_eq!(
        index.vertex_count().unwrap(),
        table.carriers.len() + table.fields.len()
    );
    for (node, children) in [
        (Node::Carrier(0), vec![]),
        (Node::Field(0), vec![Node::Carrier(0)]),
        (Node::Field(u32::MAX), vec![Node::Carrier(0)]),
        (
            Node::Carrier(u32::MAX),
            vec![Node::Field(u32::MAX), Node::Field(0), Node::Field(u32::MAX)],
        ),
        (Node::Carrier(9), vec![Node::Field(0)]),
        (Node::Carrier(10), vec![Node::Field(u32::MAX)]),
        (Node::Carrier(11), vec![Node::Field(0)]),
        (Node::Carrier(12), vec![Node::Field(0)]),
        (Node::Carrier(13), vec![Node::Field(u32::MAX)]),
        (
            Node::Carrier(14),
            vec![Node::Field(u32::MAX), Node::Field(0)],
        ),
        (
            Node::Carrier(15),
            vec![Node::Carrier(0), Node::Carrier(u32::MAX)],
        ),
        (Node::Carrier(16), vec![Node::Field(0)]),
        (
            Node::Carrier(17),
            vec![Node::Field(0), Node::Field(u32::MAX)],
        ),
    ] {
        assert_eq!(index.child_count(node).unwrap(), children.len());
        for (ordinal, expected) in children.iter().enumerate() {
            assert_eq!(index.child(node, ordinal).unwrap(), *expected);
        }
    }
    assert!(matches!(
        index.child(Node::Carrier(0), 0),
        Err(TypeCodecError::InvalidShape(
            "invalid carrier child ordinal"
        ))
    ));
}

#[test]
fn graph_struct_5000_is_only_a_borrowed_representation_index() {
    let mut table = small();
    table.carriers[0].kind = Some(Kind::StructType(wire::StructFields {
        field_ids: (0..5000)
            .map(|n| if n % 2 == 0 { 0 } else { u32::MAX })
            .collect(),
    }));
    let index = prepare(&table, SOURCE, &Control::default()).unwrap();
    assert_eq!(index.child_count(Node::Carrier(u32::MAX)).unwrap(), 5000);
    assert_eq!(
        index.child(Node::Carrier(u32::MAX), 0).unwrap(),
        Node::Field(0)
    );
    assert_eq!(
        index.child(Node::Carrier(u32::MAX), 4999).unwrap(),
        Node::Field(u32::MAX)
    );
    // The sparse index visits definitions, not unfolded occurrences. It does
    // not override the separate original full Value owner node-count law.
    assert_eq!(facts(&table).definition_count, 6);
    assert_eq!(facts(&table).allocation_requests_upper_bound, 5);
}

#[test]
fn graph_original_kind_duplicate_reference_and_capacity_errors_stay_ordinary() {
    let mut table = small();
    table.carriers.push(table.carriers[0].clone());
    ordinary(
        prepare(&table, SOURCE, &Control::default()),
        "duplicate carrier type identity",
    );
    let mut table = small();
    table.fields.push(table.fields[0].clone());
    ordinary(
        prepare(&table, SOURCE, &Control::default()),
        "duplicate field identity",
    );
    let mut table = small();
    table.carriers[0].kind = None;
    ordinary(
        prepare(&table, SOURCE, &Control::default()),
        "missing carrier kind",
    );
    let mut table = small();
    table.carriers.reserve(100);
    table.fields.reserve(100);
    table.value_types.reserve(100);
    prepare(&table, floor(&table), &Control::default()).unwrap();
    ordinary(
        prepare(&table, floor(&table) - 1, &Control::default()),
        "type graph source invoice is understated",
    );
    let mut table = small();
    table.fields[0].carrier_type_id = None;
    let index = prepare(&table, SOURCE, &Control::default()).unwrap();
    assert!(matches!(
        index.child(Node::Field(u32::MAX), 0),
        Err(TypeCodecError::InvalidShape("missing type table reference"))
    ));
    assert!(matches!(
        index.carrier(42),
        Err(TypeCodecError::InvalidShape(
            "dangling carrier type reference"
        ))
    ));
    assert!(matches!(
        index.field(42),
        Err(TypeCodecError::InvalidShape("dangling field reference"))
    ));
    for kind in [
        Kind::Dictionary(wire::DictionaryTypes {
            key_type_id: None,
            value_type_id: Some(42),
        }),
        Kind::Map(wire::MapField {
            entries_field_id: None,
            ordered: false,
        }),
        Kind::RunEndEncoded(wire::RunEndEncodedFields {
            run_ends_field_id: None,
            values_field_id: Some(42),
        }),
        Kind::UnionType(wire::UnionFields {
            mode: 1,
            fields: vec![wire::UnionField {
                type_id: 0,
                field_id: None,
            }],
        }),
        Kind::FixedSizeList(wire::FixedSizeList {
            item_field_id: None,
            length: 0,
        }),
    ] {
        let mut table = small();
        table.carriers[0].kind = Some(kind);
        let index = prepare(&table, SOURCE, &Control::default()).unwrap();
        assert!(matches!(
            index.child(Node::Carrier(u32::MAX), 0),
            Err(TypeCodecError::InvalidShape("missing type table reference"))
        ));
    }
    let mut table = small();
    table.carriers[0].kind = Some(Kind::ListFieldId(42));
    let index = prepare(&table, SOURCE, &Control::default()).unwrap();
    assert!(matches!(
        index.child(Node::Carrier(u32::MAX), 0),
        Err(TypeCodecError::InvalidShape("dangling field reference"))
    ));
}

fn independent_node_upper() -> usize {
    let pointer = Layout::new::<usize>();
    let align = pointer.align().max(align_of::<u32>());
    let raw = pointer.size()
        + 4
        + 11 * (size_of::<u32>() + pointer.size())
        + 5 * (align - 1)
        + 12 * pointer.size()
        + align
        - 1;
    raw.div_ceil(align) * align
}
fn independent_insert_work(n: usize, node: usize) -> usize {
    let levels = (usize::BITS - n.leading_zeros()) as usize + 1;
    n * levels * 64 + n * levels * node * 8
}
#[test]
fn graph_request_upper_has_independent_layout_and_zero_definition_oracles() {
    let table = small();
    let actual = facts(&table);
    let node = independent_node_upper();
    assert_eq!(actual.definition_count, 6);
    assert_eq!(actual.allocation_requests_upper_bound, 5);
    assert_eq!(actual.request_bytes_upper_bound, 5 * node);
    assert_eq!(actual.coexistence_bytes_upper_bound, SOURCE + 5 * node);
    assert_eq!(
        actual.cumulative_work_upper_bound,
        independent_insert_work(3, node) + independent_insert_work(2, node) + 128 + 5 * 64
    );
    let table = wire::TypeTable {
        carriers: vec![],
        fields: vec![],
        value_types: vec![],
    };
    let actual = facts(&table);
    assert_eq!(actual.definition_count, 0);
    assert_eq!(actual.allocation_requests_upper_bound, 0);
    assert_eq!(actual.request_bytes_upper_bound, 0);
    assert_eq!(actual.coexistence_bytes_upper_bound, SOURCE);
    assert_eq!(actual.cumulative_work_upper_bound, 128);
}

#[test]
fn graph_known_parent_caps_precede_pending_254_255_flush_and_keep_resource_primary() {
    let table = small();
    let generous = facts(&table);
    let values = [
        generous.definition_count,
        generous.allocation_requests_upper_bound,
        generous.request_bytes_upper_bound,
        generous.coexistence_bytes_upper_bound,
        generous.cumulative_work_upper_bound,
    ];
    // Private caller-pending seam: these steps model already-completed parent
    // work. They are not a claim of source Index work or library cooperation.
    for pending in [254, 255] {
        for axis in 0..values.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((1, cause)),
                    ..Control::default()
                };
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let result = Index::prepare_observed(
                    &table,
                    limits(),
                    SOURCE,
                    &mut |f| {
                        let known = [
                            f.definition_count,
                            f.allocation_requests_upper_bound,
                            f.request_bytes_upper_bound,
                            f.coexistence_bytes_upper_bound,
                            f.cumulative_work_upper_bound,
                        ];
                        if known[axis] > values[axis] - 1 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    },
                    &mut work,
                );
                assert!(matches!(
                    result,
                    Err(TypeCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(trace(&control), [0]);
            }
        }
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    Index::prepare_observed(
        &table,
        limits(),
        SOURCE,
        &mut |f| {
            assert_eq!(
                [
                    f.definition_count,
                    f.allocation_requests_upper_bound,
                    f.request_bytes_upper_bound,
                    f.coexistence_bytes_upper_bound,
                    f.cumulative_work_upper_bound
                ],
                values
            );
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
}

#[test]
fn graph_small_success_and_ordinary_actual_callback_prefixes_preserve_three_causes() {
    for case in 0..4 {
        let mut table = small();
        match case {
            1 => table.carriers[0].kind = None,
            2 => table.carriers.push(table.carriers[0].clone()),
            3 => table.fields.push(table.fields[0].clone()),
            _ => {}
        }
        let control = Control::default();
        assert_eq!(prepare(&table, SOURCE, &control).is_err(), case != 0);
        let expected = trace(&control);
        for at in 0..expected.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause)),
                    ..Control::default()
                };
                assert!(
                    matches!(prepare(&table, SOURCE, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), expected[..=at]);
            }
        }
    }
}

#[test]
fn graph_duplicate_insertions_charge_the_completed_mutation_before_the_ordinary_error() {
    for duplicate_carrier in [true, false] {
        let mut table = small();
        if duplicate_carrier {
            table.carriers.push(table.carriers[0].clone());
        } else {
            table.fields.push(table.fields[0].clone());
        }
        let control = Control::default();
        ordinary(
            prepare(&table, SOURCE, &control),
            if duplicate_carrier {
                "duplicate carrier type identity"
            } else {
                "duplicate field identity"
            },
        );
        let expected = trace(&control);
        // Three carrier insertions plus the duplicate, or three carriers and
        // three fields. The failed uniqueness test still completed insertion.
        let completed = if duplicate_carrier { 4 } else { 6 };
        assert_eq!(
            expected.iter().map(|&n| n as usize).sum::<usize>(),
            completed
        );
        let mutation_tail = expected.iter().rposition(|&n| n == 1).unwrap();
        for cause in CAUSES {
            let control = Control {
                stop: Some((mutation_tail, cause)),
                ..Control::default()
            };
            assert!(matches!(
                prepare(&table, SOURCE, &control),
                Err(TypeCodecError::Control(actual)) if actual == cause
            ));
            assert_eq!(trace(&control), expected[..=mutation_tail]);
        }
    }
}

#[test]
fn graph_wide_320_real_sparse_insertions_have_finer_opaque_observation_and_sampled_prefixes() {
    let table = wire::TypeTable {
        carriers: (0..320)
            .map(|n| carrier(if n == 319 { u32::MAX } else { n }, Kind::Primitive(5)))
            .collect(),
        fields: vec![],
        value_types: vec![],
    };
    let control = Control::default();
    let index = prepare(&table, SOURCE, &control).unwrap();
    assert_eq!(index.vertex_count().unwrap(), 320);
    assert!(std::ptr::eq(
        index.carrier(u32::MAX).unwrap(),
        &table.carriers[319]
    ));
    let expected = trace(&control);
    assert_eq!(expected.iter().map(|n| *n as usize).sum::<usize>(), 320);
    assert_eq!(expected.iter().filter(|n| **n == 1).count(), 320);
    // Each true BTree insertion is bracketed. A wide definition count does
    // not manufacture an internal library 256-unit callback.
    for at in [
        0,
        expected.iter().position(|n| *n == 1).unwrap(),
        expected.len() / 2,
        expected.len() - 1,
    ] {
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause)),
                ..Control::default()
            };
            assert!(
                matches!(prepare(&table, SOURCE, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(trace(&control), expected[..=at]);
        }
    }
}
