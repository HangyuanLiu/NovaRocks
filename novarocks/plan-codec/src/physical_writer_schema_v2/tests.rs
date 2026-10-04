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
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::sync::{Arc, Mutex};
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    state: Mutex<(bool, Option<(usize, CompileControlError)>)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let (active, stop) = *self.state.lock().unwrap();
        if !active {
            return Ok(());
        }
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.state.lock().unwrap() = (true, stop);
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}

fn limits() -> WriterSchemaProjectionLimits {
    WriterSchemaProjectionLimits {
        max_fields: 4096,
        max_name_bytes: 1 << 20,
        max_type_references: 4096,
        max_allocation_requests: 16384,
        max_allocation_request_bytes: 16 << 20,
        max_coexisting_source_and_request_bytes: 32 << 20,
        max_work: 1 << 30,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 2048,
        max_string_bytes: 1 << 20,
    }
}
#[allow(
    deprecated,
    reason = "Frozen field identity retains original Arrow dictionary IDs."
)]
fn roots() -> Vec<(u32, FunctionValueType)> {
    vec![
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
        (
            7,
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ),
        (
            9,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            11,
            FunctionValueType::new(
                DataType::List(Arc::new(
                    Field::new_dict(
                        "nested / é",
                        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                        true,
                        93,
                        true,
                    )
                    .with_metadata([("source.key".into(), "exact / 值".into())].into()),
                )),
                true,
            ),
        ),
        (12, FunctionValueType::new(DataType::Utf8, true)),
        (
            13,
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        ),
    ]
}
fn source_schema(roots: &[(u32, FunctionValueType)]) -> p::WriterRelationSchema {
    let roles = [
        p::WriterRelationFieldRole::Kind,
        p::WriterRelationFieldRole::TargetOrdinal,
        p::WriterRelationFieldRole::RowCount,
        p::WriterRelationFieldRole::CommitFragment,
        p::WriterRelationFieldRole::Auxiliary,
    ];
    let names = ["kind / é", "ordinal", "rows", "commit", "辅助"];
    p::WriterRelationSchema {
        revision: u32::MAX,
        fields: roots
            .iter()
            .take(5)
            .enumerate()
            .map(|(i, (_, ty))| p::WriterRelationField {
                value: p::ValueId::new(if i % 2 == 0 { 0 } else { u32::MAX }),
                name: names[i].into(),
                ty: ty.clone(),
                role: roles[i],
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}
fn expected_schema() -> wire::WriterRelationSchema {
    let fields = [
        (0, "kind / é", 0, wire::WriterRelationFieldRole::Kind),
        (
            u32::MAX,
            "ordinal",
            u32::MAX,
            wire::WriterRelationFieldRole::TargetOrdinal,
        ),
        (0, "rows", 7, wire::WriterRelationFieldRole::RowCount),
        (
            u32::MAX,
            "commit",
            9,
            wire::WriterRelationFieldRole::CommitFragment,
        ),
        (0, "辅助", 11, wire::WriterRelationFieldRole::Auxiliary),
    ]
    .into_iter()
    .map(|(value, name, ty, role)| wire::WriterRelationField {
        value_id: Some(value),
        name: name.into(),
        value_type_id: Some(ty),
        role: role as i32,
    })
    .collect();
    wire::WriterRelationSchema {
        revision: u32::MAX,
        fields,
    }
}
fn source_targets(roots: &[(u32, FunctionValueType)]) -> Box<[p::WriterTargetField]> {
    Box::from([
        p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes([0; 32]),
            provider_name: "first / é".into(),
            input: p::ValueId::new(0),
            ty: roots[3].1.clone(),
            hidden: false,
        },
        p::WriterTargetField {
            token: ConnectorWriteFieldToken::from_bytes([255; 32]),
            provider_name: "second / 值".into(),
            input: p::ValueId::new(u32::MAX),
            ty: roots[1].1.clone(),
            hidden: true,
        },
    ])
}
fn expected_targets() -> Vec<wire::WriterTargetField> {
    vec![
        wire::WriterTargetField {
            token: vec![0; 32],
            provider_name: "first / é".into(),
            input_value_id: Some(0),
            value_type_id: Some(9),
            hidden: false,
        },
        wire::WriterTargetField {
            token: vec![255; 32],
            provider_name: "second / 值".into(),
            input_value_id: Some(u32::MAX),
            value_type_id: Some(u32::MAX),
            hidden: true,
        },
    ]
}
fn with_types<R>(
    c: &Control,
    run: impl FnOnce(&[(u32, FunctionValueType)], &EncodedTypeTable<'_>, &DecodedTypeTable) -> R,
) -> R {
    let roots = roots();
    let encoded = encode_type_table_sources(&roots, &[], type_limits(), c).unwrap();
    let decoded = decode_type_table(encoded.as_wire(), type_limits(), c).unwrap();
    run(&roots, &encoded, &decoded)
}
fn units(c: &Control) -> u64 {
    c.trace().iter().map(|(_, n)| u64::from(*n)).sum()
}
#[test]
#[allow(
    deprecated,
    reason = "This oracle checks frozen Arrow dictionary identity."
)]
fn writer_schema_complete_independent_wire_preserves_roles_names_revision_and_full_types() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        c.arm(None);
        assert_eq!(
            encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c)
                .unwrap()
                .0,
            expected_schema()
        );
        c.arm(None);
        let decoded = decode_writer_schema(&expected_schema(), read, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert_eq!(decoded, schema);
        assert_eq!(decoded.fields[1].ty.logical_type, ValueLogicalType::Json);
        assert_eq!(
            decoded.fields[2].ty.logical_type,
            ValueLogicalType::LargeInt
        );
        if let DataType::List(field) = &decoded.fields[4].ty.data_type {
            assert_eq!(field.dict_id(), Some(93));
            assert_eq!(field.dict_is_ordered(), Some(true));
            assert_eq!(
                field.metadata().get("source.key").map(String::as_str),
                Some("exact / 值")
            );
            let original = match &read.value_type(11).unwrap().data_type {
                DataType::List(f) => f,
                _ => unreachable!(),
            };
            assert!(Arc::ptr_eq(field, original));
        } else {
            panic!("lost nested full source type");
        }
    });
}
#[test]
fn writer_target_fields_complete_independent_wire_preserves_tokens_hidden_and_nullable() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let fields = source_targets(roots);
        let ids = [9, u32::MAX];
        c.arm(None);
        assert_eq!(
            encode_writer_target_fields(&fields, &ids, types, SOURCE, limits(), &c)
                .unwrap()
                .0,
            expected_targets()
        );
        c.arm(None);
        assert_eq!(
            decode_writer_target_fields(&expected_targets(), read, SOURCE, limits(), &c)
                .unwrap()
                .0,
            fields
        );
        // Input membership and its nullability are not rebound by this component:
        // the declared target field type comes only from its authored type ID.
        let input = vec![wire::WriterTargetField {
            token: vec![7; 32],
            provider_name: "".into(),
            input_value_id: Some(u32::MAX),
            value_type_id: Some(0),
            hidden: true,
        }];
        c.arm(None);
        let decoded = decode_writer_target_fields(&input, read, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert!(!decoded[0].ty.nullable);
        assert_eq!(decoded[0].input.get(), u32::MAX);
        assert_eq!(decoded[0].token.to_bytes(), [7; 32]);
    });
}
#[test]
fn writer_prepared_original_loans_and_positive_work_equal_combined_for_all_four_routes() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        let raw = expected_schema();
        let targets = source_targets(roots);
        let target_ids = [9, u32::MAX];
        let rawtargets = expected_targets();
        c.arm(None);
        let combined = encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_schema_encode(&schema, &ids, types, SOURCE, limits(), &c).unwrap();
        assert!(std::ptr::eq(prepared.input, &schema));
        assert!(std::ptr::eq(prepared.types(), types));
        assert!(std::ptr::eq(
            prepared.control,
            &c as &dyn PureCompileControl
        ));
        assert_eq!(prepared.facts(), &combined.1);
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined = decode_writer_schema(&raw, read, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared = prepare_writer_schema_decode(&raw, read, SOURCE, limits(), &c).unwrap();
        assert!(std::ptr::eq(prepared.input, &raw));
        assert!(std::ptr::eq(prepared.types(), read));
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined =
            encode_writer_target_fields(&targets, &target_ids, types, SOURCE, limits(), &c)
                .unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_target_fields_encode(&targets, &target_ids, types, SOURCE, limits(), &c)
                .unwrap();
        assert!(std::ptr::eq(prepared.input, targets.as_ref()));
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
        c.arm(None);
        let combined =
            decode_writer_target_fields(&rawtargets, read, SOURCE, limits(), &c).unwrap();
        let work = units(&c);
        c.arm(None);
        let prepared =
            prepare_writer_target_fields_decode(&rawtargets, read, SOURCE, limits(), &c).unwrap();
        assert_eq!(prepared.emit().unwrap(), combined);
        assert_eq!(units(&c), work);
    });
}
fn exact(f: WriterSchemaProjectionFacts) -> WriterSchemaProjectionLimits {
    WriterSchemaProjectionLimits {
        max_fields: f.field_count,
        max_name_bytes: f.name_bytes,
        max_type_references: f.type_reference_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn under(mut l: WriterSchemaProjectionLimits, axis: usize) -> WriterSchemaProjectionLimits {
    match axis {
        0 => l.max_fields -= 1,
        1 => l.max_name_bytes -= 1,
        2 => l.max_type_references -= 1,
        3 => l.max_allocation_requests -= 1,
        4 => l.max_allocation_request_bytes -= 1,
        5 => l.max_coexisting_source_and_request_bytes -= 1,
        6 => l.max_work -= 1,
        _ => unreachable!(),
    };
    l
}
#[test]
fn writer_independent_layout_dictionary_clone_and_seven_caps_all_four_routes() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        let ids = [0, u32::MAX, 7, 9, 11];
        let targets = source_targets(roots);
        let target_ids = [9, u32::MAX];
        let raw = expected_schema();
        let rt = expected_targets();
        let names = raw.fields.iter().map(|f| f.name.len()).sum::<usize>();
        let targetnames = rt.iter().map(|f| f.provider_name.len()).sum::<usize>();
        c.arm(None);
        let se = encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let sd = decode_writer_schema(&raw, read, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let te = encode_writer_target_fields(&targets, &target_ids, types, SOURCE, limits(), &c)
            .unwrap()
            .1;
        c.arm(None);
        let td = decode_writer_target_fields(&rt, read, SOURCE, limits(), &c)
            .unwrap()
            .1;
        assert_eq!(se.allocation_requests_upper_bound, 6);
        assert_eq!(
            se.allocation_request_bytes_upper_bound,
            5 * size_of::<wire::WriterRelationField>() + names
        );
        // Only the ROOT Dictionary owns two cloned Box<DataType> requests. The
        // nested Dictionary under a List FieldRef shares the actual original Arc.
        assert_eq!(sd.allocation_requests_upper_bound, 14);
        assert_eq!(
            sd.allocation_request_bytes_upper_bound,
            2 * 5 * size_of::<p::WriterRelationField>() + 2 * names + 2 * size_of::<DataType>()
        );
        assert_eq!(te.allocation_requests_upper_bound, 5);
        assert_eq!(
            te.allocation_request_bytes_upper_bound,
            2 * size_of::<wire::WriterTargetField>() + 64 + targetnames
        );
        assert_eq!(td.allocation_requests_upper_bound, 8);
        assert_eq!(
            td.allocation_request_bytes_upper_bound,
            2 * 2 * size_of::<p::WriterTargetField>() + 2 * targetnames + 2 * size_of::<DataType>()
        );
        for route in 0..4 {
            let f = [se, sd, te, td][route];
            let invoke = |l| match route {
                0 => encode_writer_schema(&schema, &ids, types, SOURCE, l, &c).map(|_| ()),
                1 => decode_writer_schema(&raw, read, SOURCE, l, &c).map(|_| ()),
                2 => encode_writer_target_fields(&targets, &target_ids, types, SOURCE, l, &c)
                    .map(|_| ()),
                3 => decode_writer_target_fields(&rt, read, SOURCE, l, &c).map(|_| ()),
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(invoke(exact(f)).is_ok(), "exact route {route}");
            for axis in 0..7 {
                c.arm(None);
                assert!(
                    invoke(under(exact(f), axis)).is_err(),
                    "route {route} axis {axis}"
                );
            }
        }
    });
}
#[test]
fn writer_exact_type_required_presence_closed_roles_and_tokens_reject_without_defaults() {
    let c = Control::default();
    with_types(&c, |roots, types, read| {
        let schema = source_schema(roots);
        for ids in [
            vec![0],
            vec![0, u32::MAX, 7, 9, 99],
            vec![0, 0, 7, 9, 11],
            vec![0, u32::MAX, 0, 9, 11],
            vec![0, 12, 7, 9, 11], // JSON versus identical physical Utf8.
            vec![0, u32::MAX, 13, 9, 11], // LargeInt versus physical fixed-16.
        ] {
            c.arm(None);
            assert!(encode_writer_schema(&schema, &ids, types, SOURCE, limits(), &c).is_err());
        }
        let mut metadata_source = schema.clone();
        if let DataType::List(field) = &metadata_source.fields[4].ty.data_type {
            metadata_source.fields[4].ty.data_type = DataType::List(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_metadata([("source.key".into(), "changed".into())].into()),
            ));
        }
        c.arm(None);
        assert!(
            encode_writer_schema(
                &metadata_source,
                &[0, u32::MAX, 7, 9, 11],
                types,
                SOURCE,
                limits(),
                &c
            )
            .is_err()
        );
        for shape in 0..5 {
            let mut raw = expected_schema();
            match shape {
                0 => raw.fields[0].value_id = None,
                1 => raw.fields[0].value_type_id = None,
                2 => raw.fields[0].value_type_id = Some(99),
                3 => raw.fields[0].role = 0,
                4 => raw.fields[0].role = i32::MAX,
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        }
        for shape in 0..5 {
            let mut raw = expected_targets();
            match shape {
                0 => raw[0].input_value_id = None,
                1 => raw[0].value_type_id = None,
                2 => raw[0].value_type_id = Some(99),
                3 => {
                    raw[0].token.pop();
                }
                4 => raw[0].token.push(0),
                _ => unreachable!(),
            };
            c.arm(None);
            assert!(decode_writer_target_fields(&raw, read, SOURCE, limits(), &c).is_err());
        }
        let empty = p::WriterRelationSchema {
            revision: 0,
            fields: Box::default(),
        };
        c.arm(None);
        let raw = encode_writer_schema(&empty, &[], types, SOURCE, limits(), &c)
            .unwrap()
            .0;
        assert_eq!(
            raw,
            wire::WriterRelationSchema {
                revision: 0,
                fields: vec![]
            }
        );
        c.arm(None);
        assert_eq!(
            decode_writer_schema(&raw, read, SOURCE, limits(), &c)
                .unwrap()
                .0,
            empty
        );
    });
}

fn small_schema() -> p::WriterRelationSchema {
    p::WriterRelationSchema {
        revision: 0,
        fields: Box::from([p::WriterRelationField {
            value: p::ValueId::new(0),
            name: "a".into(),
            ty: FunctionValueType::new(DataType::Int64, false),
            role: p::WriterRelationFieldRole::Auxiliary,
        }]),
    }
}
fn small_raw() -> wire::WriterRelationSchema {
    wire::WriterRelationSchema {
        revision: 0,
        fields: vec![wire::WriterRelationField {
            value_id: Some(0),
            name: "a".into(),
            value_type_id: Some(0),
            role: wire::WriterRelationFieldRole::Auxiliary as i32,
        }],
    }
}
fn small_target() -> Box<[p::WriterTargetField]> {
    Box::from([p::WriterTargetField {
        token: ConnectorWriteFieldToken::from_bytes([7; 32]),
        provider_name: "a".into(),
        input: p::ValueId::new(0),
        ty: FunctionValueType::new(DataType::Int64, false),
        hidden: false,
    }])
}
fn small_raw_target() -> Vec<wire::WriterTargetField> {
    vec![wire::WriterTargetField {
        token: vec![7; 32],
        provider_name: "a".into(),
        input_value_id: Some(0),
        value_type_id: Some(0),
        hidden: false,
    }]
}
#[test]
fn writer_actual_capacity_source_and_count_work_prefix_are_admitted_before_outputs() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        let mut raw = small_raw();
        raw.fields
            .reserve_exact(SOURCE / size_of::<wire::WriterRelationField>() + 1);
        c.arm(None);
        assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        let mut targets = small_raw_target();
        targets.reserve_exact(SOURCE / size_of::<wire::WriterTargetField>() + 1);
        c.arm(None);
        assert!(decode_writer_target_fields(&targets, read, SOURCE, limits(), &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        let mut raw = small_raw();
        raw.fields[0].name.reserve_exact(SOURCE);
        c.arm(None);
        assert!(decode_writer_schema(&raw, read, SOURCE, limits(), &c).is_err());
        let mut targets = small_raw_target();
        targets[0].token.reserve_exact(SOURCE);
        c.arm(None);
        assert!(decode_writer_target_fields(&targets, read, SOURCE, limits(), &c).is_err());
        let mut wide = small_schema();
        wide.fields = vec![wide.fields[0].clone(); 320].into_boxed_slice();
        let ids = vec![0; 320];
        let mut wire = small_raw();
        wire.fields = vec![wire.fields[0].clone(); 320];
        let low = WriterSchemaProjectionLimits {
            max_work: 1024,
            ..limits()
        };
        c.arm(None);
        assert!(encode_writer_schema(&wide, &ids, types, SOURCE, low, &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        c.arm(None);
        assert!(decode_writer_schema(&wire, read, SOURCE, low, &c).is_err());
        assert!(!c.trace().iter().any(|(_, u)| *u == 256));
        c.arm(None);
        assert!(encode_writer_schema(&small_schema(), &[0], types, 0, limits(), &c).is_err());
        c.arm(None);
        assert!(decode_writer_target_fields(&small_raw_target(), read, 0, limits(), &c).is_err());
    });
}
#[test]
fn writer_all_small_callback_prefixes_and_wide_actual_name_copy_preserve_primary() {
    let c = Control::default();
    with_types(&c, |_, types, read| {
        for route in 0..4 {
            for ordinary in [false, true] {
                let schema = small_schema();
                let mut raw = small_raw();
                let target = small_target();
                let mut rt = small_raw_target();
                let id = if ordinary { 99 } else { 0 };
                if ordinary {
                    raw.fields[0].role = 0;
                    rt[0].token.pop();
                }
                let invoke = || match route {
                    0 => encode_writer_schema(&schema, &[id], types, SOURCE, limits(), &c)
                        .map(|_| ()),
                    1 => decode_writer_schema(&raw, read, SOURCE, limits(), &c).map(|_| ()),
                    2 => encode_writer_target_fields(&target, &[id], types, SOURCE, limits(), &c)
                        .map(|_| ()),
                    3 => decode_writer_target_fields(&rt, read, SOURCE, limits(), &c).map(|_| ()),
                    _ => unreachable!(),
                };
                c.arm(None);
                assert_eq!(invoke().is_err(), ordinary);
                let trace = c.trace();
                assert_eq!(
                    trace[0],
                    (
                        if route == 0 || route == 2 {
                            CompilePhase::Encode
                        } else {
                            CompilePhase::Decode
                        },
                        0
                    )
                );
                if !ordinary && (route == 1 || route == 3) {
                    // Vec-to-Box opaque exit and the successful finish both
                    // observe zero pending units; no synthetic step is added.
                    assert_eq!(trace.last(), Some(&(CompilePhase::Decode, 0)));
                } else {
                    assert!(trace.last().is_some_and(|(_, u)| *u > 0));
                }
                for at in 0..trace.len() {
                    for cause in CAUSES {
                        c.arm(Some((at, cause)));
                        assert!(
                            matches!(invoke(),Err(Error::Control(actual))if actual==cause),
                            "route {route}, ordinary {ordinary}, at {at}"
                        );
                        assert_eq!(c.trace(), trace[..=at]);
                    }
                }
            }
        }
        let mut schema = small_schema();
        schema.fields[0].name = "é值".repeat(65536).into_boxed_str();
        let mut raw = small_raw();
        raw.fields[0].name = "é值".repeat(65536);
        for decode in [false, true] {
            let invoke = || {
                if decode {
                    decode_writer_schema(&raw, read, SOURCE, limits(), &c).map(|_| ())
                } else {
                    encode_writer_schema(&schema, &[0], types, SOURCE, limits(), &c).map(|_| ())
                }
            };
            c.arm(None);
            assert!(invoke().is_ok());
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause)));
                assert!(matches!(invoke(),Err(Error::Control(actual))if actual==cause));
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
        // Separate genuine 320-field source, sampled at its actual quantum. The
        // source invoice is an explicit whole fixture union, not a MEM grant.
        let mut wide = small_schema();
        wide.fields = vec![wide.fields[0].clone(); 320].into_boxed_slice();
        let ids = vec![0; 320];
        let mut wire = small_raw();
        wire.fields = vec![wire.fields[0].clone(); 320];
        for decode in [false, true] {
            let invoke = || {
                if decode {
                    decode_writer_schema(&wire, read, 128 << 10, limits(), &c).map(|_| ())
                } else {
                    encode_writer_schema(&wide, &ids, types, 128 << 10, limits(), &c).map(|_| ())
                }
            };
            c.arm(None);
            assert!(invoke().is_ok());
            let trace = c.trace();
            let at = trace.iter().position(|(_, u)| *u == 256).unwrap();
            for cause in CAUSES {
                c.arm(Some((at, cause)));
                assert!(matches!(invoke(),Err(Error::Control(actual))if actual==cause));
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    });
}
