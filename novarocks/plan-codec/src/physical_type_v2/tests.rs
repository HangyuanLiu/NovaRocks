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
use arrow::datatypes::{IntervalUnit, TimeUnit, UnionFields, UnionMode};
use novarocks_proto_models::plan;
use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, NR_LOGICAL_TYPE_KEY};
use std::{collections::HashMap, sync::Mutex};
use wire::carrier_type_definition::Kind;

#[derive(Default)]
struct Control {
    fail: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        events.push((phase, units));
        if let Some((at, error)) = self.fail
            && at == events.len()
        {
            return Err(error);
        }
        Ok(())
    }
}
fn limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100_000,
        max_expanded_nodes: 100_000,
        max_string_bytes: 100_000,
    }
}
fn c(id: u32, kind: Kind) -> wire::CarrierTypeDefinition {
    wire::CarrierTypeDefinition {
        id,
        kind: Some(kind),
    }
}
fn f(id: u32, carrier: u32) -> wire::FieldDefinition {
    wire::FieldDefinition {
        id,
        name: "x".into(),
        nullable: true,
        carrier_type_id: Some(carrier),
        metadata: vec![],
        dictionary_id: None,
        dictionary_is_ordered: None,
    }
}
fn v(id: u32, carrier: u32, logical: i32) -> wire::ValueTypeDefinition {
    wire::ValueTypeDefinition {
        id,
        carrier_type_id: Some(carrier),
        nullable: true,
        logical_type: logical,
    }
}
fn sparse_list() -> wire::TypeTable {
    // Three separate namespaces intentionally reuse zero and MAX.
    wire::TypeTable {
        carriers: vec![c(u32::MAX, Kind::ListFieldId(0)), c(0, Kind::Primitive(5))],
        fields: vec![f(0, 0)],
        value_types: vec![v(u32::MAX, u32::MAX, 1)],
    }
}
fn reject(table: &wire::TypeTable) {
    assert!(decode_type_table(table, limits(), &Control::default()).is_err());
}
fn roundtrip(values: &[(u32, FunctionValueType)]) -> (wire::TypeTable, DecodedTypeTable) {
    let dto = encode_type_table(values, limits(), &Control::default()).unwrap();
    let decoded = decode_type_table(&dto, limits(), &Control::default()).unwrap();
    assert_eq!(decoded.value_types().len(), values.len());
    for (id, value) in values {
        assert_eq!(decoded.value_type(*id), Some(value));
    }
    (dto, decoded)
}

#[test]
fn handauthored_sparse_list_resolves_exact_separate_namespaces() {
    let table = sparse_list();
    let decoded = decode_type_table(&table, limits(), &Control::default()).unwrap();
    let child = Arc::new(Field::new("x", DataType::Int32, true));
    let expected = FunctionValueType::new(DataType::List(child.clone()), true);
    assert_eq!(decoded.value_type(u32::MAX), Some(&expected));
    assert_eq!(decoded.carrier(0), Some(&DataType::Int32));
    assert!(novarocks_type_contract::arrow_fields_exact(
        decoded.field(0).unwrap(),
        &child
    ));
    assert!(decoded.carrier(1).is_none());
    assert!(decoded.field(u32::MAX).is_none());
}

#[test]
fn fixed_binary_sixteen_preserves_three_explicit_root_identities_without_inference() {
    let table = wire::TypeTable {
        carriers: vec![c(u32::MAX, Kind::FixedSizeBinary(16))],
        fields: vec![],
        value_types: vec![
            v(0, u32::MAX, 1),
            v(9, u32::MAX, 8),
            v(u32::MAX, u32::MAX, 9),
        ],
    };
    let decoded = decode_type_table(&table, limits(), &Control::default()).unwrap();
    for (id, logical) in [
        (0, ValueLogicalType::Physical),
        (9, ValueLogicalType::LargeInt),
        (u32::MAX, ValueLogicalType::Uuid),
    ] {
        let expected =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        assert_eq!(decoded.value_type(id), Some(&expected));
    }
    assert_ne!(decoded.value_type(0), decoded.value_type(9));
    assert_ne!(decoded.value_type(9), decoded.value_type(u32::MAX));
}

#[test]
#[allow(deprecated)]
fn every_nested_carrier_roundtrips_complete_fields_dictionary_metadata_and_union_order() {
    let json = Arc::new(
        Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
            ("provider".into(), "authored".into()),
        ])),
    );
    let dict = Arc::new(
        Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            false,
            -99,
            true,
        )
        .with_metadata(HashMap::from([
            ("z".into(), "last".into()),
            ("a".into(), "first".into()),
        ])),
    );
    let pair = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                json.clone(),
            ]
            .into(),
        ),
        false,
    ));
    let ends = Arc::new(Field::new("run_ends", DataType::Int32, false));
    let union: UnionFields = [(7, dict.clone()), (1, json.clone())].into_iter().collect();
    let types = vec![
        DataType::List(json.clone()),
        DataType::ListView(json.clone()),
        DataType::LargeList(json.clone()),
        DataType::LargeListView(json.clone()),
        DataType::FixedSizeList(json.clone(), 3),
        DataType::Struct(vec![json.clone(), dict.clone(), json.clone()].into()),
        DataType::Union(union.clone(), UnionMode::Sparse),
        DataType::Union(union, UnionMode::Dense),
        dict.data_type().clone(),
        DataType::Map(pair, true),
        DataType::RunEndEncoded(ends, json),
    ];
    let values: Vec<_> = types
        .into_iter()
        .enumerate()
        .map(|(i, t)| (i as u32, FunctionValueType::new(t, i % 2 == 0)))
        .collect();
    let (dto, decoded) = roundtrip(&values);
    let dictionary = dto
        .fields
        .iter()
        .find(|field| field.name == "dictionary")
        .unwrap();
    assert_eq!(dictionary.dictionary_id, Some(-99));
    assert_eq!(dictionary.dictionary_is_ordered, Some(true));
    assert_eq!(
        dictionary
            .metadata
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
    for id in [6, 7] {
        let DataType::Union(fields, _) = &decoded.value_type(id).unwrap().data_type else {
            panic!("expected union");
        };
        assert_eq!(fields.iter().map(|(id, _)| id).collect::<Vec<_>>(), [7, 1]);
    }
    let DataType::Struct(fields) = &decoded.value_type(5).unwrap().data_type else {
        panic!("expected struct");
    };
    assert!(novarocks_type_contract::arrow_fields_exact(
        &fields[0], &fields[2]
    ));
}

#[test]
fn complete_scalar_logical_domains_and_temporal_facts_survive_public_table_apis() {
    let mut values = Vec::new();
    for (logical, carrier) in [
        (ValueLogicalType::Json, DataType::Utf8),
        (ValueLogicalType::Variant, DataType::LargeBinary),
        (ValueLogicalType::Hll, DataType::Binary),
        (ValueLogicalType::Bitmap, DataType::Binary),
        (ValueLogicalType::Object, DataType::LargeBinary),
        (ValueLogicalType::Percentile, DataType::Binary),
    ] {
        values.push((
            values.len() as u32,
            FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap(),
        ));
    }
    for ty in [
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        DataType::Timestamp(TimeUnit::Second, Some("Asia/Shanghai".into())),
        DataType::Decimal32(9, -128),
        DataType::Decimal64(18, -5),
        DataType::Decimal128(38, -46),
        DataType::Decimal256(76, 76),
        DataType::Duration(TimeUnit::Millisecond),
        DataType::Interval(IntervalUnit::MonthDayNano),
    ] {
        values.push((values.len() as u32, FunctionValueType::new(ty, false)));
    }
    roundtrip(&values);
}

#[test]
fn duplicates_missing_kind_and_required_references_are_rejected_in_each_namespace() {
    let table = sparse_list();
    for namespace in 0..3 {
        let mut bad = table.clone();
        match namespace {
            0 => bad.carriers.push(bad.carriers[0].clone()),
            1 => bad.fields.push(bad.fields[0].clone()),
            _ => bad.value_types.push(bad.value_types[0]),
        }
        reject(&bad);
    }
    let mut bad = table.clone();
    bad.carriers[0].kind = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.fields[0].carrier_type_id = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.value_types[0].carrier_type_id = None;
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::FixedSizeList(wire::FixedSizeList {
        item_field_id: None,
        length: 1,
    }));
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::Dictionary(wire::DictionaryTypes {
        key_type_id: None,
        value_type_id: Some(0),
    }));
    reject(&bad);
    let mut bad = table.clone();
    bad.carriers[0].kind = Some(Kind::Map(wire::MapField {
        entries_field_id: None,
        ordered: false,
    }));
    reject(&bad);
    let mut bad = table;
    bad.carriers[0].kind = Some(Kind::RunEndEncoded(wire::RunEndEncodedFields {
        run_ends_field_id: Some(0),
        values_field_id: None,
    }));
    reject(&bad);
}

#[test]
fn cross_namespace_ids_cannot_satisfy_missing_carrier_or_field_references() {
    let mut table = sparse_list();
    table.carriers[0].kind = Some(Kind::ListFieldId(u32::MAX));
    // A carrier and a value with this ID exist, but the field namespace does not.
    reject(&table);
    let mut table = sparse_list();
    table.fields[0].carrier_type_id = Some(31);
    table.value_types.push(v(31, 0, 1));
    reject(&table);
    let mut table = sparse_list();
    table.value_types[0].carrier_type_id = Some(31);
    table.fields.push(f(31, 0));
    reject(&table);
}

#[test]
fn carrier_field_and_dictionary_cycles_fail_before_materialization() {
    let mut table = sparse_list();
    table.fields[0].carrier_type_id = Some(u32::MAX);
    reject(&table);
    let table = wire::TypeTable {
        carriers: vec![
            c(0, Kind::Primitive(5)),
            c(
                u32::MAX,
                Kind::Dictionary(wire::DictionaryTypes {
                    key_type_id: Some(0),
                    value_type_id: Some(u32::MAX),
                }),
            ),
        ],
        fields: vec![],
        value_types: vec![v(0, u32::MAX, 1)],
    };
    reject(&table);
}

fn nested_list(depth: usize) -> wire::TypeTable {
    let mut table = wire::TypeTable {
        carriers: vec![c(0, Kind::Primitive(5))],
        fields: vec![],
        value_types: vec![v(0, (depth - 1) as u32, 1)],
    };
    for id in 1..depth {
        table.fields.push(f(id as u32, (id - 1) as u32));
        table
            .carriers
            .push(c(id as u32, Kind::ListFieldId(id as u32)));
    }
    table
}
#[test]
fn actual_type_depth_sixty_four_is_valid_and_sixty_five_is_rejected() {
    let decoded = decode_type_table(
        &nested_list(MAX_VALUE_TYPE_DEPTH),
        limits(),
        &Control::default(),
    )
    .unwrap();
    let mut cursor = &decoded.value_type(0).unwrap().data_type;
    for _ in 1..MAX_VALUE_TYPE_DEPTH {
        let DataType::List(field) = cursor else {
            panic!("missing list layer");
        };
        cursor = field.data_type();
    }
    assert_eq!(cursor, &DataType::Int32);
    reject(&nested_list(MAX_VALUE_TYPE_DEPTH + 1));
}
fn shared_doubling(levels: usize) -> wire::TypeTable {
    let mut table = wire::TypeTable {
        carriers: vec![c(0, Kind::Primitive(5))],
        fields: vec![],
        value_types: vec![v(0, levels as u32, 1)],
    };
    for id in 1..=levels {
        table.fields.push(f(id as u32, (id - 1) as u32));
        table.carriers.push(c(
            id as u32,
            Kind::StructType(wire::StructFields {
                field_ids: vec![id as u32; 2],
            }),
        ));
    }
    table
}
#[test]
fn repeated_reference_dag_expansion_enforces_owner_node_bound_before_constructing_arrow_tree() {
    let near = shared_doubling(11); // 4095 expanded type nodes, despite only 12 carriers.
    decode_type_table(&near, limits(), &Control::default()).unwrap();
    reject(&shared_doubling(12)); // 8191 expanded type nodes.
}

#[test]
fn projection_definition_limits_count_all_three_namespaces_including_unused_definitions() {
    let base = sparse_list();
    let exact = TypeProjectionLimits {
        max_definitions: 4,
        ..limits()
    };
    decode_type_table(&base, exact, &Control::default()).unwrap();
    assert!(
        decode_type_table(
            &base,
            TypeProjectionLimits {
                max_definitions: 3,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    for namespace in 0..3 {
        let mut table = base.clone();
        match namespace {
            0 => table.carriers.push(c(31, Kind::Primitive(5))),
            1 => table.fields.push(f(31, 0)),
            _ => table.value_types.push(v(31, 0, 1)),
        }
        assert!(decode_type_table(&table, exact, &Control::default()).is_err());
    }
    let values = [(
        u32::MAX,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("x", DataType::Int32, true))),
            true,
        ),
    )];
    assert_eq!(
        encode_type_table(&values, exact, &Control::default())
            .unwrap()
            .carriers
            .len(),
        2
    );
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_definitions: 3,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn projection_expanded_limit_counts_carrier_field_and_value_roots_with_repeated_children() {
    let base = sparse_list(); // C0=1,Cmax=2,F0=1,Vmax=2 => 6.
    decode_type_table(
        &base,
        TypeProjectionLimits {
            max_expanded_nodes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &base,
            TypeProjectionLimits {
                max_expanded_nodes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let mut repeated = base;
    repeated.carriers[0].kind = Some(Kind::StructType(wire::StructFields {
        field_ids: vec![0, 0],
    })); // 1+3+1+3=8.
    decode_type_table(
        &repeated,
        TypeProjectionLimits {
            max_expanded_nodes: 8,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &repeated,
            TypeProjectionLimits {
                max_expanded_nodes: 7,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let values = [(
        0,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("x", DataType::Int32, true))),
            true,
        ),
    )];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_expanded_nodes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_expanded_nodes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
#[allow(deprecated)]
fn dictionary_field_requires_both_optional_attributes_and_rejects_attributes_on_other_carriers() {
    let table = wire::TypeTable {
        carriers: vec![
            c(0, Kind::Primitive(5)),
            c(1, Kind::Primitive(19)),
            c(
                2,
                Kind::Dictionary(wire::DictionaryTypes {
                    key_type_id: Some(0),
                    value_type_id: Some(1),
                }),
            ),
            c(3, Kind::ListFieldId(0)),
        ],
        fields: vec![wire::FieldDefinition {
            dictionary_id: Some(i64::MIN),
            dictionary_is_ordered: Some(false),
            ..f(0, 2)
        }],
        value_types: vec![v(0, 3, 1)],
    };
    let result = decode_type_table(&table, limits(), &Control::default()).unwrap();
    assert_eq!(result.field(0).unwrap().dict_id(), Some(i64::MIN));
    assert_eq!(result.field(0).unwrap().dict_is_ordered(), Some(false));
    for missing in 0..3 {
        let mut bad = table.clone();
        if missing != 1 {
            bad.fields[0].dictionary_id = None;
        }
        if missing != 0 {
            bad.fields[0].dictionary_is_ordered = None;
        }
        reject(&bad);
    }
    for extra in 0..3 {
        let mut bad = sparse_list();
        if extra != 1 {
            bad.fields[0].dictionary_id = Some(0);
        }
        if extra != 0 {
            bad.fields[0].dictionary_is_ordered = Some(false);
        }
        reject(&bad);
    }
}

#[test]
fn field_metadata_must_be_sorted_unique_and_retains_complete_source_entries() {
    let entry = |key: &str, value: &str| plan::ArrowFieldMetadataEntry {
        key: key.into(),
        value: value.into(),
    };
    let mut valid = sparse_list();
    valid.fields[0].metadata = vec![entry("a", "first"), entry("z", "last")];
    let decoded = decode_type_table(&valid, limits(), &Control::default()).unwrap();
    assert_eq!(
        decoded
            .field(0)
            .unwrap()
            .metadata()
            .get("z")
            .map(String::as_str),
        Some("last")
    );
    let mut unsorted = valid.clone();
    unsorted.fields[0].metadata.reverse();
    reject(&unsorted);
    let mut duplicate = valid;
    duplicate.fields[0].metadata = vec![entry("a", "first"), entry("a", "last")];
    reject(&duplicate);
}

#[test]
fn logical_tags_are_required_and_must_match_actual_root_and_nested_carriers() {
    for logical in [0, -1, i32::MAX, 2, 8, 9] {
        let mut bad = sparse_list();
        bad.value_types[0].logical_type = logical;
        reject(&bad);
    }
    let mut bad = sparse_list();
    bad.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: NR_LOGICAL_TYPE_KEY.into(),
        value: "json".into(),
    }];
    reject(&bad);
    let mut bad = sparse_list();
    bad.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: NR_LOGICAL_TYPE_KEY.into(),
        value: "unknown".into(),
    }];
    reject(&bad);
}

#[test]
fn original_control_covers_real_wide_type_work_entry_256_tail_and_final_publication() {
    let values: Vec<_> = (0..320)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    let encode_control = Control::default();
    let dto = encode_type_table(&values, limits(), &encode_control).unwrap();
    let encode_trace = encode_control.events.into_inner().unwrap();
    let decode_control = Control::default();
    decode_type_table(&dto, limits(), &decode_control).unwrap();
    let decode_trace = decode_control.events.into_inner().unwrap();
    for trace in [&encode_trace, &decode_trace] {
        assert_eq!(trace[0].1, 0);
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().any(|(_, units)| *units > 0 && *units < 256));
    }
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=encode_trace.len() {
            let stop = Control {
                fail: Some((at, error)),
                events: Mutex::default(),
            };
            assert!(
                matches!(encode_type_table(&values,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
        for at in 1..=decode_trace.len() {
            let stop = Control {
                fail: Some((at, error)),
                events: Mutex::default(),
            };
            assert!(
                matches!(decode_type_table(&dto,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
    }
}

#[test]
fn malformed_tail_still_observes_original_control_without_publishing_a_prefix() {
    let mut dto = wire::TypeTable {
        carriers: (0..320).map(|id| c(id, Kind::Primitive(5))).collect(),
        fields: vec![],
        value_types: vec![v(0, 0, 1)],
    };
    dto.carriers[319].kind = None;
    let good = Control::default();
    assert!(decode_type_table(&dto, limits(), &good).is_err());
    let trace = good.events.into_inner().unwrap();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let stop = Control {
                fail: Some((at, error)),
                events: Mutex::default(),
            };
            assert!(
                matches!(decode_type_table(&dto,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
        }
    }
    let mut values: Vec<_> = (0..320)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    values[319].1.logical_type = ValueLogicalType::Json;
    let good = Control::default();
    assert!(encode_type_table(&values, limits(), &good).is_err());
    let trace = good.events.into_inner().unwrap();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let stop = Control {
                fail: Some((at, error)),
                events: Mutex::default(),
            };
            assert!(
                matches!(encode_type_table(&values,limits(),&stop),Err(TypeCodecError::Control(actual)) if actual==error)
            );
        }
    }
}

#[test]
fn projection_string_byte_bound_counts_flat_names_metadata_and_timestamp_zone_exactly() {
    let mut table = sparse_list();
    table.carriers[1].kind = Some(Kind::Timestamp(plan::ArrowTimestampType {
        unit: 4,
        timezone: Some("UTC".into()),
    }));
    table.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "a".into(),
        value: "b".into(),
    }];
    // Flat definition bytes: zone UTC (3) + name x (1) + metadata a:b (2).
    decode_type_table(
        &table,
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &table,
            TypeProjectionLimits {
                max_string_bytes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let field = Arc::new(
        Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )
        .with_metadata(HashMap::from([("a".into(), "b".into())])),
    );
    let values = [(0, FunctionValueType::new(DataType::List(field), true))];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_string_bytes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let mut unicode = sparse_list();
    unicode.fields[0].name = "é".into();
    unicode.fields[0].metadata = vec![plan::ArrowFieldMetadataEntry {
        key: "a".into(),
        value: "漢".into(),
    }];
    // Three characters occupy six UTF-8 bytes: 2 + 1 + 3.
    decode_type_table(
        &unicode,
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        decode_type_table(
            &unicode,
            TypeProjectionLimits {
                max_string_bytes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
    let field = Arc::new(
        Field::new("é", DataType::Int32, true)
            .with_metadata(HashMap::from([("a".into(), "漢".into())])),
    );
    let values = [(0, FunctionValueType::new(DataType::List(field), true))];
    encode_type_table(
        &values,
        TypeProjectionLimits {
            max_string_bytes: 6,
            ..limits()
        },
        &Control::default(),
    )
    .unwrap();
    assert!(
        encode_type_table(
            &values,
            TypeProjectionLimits {
                max_string_bytes: 5,
                ..limits()
            },
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn many_independent_valid_values_do_not_acquire_a_global_4096_type_table_limit() {
    let values: Vec<_> = (0..4100)
        .map(|id| (id, FunctionValueType::new(DataType::Int32, false)))
        .collect();
    let (dto, decoded) = roundtrip(&values);
    assert_eq!(dto.value_types.len(), 4100);
    assert_eq!(decoded.value_types().len(), 4100);
}
