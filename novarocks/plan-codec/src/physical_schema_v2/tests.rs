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
use crate::{
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Mutex;
const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        let stop = *self.stop.lock().unwrap();
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
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 8 << 20,
        max_coexisting_source_and_request_bytes: 16 << 20,
        max_work: 64 << 30,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: 16 << 20,
            max_work: 64 << 30,
        },
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 8192,
        max_string_bytes: 1 << 20,
    }
}
fn field() -> Arc<Field> {
    Arc::new(
        Field::new("original", DataType::Int64, true)
            .with_metadata([("field.tag".into(), "unmodified".into())].into()),
    )
}
fn metadata() -> HashMap<String, String> {
    [
        ("".into(), "".into()),
        ("a\0".into(), "雪\0".into()),
        ("雪".into(), "value".into()),
    ]
    .into()
}
fn run_encode<'loan, 'source, 'control>(
    sources: &'loan [SchemaSource<'source>],
    types: &'loan EncodedTypeTable<'source>,
    control: &'control Control,
) -> Result<EncodedSchemas<'loan, 'source, 'control>, Error> {
    prepare_schemas_encode(sources, types, SOURCE, limits(), control)?.emit()
}
fn run_decode<'loan, 'control>(
    wire: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    control: &'control Control,
) -> Result<DecodedSchemas<'loan, 'control>, Error> {
    prepare_schemas_decode(wire, types, SOURCE, limits(), control)?.emit()
}
fn empty_types(control: &Control) -> DecodedTypeTable {
    let wire = encode_type_table_sources(&[], &[], type_limits(), control).unwrap();
    decode_type_table(wire.as_wire(), type_limits(), control).unwrap()
}
fn ordinary(result: Result<(), Error>) {
    assert!(result.is_err());
    assert!(!matches!(result, Err(Error::Control(_))));
}

#[test]
fn schemas_preserve_zero_max_ordered_repeated_fields_and_empty_root() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone()), (u32::MAX, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new(vec![original.clone(), original.clone(), original]);
    let empty = Schema::empty();
    let sources = [
        SchemaSource {
            id: u32::MAX,
            source: &schema,
            field_ids: &[u32::MAX, 0, u32::MAX],
        },
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
    ];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert_eq!(
        encoded.as_wire(),
        [
            wire::SchemaDefinition {
                id: u32::MAX,
                field_ids: vec![u32::MAX, 0, u32::MAX],
                metadata: vec![]
            },
            wire::SchemaDefinition {
                id: 0,
                field_ids: vec![],
                metadata: vec![]
            }
        ]
    );
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    assert_eq!(decoded.definitions()[0].0, u32::MAX);
    assert_eq!(decoded.definitions()[1].0, 0);
    assert!(decoded.definitions()[1].1.fields().is_empty());
    let output = decoded.definitions()[0].1.fields();
    assert!(Arc::ptr_eq(&output[0], read.field(u32::MAX).unwrap()));
    assert!(Arc::ptr_eq(&output[1], read.field(0).unwrap()));
    assert!(Arc::ptr_eq(&output[2], read.field(u32::MAX).unwrap()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert!(decoded.schema_observed(7, &mut work).unwrap().is_none());
    assert!(std::ptr::eq(
        decoded.schema_observed(0, &mut work).unwrap().unwrap(),
        &decoded.definitions()[1].1
    ));
    work.finish().unwrap();
    let foreign = Control::default();
    let mut work = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    ordinary(decoded.schema_observed(0, &mut work).map(|_| ()));
}
#[test]
#[allow(deprecated)]
fn schema_metadata_hand_oracle_is_independent_of_complete_field_metadata() {
    let control = Control::default();
    let nested = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
    .try_to_field("large")
    .unwrap();
    let dictionary = Arc::new(
        Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
            77,
            true,
        )
        .with_metadata([("unknown.field".into(), "雪".into())].into()),
    );
    let structure = Arc::new(Field::new(
        "nested",
        DataType::Struct(vec![Arc::new(nested)].into()),
        false,
    ));
    let roots = [(0, dictionary.clone()), (u32::MAX, structure.clone())];
    let schema = Schema::new_with_metadata(vec![dictionary, structure], metadata());
    let sources = [SchemaSource {
        id: 7,
        source: &schema,
        field_ids: &[0, u32::MAX],
    }];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert_eq!(
        encoded.as_wire()[0].metadata,
        vec![
            ArrowFieldMetadataEntry {
                key: "".into(),
                value: "".into()
            },
            ArrowFieldMetadataEntry {
                key: "a\0".into(),
                value: "雪\0".into()
            },
            ArrowFieldMetadataEntry {
                key: "雪".into(),
                value: "value".into()
            }
        ]
    );
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let actual = &decoded.definitions()[0].1;
    assert_eq!(actual.metadata(), &metadata());
    assert_eq!(actual.fields()[0].dict_id(), Some(77));
    assert_eq!(actual.fields()[0].dict_is_ordered(), Some(true));
    assert_eq!(
        actual.fields()[0].metadata().get("unknown.field").unwrap(),
        "雪"
    );
    assert!(!actual.fields()[1].is_nullable());
    let DataType::Struct(fields) = actual.fields()[1].data_type() else {
        panic!()
    };
    assert_eq!(
        novarocks_type_contract::field_logical_type(&fields[0]).unwrap(),
        ValueLogicalType::LargeInt
    );
    assert!(Arc::ptr_eq(
        &actual.fields()[1],
        read.field(u32::MAX).unwrap()
    ));
}
#[test]
fn schema_field_namespace_requires_original_arc_and_real_connector_schema() {
    use novarocks_connector_contract::{
        ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
        ConnectorReadProperties, ConnectorReadPublicFacts, ConnectorReadStaticFacts, ScanColumnId,
    };
    let control = Control::default();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new(vec![1]).unwrap(),
        [1; 32],
        ConnectorReadProperties::<ScanColumnId>::try_new(
            ConnectorReadDistribution::Unconstrained,
            Vec::new(),
        )
        .unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        Vec::<u8>::new(),
    )
    .unwrap();
    let facts = ConnectorReadPublicFacts::try_new(
        source,
        None,
        Schema::new(vec![field()]),
        vec![ValueLogicalType::Physical],
    )
    .unwrap();
    let roots = [(u32::MAX, facts.schema().fields()[0].clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let sources = [SchemaSource {
        id: 0,
        source: facts.schema(),
        field_ids: &[u32::MAX],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert_eq!(encoded.as_wire()[0].field_ids, [u32::MAX]);
    let wrong_count = [SchemaSource {
        id: 0,
        source: facts.schema(),
        field_ids: &[],
    }];
    ordinary(run_encode(&wrong_count, &types, &control).map(|_| ()));
    let foreign = Schema::new(vec![Arc::new(roots[0].1.as_ref().clone())]);
    let sources = [SchemaSource {
        id: 0,
        source: &foreign,
        field_ids: &[u32::MAX],
    }];
    ordinary(run_encode(&sources, &types, &control).map(|_| ()));
    let bad = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![7],
        metadata: vec![],
    }];
    ordinary(run_decode(&bad, &read, &control).map(|_| ()));
}
#[test]
fn schema_metadata_duplicate_unsorted_and_duplicate_ids_never_normalize() {
    let control = Control::default();
    let types = empty_types(&control);
    for keys in [["a", "a"], ["b", "a"]] {
        let input = [wire::SchemaDefinition {
            id: 0,
            field_ids: vec![],
            metadata: keys
                .into_iter()
                .map(|key| ArrowFieldMetadataEntry {
                    key: key.into(),
                    value: "".into(),
                })
                .collect(),
        }];
        ordinary(run_decode(&input, &types, &control).map(|_| ()));
    }
    let duplicates = [
        wire::SchemaDefinition {
            id: u32::MAX,
            field_ids: vec![],
            metadata: vec![],
        },
        wire::SchemaDefinition {
            id: u32::MAX,
            field_ids: vec![],
            metadata: vec![],
        },
    ];
    assert!(matches!(
        run_decode(&duplicates, &types, &control),
        Err(Error::Binding(BindingCodecError::InvalidShape(_)))
    ));
    let empty = Schema::empty();
    let roots = [];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let sources = [
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
        SchemaSource {
            id: 0,
            source: &empty,
            field_ids: &[],
        },
    ];
    ordinary(run_encode(&sources, &types, &control).map(|_| ()));
}
fn tighten(f: NodeProjectionFacts) -> NodeProjectionLimits {
    let mut l = limits();
    l.max_value_references = f.value_reference_count;
    l.max_list_items = f.list_item_count;
    l.max_allocation_requests = f.allocation_requests_upper_bound;
    l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.max_coexisting_source_and_request_bytes = f.coexisting_source_and_request_bytes_upper_bound;
    l.max_work = f.cumulative_work_upper_bound;
    l
}
#[test]
fn schema_request_layout_golden_includes_empty_fields_arc_and_persistent_index() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new_with_metadata(vec![original.clone(), original], metadata());
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[0, 0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let string_bytes = "a\0".len() + "雪\0".len() + "雪".len() + "value".len();
    let sent = encoded.facts();
    let send_bytes = Layout::array::<usize>(1).unwrap().size()
        + Layout::array::<wire::SchemaDefinition>(1).unwrap().size()
        + Layout::array::<u32>(2).unwrap().size()
        + Layout::array::<(&str, &str)>(3).unwrap().size()
        + Layout::array::<ArrowFieldMetadataEntry>(3).unwrap().size()
        + string_bytes;
    assert_eq!(sent.allocation_requests_upper_bound, 9);
    assert_eq!(sent.allocation_request_bytes_upper_bound, send_bytes);
    // Locked aarch64 Group8 or x86 SSE2 Group16; String pairs use 4 buckets
    // for three entries. This oracle uses public actual pair/header Layout.
    let group = if cfg!(all(
        target_arch = "x86_64",
        target_feature = "sse2",
        not(miri)
    )) {
        16
    } else {
        8
    };
    let pair = Layout::new::<(String, String)>();
    let align = pair.align().max(group);
    let table = (pair.size() * 4).div_ceil(align) * align + 4 + group;
    let arc = Layout::new::<[usize; 2]>()
        .extend(Layout::array::<Arc<Field>>(2).unwrap())
        .unwrap()
        .0
        .pad_to_align()
        .size();
    let receive_bytes = Layout::array::<usize>(1).unwrap().size()
        + 2 * Layout::array::<(u32, Schema)>(1).unwrap().size()
        + Layout::array::<Arc<Field>>(2).unwrap().size()
        + arc
        + table
        + string_bytes;
    assert_eq!(decoded.facts().allocation_requests_upper_bound, 10);
    assert_eq!(
        decoded.facts().allocation_request_bytes_upper_bound,
        receive_bytes
    );
    let empty = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![],
        metadata: vec![],
    }];
    let empty_decoded = run_decode(&empty, &read, &control).unwrap();
    assert_eq!(empty_decoded.facts().allocation_requests_upper_bound, 4);
    assert_eq!(
        empty_decoded.facts().allocation_request_bytes_upper_bound,
        Layout::array::<usize>(1).unwrap().size()
            + 2 * Layout::array::<(u32, Schema)>(1).unwrap().size()
            + Layout::new::<[usize; 2]>().size()
    );
    for receive in [false, true] {
        let facts = if receive { *decoded.facts() } else { *sent };
        let exact = tighten(facts);
        if receive {
            prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, exact, &control)
                .unwrap()
                .emit()
                .unwrap();
        } else {
            prepare_schemas_encode(&sources, &types, SOURCE, exact, &control)
                .unwrap()
                .emit()
                .unwrap();
        }
        for axis in 0..6 {
            let mut l = exact;
            let n = match axis {
                0 => &mut l.max_value_references,
                1 => &mut l.max_list_items,
                2 => &mut l.max_allocation_requests,
                3 => &mut l.max_allocation_request_bytes,
                4 => &mut l.max_coexisting_source_and_request_bytes,
                5 => &mut l.max_work,
                _ => unreachable!(),
            };
            assert!(*n > 0);
            *n -= 1;
            let result = if receive {
                prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, l, &control).map(|_| ())
            } else {
                prepare_schemas_encode(&sources, &types, SOURCE, l, &control).map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    }
}
#[test]
fn shared_schema_source_union_floor_and_deleted_hash_backing_remain_truthful() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let mut deleted = HashMap::new();
    deleted.reserve(4096);
    for id in 0..4096 {
        deleted.insert(id.to_string(), String::new());
    }
    deleted.clear();
    let schema = Schema::new_with_metadata(vec![original], deleted);
    let ids = [0];
    let sources = [
        SchemaSource {
            id: 0,
            source: &schema,
            field_ids: &ids,
        },
        SchemaSource {
            id: u32::MAX,
            source: &schema,
            field_ids: &ids,
        },
    ];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    assert!(encoded.as_wire().iter().all(|s| s.metadata.is_empty()));
    assert!(
        encoded.facts().cumulative_work_upper_bound
            >= 4 * maps::source_iterator_work_upper_bound(SOURCE, 0).unwrap()
    );
    let mut input = vec![wire::SchemaDefinition {
        id: 0,
        field_ids: vec![0],
        metadata: vec![ArrowFieldMetadataEntry {
            key: "key".into(),
            value: "value".into(),
        }],
    }];
    input[0].field_ids.reserve(64);
    input[0].metadata[0].value.reserve(4096);
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let known = bytes::<wire::SchemaDefinition>(1).unwrap()
        + read.necessary_fields_retained_floor().unwrap()
        + bytes::<u32>(input[0].field_ids.capacity()).unwrap()
        + bytes::<ArrowFieldMetadataEntry>(input[0].metadata.capacity()).unwrap()
        + input[0].metadata[0].key.capacity()
        + input[0].metadata[0].value.capacity();
    ordinary(prepare_schemas_decode(&input, &read, known - 1, limits(), &control).map(|_| ()));
    // Eight exact aliases share one 320-field Fields allocation and one IDs
    // slice. The caller invoice covers their actual union, including the
    // original Field, emitted type DTO and source-root handles. It is larger
    // than the distinct allocations, but smaller than eight invented Fields
    // allocations. A necessary lower floor is not the complete invoice.
    let shared = Schema::new(vec![field(); 320]);
    let shared_ids = vec![0; 320];
    let shared_roots = [(0, shared.fields()[0].clone())];
    let shared_types =
        encode_type_table_sources(&[], &shared_roots, type_limits(), &control).unwrap();
    let aliases = (0..8)
        .map(|id| SchemaSource {
            id,
            source: &shared,
            field_ids: &shared_ids,
        })
        .collect::<Vec<_>>();
    let shared_union_invoice = 16 << 10;
    let one_schema_floor =
        size_of::<Schema>() + fields_layout(320).unwrap().size() + bytes::<u32>(320).unwrap();
    assert!(one_schema_floor * aliases.len() > shared_union_invoice);
    prepare_schemas_encode(
        &aliases,
        &shared_types,
        shared_union_invoice,
        limits(),
        &control,
    )
    .unwrap()
    .emit()
    .unwrap();
}
fn prefixes(control: &Control, mut invoke: impl FnMut() -> Result<(), Error>, success: bool) {
    control.arm(None);
    assert_eq!(invoke().is_ok(), success);
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause)));
            assert!(matches!(invoke(),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    control.arm(None);
}
#[test]
fn every_small_schema_callback_preserves_three_primary_causes_and_ordinary_tail() {
    let control = Control::default();
    let original = field();
    let roots = [(0, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema = Schema::new_with_metadata(vec![original], metadata());
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[0],
    }];
    let encoded = run_encode(&sources, &types, &control).unwrap();
    prefixes(
        &control,
        || run_encode(&sources, &types, &control).map(|_| ()),
        true,
    );
    prefixes(
        &control,
        || run_decode(encoded.as_wire(), &read, &control).map(|_| ()),
        true,
    );
    let bad = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![7],
        metadata: vec![],
    }];
    control.arm(None);
    ordinary(run_decode(&bad, &read, &control).map(|_| ()));
    assert!(control.trace().last().unwrap().1 > 0);
    prefixes(
        &control,
        || run_decode(&bad, &read, &control).map(|_| ()),
        false,
    );
    let mut l = limits();
    l.max_allocation_requests = 0;
    control.arm(Some((1, CompileControlError::DeadlineExceeded)));
    assert!(matches!(
        prepare_schemas_decode(encoded.as_wire(), &read, SOURCE, l, &control),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(control.trace(), [(CompilePhase::Decode, 0)]);
}
#[test]
fn known_metadata_prefix_resource_refusal_precedes_late_control() {
    let control = Control::default();
    let roots = [];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let schema =
        Schema::new_with_metadata(Vec::<Arc<Field>>::new(), [("x".into(), "".into())].into());
    let sources = [SchemaSource {
        id: 0,
        source: &schema,
        field_ids: &[],
    }];
    // The original container requests fit exactly; the first actual key adds
    // one known byte. No subsequent iterator/footer observation may win.
    let mut send = limits();
    send.max_allocation_request_bytes = Layout::array::<usize>(1).unwrap().size()
        + Layout::array::<wire::SchemaDefinition>(1).unwrap().size()
        + Layout::array::<(&str, &str)>(1).unwrap().size()
        + Layout::array::<ArrowFieldMetadataEntry>(1).unwrap().size();
    let empty_key_schema =
        Schema::new_with_metadata(Vec::<Arc<Field>>::new(), [("".into(), "".into())].into());
    let empty_key_sources = [SchemaSource {
        id: 0,
        source: &empty_key_schema,
        field_ids: &[],
    }];
    let mut send_work = limits();
    send_work.max_work = run_encode(&empty_key_sources, &types, &control)
        .unwrap()
        .facts()
        .cumulative_work_upper_bound;
    control.arm(Some((5, CompileControlError::Cancelled)));
    let late = prepare_schemas_encode(&sources, &types, SOURCE, send, &control);
    assert!(
        matches!(
            late,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ),
        "late control replaced known resource: {:?}",
        late.err()
    );
    control.arm(None);
    assert!(matches!(
        prepare_schemas_encode(&sources, &types, SOURCE, send, &control),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    let prefix = control.trace();
    assert_eq!(
        prefix.len(),
        5,
        "no callback after the first key's known refusal"
    );
    for bound in [send, send_work] {
        for cause in CAUSES {
            control.arm(Some((prefix.len(), cause)));
            assert!(matches!(
                prepare_schemas_encode(&sources, &types, SOURCE, bound, &control),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), prefix);
        }
    }
    let input = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![],
        metadata: vec![ArrowFieldMetadataEntry {
            key: "x".into(),
            value: "".into(),
        }],
    }];
    let mut receive = limits();
    receive.max_allocation_request_bytes = Layout::array::<usize>(1).unwrap().size()
        + 2 * Layout::array::<(u32, Schema)>(1).unwrap().size()
        + Layout::new::<[usize; 2]>().size()
        + maps::fresh_table_layout::<String, String>(1)
            .unwrap()
            .layout
            .unwrap()
            .size();
    control.arm(None);
    let empty_key_input = [wire::SchemaDefinition {
        id: 0,
        field_ids: vec![],
        metadata: vec![ArrowFieldMetadataEntry {
            key: "".into(),
            value: "".into(),
        }],
    }];
    let mut receive_work = limits();
    receive_work.max_work = run_decode(&empty_key_input, &read, &control)
        .unwrap()
        .facts()
        .cumulative_work_upper_bound;
    // Exercise the sole count author in an already active caller scope, with
    // a real pending quantum: its three preceding header gates leave 255.
    for bound in [receive, receive_work] {
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..240 {
                work.step().unwrap();
            }
            assert!(matches!(
                decode_preflight(&input, &read, SOURCE, bound, &mut work),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [(CompilePhase::Decode, 0)]);
        }
    }
    control.arm(None);
}
#[test]
fn wide_schema_copy_has_actual_quantum_without_claiming_hashmap_internal_sampling() {
    let control = Control::default();
    let original = field();
    let roots = [(u32::MAX, original.clone())];
    let types = encode_type_table_sources(&[], &roots, type_limits(), &control).unwrap();
    let read = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let text = "雪".repeat(320);
    let schema =
        Schema::new_with_metadata(vec![original; 320], [("key".into(), text.clone())].into());
    let ids = vec![u32::MAX; 320];
    let sources = [SchemaSource {
        id: u32::MAX,
        source: &schema,
        field_ids: &ids,
    }];
    control.arm(None);
    let encoded = run_encode(&sources, &types, &control).unwrap();
    let sent = control.trace();
    assert!(sent.iter().any(|(_, units)| *units == 256));
    assert_eq!(encoded.as_wire()[0].field_ids, ids);
    assert_eq!(encoded.as_wire()[0].metadata[0].value, text);
    control.arm(None);
    let decoded = run_decode(encoded.as_wire(), &read, &control).unwrap();
    let received = control.trace();
    assert!(received.iter().any(|(_, units)| *units == 256));
    assert_eq!(decoded.definitions()[0].1.fields().len(), 320);
    for receive in [false, true] {
        let trace = if receive { &received } else { &sent };
        let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
        for at in [0, quantum, trace.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause)));
                let result = if receive {
                    run_decode(encoded.as_wire(), &read, &control).map(|_| ())
                } else {
                    run_encode(&sources, &types, &control).map(|_| ())
                };
                assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}
