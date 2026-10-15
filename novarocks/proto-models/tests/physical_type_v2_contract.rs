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

use std::collections::BTreeSet;

use novarocks_proto_models::{FILE_DESCRIPTOR_SET, physical_type_v2 as v2, plan};
use prost::Message;
use prost_reflect::{DescriptorPool, Kind, MessageDescriptor};

#[test]
fn reachable_type_messages_have_no_recursive_definition_embedding() {
    fn walk(message: MessageDescriptor, path: &mut BTreeSet<String>) -> usize {
        assert!(
            path.insert(message.full_name().to_owned()),
            "recursive type message: {}",
            message.full_name()
        );
        let mut depth = 1;
        for field in message.fields() {
            if let Kind::Message(child) = field.kind() {
                assert!(!child.full_name().ends_with(".ArrowPhysicalType"));
                assert!(!child.full_name().ends_with(".ArrowPhysicalField"));
                assert!(!child.full_name().ends_with(".TypeDesc"));
                depth = depth.max(1 + walk(child, path));
            }
        }
        path.remove(message.full_name());
        depth
    }
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET).unwrap();
    let root = pool
        .get_message_by_name("novarocks.physical_type_v2.TypeTable")
        .unwrap();
    // This bounds protobuf representation depth, not semantic type depth or
    // decoded allocation. Semantic nesting remains a graph of sparse IDs.
    // The complete semantic namespace adds the fixed Union field envelope:
    // TypeTable -> LogicalTypeDefinition -> UnionValue -> UnionValueField
    // -> NamedValueField. Its child types remain sparse IDs, never embedded.
    let logical = pool
        .get_message_by_name("novarocks.value_type.LogicalTypeDefinition")
        .unwrap();
    assert_eq!(walk(logical, &mut BTreeSet::new()), 4);
    assert_eq!(walk(root, &mut BTreeSet::new()), 5);
    let original = pool
        .get_message_by_name("novarocks.arrow_type.ArrowPhysicalType")
        .unwrap();
    let flat = pool
        .get_message_by_name("novarocks.physical_type_v2.CarrierTypeDefinition")
        .unwrap();
    let kinds = |message: &MessageDescriptor| {
        message
            .oneofs()
            .find(|oneof| oneof.name() == "kind")
            .unwrap()
            .fields()
            .count()
    };
    assert_eq!(kinds(&original), kinds(&flat));
}

#[test]
fn zero_sparse_references_have_presence_and_are_distinct_from_missing() {
    let present = v2::FieldDefinition {
        id: u32::MAX,
        name: "nested".into(),
        carrier_type_id: Some(0),
        ..Default::default()
    };
    let mut absent = present.clone();
    absent.carrier_type_id = None;
    assert_ne!(present.encode_to_vec(), absent.encode_to_vec());
    assert_eq!(
        v2::FieldDefinition::decode(present.encode_to_vec().as_slice()).unwrap(),
        present
    );
    assert_eq!(
        v2::FieldDefinition::decode(absent.encode_to_vec().as_slice()).unwrap(),
        absent
    );
    let source_present = v2::SourceArrowFieldFacts {
        field_id: Some(0),
        metadata: vec![],
        dictionary_id: Some(0),
        dictionary_is_ordered: Some(false),
    };
    let mut source_absent = source_present.clone();
    source_absent.dictionary_id = None;
    source_absent.dictionary_is_ordered = None;
    assert_ne!(
        source_present.encode_to_vec(),
        source_absent.encode_to_vec()
    );
    assert_eq!(
        v2::SourceArrowFieldFacts::decode(source_present.encode_to_vec().as_slice()).unwrap(),
        source_present
    );
    // A oneof scalar also preserves a present zero field reference.
    let list = v2::CarrierTypeDefinition {
        id: 0,
        kind: Some(v2::carrier_type_definition::Kind::ListFieldId(0)),
    };
    assert_eq!(
        v2::CarrierTypeDefinition::decode(list.encode_to_vec().as_slice()).unwrap(),
        list
    );
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET).unwrap();
    for message in pool
        .all_messages()
        .filter(|message| message.package_name() == "novarocks.physical_type_v2")
    {
        for field in message
            .fields()
            .filter(|field| field.name().ends_with("_id") && field.name() != "type_id")
        {
            assert!(field.supports_presence(), "{}", field.full_name());
        }
    }
}

#[test]
fn nested_field_attributes_and_explicit_logical_domains_survive_the_carrier() {
    use v2::carrier_type_definition::Kind;
    let table = v2::TypeTable {
        logical_types: vec![
            novarocks_proto_models::value_type::LogicalTypeDefinition {
                id: Some(0),
                nullable: false,
                kind: Some(
                    novarocks_proto_models::value_type::logical_type_definition::Kind::Scalar(
                        novarocks_proto_models::value_type::ScalarValue {
                            kind: novarocks_proto_models::value_type::ScalarKind::FixedSizeBinary
                                as i32,
                            fixed_binary_width: Some(16),
                            ..Default::default()
                        },
                    ),
                ),
            },
            novarocks_proto_models::value_type::LogicalTypeDefinition {
                id: Some(1),
                nullable: false,
                kind: Some(
                    novarocks_proto_models::value_type::logical_type_definition::Kind::Nominal(
                        novarocks_proto_models::value_type::NominalValue::LargeInt as i32,
                    ),
                ),
            },
            novarocks_proto_models::value_type::LogicalTypeDefinition {
                id: Some(2),
                nullable: false,
                kind: Some(
                    novarocks_proto_models::value_type::logical_type_definition::Kind::Nominal(
                        novarocks_proto_models::value_type::NominalValue::Uuid as i32,
                    ),
                ),
            },
        ],
        carriers: vec![
            v2::CarrierTypeDefinition {
                id: 0,
                kind: Some(Kind::FixedSizeBinary(16)),
            },
            v2::CarrierTypeDefinition {
                id: u32::MAX,
                kind: Some(Kind::StructType(v2::StructFields {
                    field_ids: vec![0, 0],
                })),
            },
            v2::CarrierTypeDefinition {
                id: 9,
                kind: Some(Kind::Timestamp(plan::ArrowTimestampType {
                    unit: plan::ArrowTimeUnit::Nanosecond as i32,
                    timezone: Some("UTC".into()),
                })),
            },
            v2::CarrierTypeDefinition {
                id: 10,
                kind: Some(Kind::Decimal256(plan::ArrowDecimalType {
                    precision: 76,
                    scale: -3,
                })),
            },
            v2::CarrierTypeDefinition {
                id: 12,
                kind: Some(Kind::Primitive(plan::ArrowPrimitiveType::Int32 as i32)),
            },
            v2::CarrierTypeDefinition {
                id: 11,
                kind: Some(Kind::Dictionary(v2::DictionaryTypes {
                    key_type_id: Some(12),
                    value_type_id: Some(u32::MAX),
                })),
            },
        ],
        fields: vec![v2::FieldDefinition {
            value_binding_id: None,
            id: 0,
            name: "item".into(),
            nullable: true,
            carrier_type_id: Some(0),

            logical_type_id: None,
        }],
        value_types: (0..3)
            .map(|id| v2::ValueTypeDefinition {
                id,
                carrier_type_id: Some(0),
                logical_type_id: Some(id),
            })
            .collect(),
    };
    // This is wire representation coverage, not application semantic admission
    // of this table. No type is inferred from the fixed binary width.
    assert_eq!(
        v2::TypeTable::decode(table.encode_to_vec().as_slice()).unwrap(),
        table
    );
    let source = v2::SourceFieldTable {
        shape: Some(v2::TypeTable {
            value_types: vec![],
            logical_types: vec![],
            ..table.clone()
        }),
        field_facts: vec![v2::SourceArrowFieldFacts {
            field_id: Some(0),
            metadata: vec![plan::ArrowFieldMetadataEntry {
                key: "nr.logical_type".into(),
                value: "largeint".into(),
            }],
            dictionary_id: Some(-17),
            dictionary_is_ordered: Some(true),
        }],
    };
    assert_eq!(
        v2::SourceFieldTable::decode(source.encode_to_vec().as_slice()).unwrap(),
        source
    );
    use novarocks_proto_models::value_type::{
        NominalValue, ScalarKind, logical_type_definition::Kind as L,
    };
    let logical: Vec<_> = table
        .value_types
        .iter()
        .map(|binding| {
            let logical = table
                .logical_types
                .iter()
                .find(|logical| logical.id == binding.logical_type_id)
                .unwrap();
            assert!(!logical.nullable);
            logical.kind.as_ref().unwrap()
        })
        .collect();
    assert_eq!(logical.len(), 3);
    assert!(
        matches!(logical[0],L::Scalar(value) if value.kind==ScalarKind::FixedSizeBinary as i32 && value.fixed_binary_width==Some(16))
    );
    assert_eq!(logical[1], &L::Nominal(NominalValue::LargeInt as i32));
    assert_eq!(logical[2], &L::Nominal(NominalValue::Uuid as i32));
    assert_ne!(logical[1], logical[2]);
}

#[test]
fn execution_shape_retires_source_attributes_with_reserved_wire_numbers_and_names() {
    let pool = DescriptorPool::decode(FILE_DESCRIPTOR_SET).unwrap();
    for (message, retired) in [
        (
            "novarocks.physical_type_v2.FieldDefinition",
            &[
                (5, "metadata"),
                (6, "dictionary_id"),
                (7, "dictionary_is_ordered"),
            ][..],
        ),
        (
            "novarocks.physical_type_v2.ValueTypeDefinition",
            &[(3, "nullable"), (4, "logical_type")][..],
        ),
        (
            "novarocks.physical_package_v2.SchemaDefinition",
            &[(3, "metadata")][..],
        ),
    ] {
        let descriptor = pool.get_message_by_name(message).unwrap();
        for (number, name) in retired {
            assert!(descriptor.get_field(*number).is_none());
            assert!(
                descriptor
                    .reserved_ranges()
                    .any(|range| range.contains(number))
            );
            assert!(
                descriptor
                    .reserved_names()
                    .any(|reserved| reserved == *name)
            );
        }
    }
}
