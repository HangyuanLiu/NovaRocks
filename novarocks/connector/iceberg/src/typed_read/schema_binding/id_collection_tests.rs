// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::field_domain::{FieldDomain, FieldDomains};
use crate::iceberg::spec::{ListType, StructType};
use arrow::array::{Int8Array, Int32Array, LargeListArray, ListArray, MapArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Fields;

fn identified(name: &str, id: i32, data_type: DataType, nullable: bool) -> Arc<Field> {
    Arc::new(
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )])),
    )
}

#[test]
fn collection_id_projection_list_variants_preserve_sliced_hidden_domain_mask() {
    // Equal child types make ordinal projection silently wrong rather than
    // producing a type mismatch. Each existing offset-width variant must use semantic IDs.
    let old_children: Fields = vec![
        identified("old_b", 12, DataType::Int32, true),
        identified("old_a", 11, DataType::Int32, true),
    ]
    .into();
    let new_children: Fields = vec![
        identified("new_a", 11, DataType::Int32, true),
        identified("new_b", 12, DataType::Int32, true),
    ]
    .into();
    let old_element = identified("element", 10, DataType::Struct(old_children.clone()), true);
    let new_element = identified("element", 10, DataType::Struct(new_children), true);
    let values: ArrayRef = Arc::new(StructArray::new(
        old_children,
        vec![
            Arc::new(Int32Array::from(vec![999, 999, 999, 9])),
            Arc::new(Int32Array::from(vec![999, 999, 999, 7])),
        ],
        None,
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        old_element.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 3, 4])),
        values.clone(),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let large_list: ArrayRef = Arc::new(LargeListArray::new(
        old_element,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 3, 4])),
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let storage = NestedField::optional(
        1,
        "items",
        Type::List(ListType::new(Arc::new(NestedField::list_element(
            10,
            Type::Struct(StructType::new(vec![
                Arc::new(NestedField::optional(
                    11,
                    "new_a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "new_b",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])),
            false,
        )))),
    );
    let domains = FieldDomains::from([(11, FieldDomain::Int8), (12, FieldDomain::Int8)]);
    for (source, target_type) in [
        (list, DataType::List(new_element.clone())),
        (large_list, DataType::LargeList(new_element)),
    ] {
        let source_field = identified("old_items", 1, source.data_type().clone(), true);
        let target = identified("items", 1, target_type, true);
        let slice = source.slice(1, 2);
        let projected = project_physical_array(
            &slice,
            &source_field,
            &target,
            &[true, true],
            "collection.parquet",
        )
        .expect("same-variant slice must retain semantic children");
        assert!(projected.is_null(0));
        let logical = crate::field_domain::apply_field(&target, &storage, &domains).unwrap();
        let restored = crate::field_domain::restore_array(&projected, &storage, &logical, &domains)
            .expect("overflow in excluded or null-parent slots is invisible");
        let child_values = match restored.data_type() {
            DataType::List(_) => {
                let a = restored.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(a.value_offsets(), &[1, 3, 4]);
                a.values()
            }
            DataType::LargeList(_) => {
                let a = restored.as_any().downcast_ref::<LargeListArray>().unwrap();
                assert_eq!(a.value_offsets(), &[1_i64, 3, 4]);
                a.values()
            }
            _ => panic!("offset width must match the target"),
        };
        let children = child_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            children
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(3),
            7
        );
        assert_eq!(
            children
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(3),
            9
        );
        assert_eq!(restored.nulls(), slice.nulls());

        // The same physical overflow must refuse when the collection is visible.
        let first = source.slice(0, 1);
        let first = project_physical_array(
            &first,
            &source_field,
            &target,
            &[true],
            "collection.parquet",
        )
        .unwrap();
        let error = crate::field_domain::restore_array(&first, &storage, &logical, &domains)
            .expect_err("visible out-of-domain INT32 must refuse");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
        assert!(error.to_string().contains("visible INT exceeds"));
    }
}

#[test]
fn collection_id_projection_map_reorders_nested_members_and_keeps_nullable_query_keys() {
    let key_old: Fields = vec![
        identified("old_kb", 22, DataType::Int32, true),
        identified("old_ka", 21, DataType::Int32, true),
    ]
    .into();
    let value_old: Fields = vec![
        identified("old_vb", 32, DataType::Int32, true),
        identified("old_va", 31, DataType::Int32, true),
    ]
    .into();
    let from_children: Fields = vec![
        identified("key", 2, DataType::Struct(key_old.clone()), true),
        identified("value", 3, DataType::Struct(value_old.clone()), true),
    ]
    .into();
    let to_children: Fields = vec![
        identified(
            "key",
            2,
            DataType::Struct(
                vec![
                    identified("ka", 21, DataType::Int32, true),
                    identified("kb", 22, DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ),
        identified(
            "value",
            3,
            DataType::Struct(
                vec![
                    identified("va", 31, DataType::Int32, true),
                    identified("vb", 32, DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        ),
    ]
    .into();
    // The entries wrapper has no semantic ID. A NULL key remains a legal
    // query carrier; the write owner's rejection must not leak into reads.
    let from_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(from_children.clone()),
        false,
    ));
    let to_entries = Arc::new(Field::new("entries", DataType::Struct(to_children), false));
    let keys: ArrayRef = Arc::new(StructArray::new(
        key_old,
        vec![
            Arc::new(Int32Array::from(vec![200, 202, 204])),
            Arc::new(Int32Array::from(vec![100, 102, 104])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let values: ArrayRef = Arc::new(StructArray::new(
        value_old,
        vec![
            Arc::new(Int32Array::from(vec![400, 402, 404])),
            Arc::new(Int32Array::from(vec![300, 302, 304])),
        ],
        None,
    ));
    let entries = StructArray::new(from_children, vec![keys, values], None);
    let source: ArrayRef = Arc::new(MapArray::new(
        from_entries,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 3])),
        entries,
        None,
        false,
    ));
    let from = identified("old_map", 1, source.data_type().clone(), true);
    let target = identified("map", 1, DataType::Map(to_entries, false), true);
    let slice = source.slice(1, 1);
    let projected = project_physical_array(&slice, &from, &target, &[true], "map.parquet")
        .expect("nested key/value members must project by ID");
    let map = projected.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[1, 3]);
    assert!(map.keys().is_null(1));
    let keys = map.keys().as_any().downcast_ref::<StructArray>().unwrap();
    let values = map.values().as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        keys.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(2),
        104
    );
    assert_eq!(
        keys.column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(2),
        204
    );
    assert_eq!(
        values
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        302
    );
    assert_eq!(
        values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        402
    );
    assert_eq!(projected.nulls(), slice.nulls());
}

#[test]
fn collection_id_projection_rejects_same_type_list_member_with_wrong_identity() {
    let from_element = identified("element", 2, DataType::Int32, true);
    let to_element = identified("element", 3, DataType::Int32, true);
    let source: ArrayRef = Arc::new(ListArray::new(
        from_element,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Arc::new(Int32Array::from(vec![7])),
        None,
    ));
    let from = identified("items", 1, source.data_type().clone(), true);
    let to = identified("items", 1, DataType::List(to_element), true);
    let error = project_physical_array(&source, &from, &to, &[true], "wrong-member.parquet")
        .expect_err("same carrier does not establish collection member identity");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
    assert!(error.to_string().contains("different field ID"));

    let same_element = match source.data_type() {
        DataType::List(element) => element.clone(),
        _ => unreachable!(),
    };
    let cross = identified("items", 1, DataType::LargeList(same_element), true);
    let error = project_physical_array(&source, &from, &cross, &[true], "wrong-width.parquet")
        .expect_err("ID reconstruction must not add cross-offset representation support");
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );
}
