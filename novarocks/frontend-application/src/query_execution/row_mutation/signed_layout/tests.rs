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
use arrow::{
    array::{FixedSizeBinaryArray, Int32Array, Int64Array},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::Schema,
};
fn normalize_one(array: ArrayRef, target: Field) -> Result<RecordBatch, String> {
    let source = Arc::new(Schema::new(vec![Field::new(
        "source",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(source, vec![array]).unwrap();
    super::super::normalize_to_signed_selection(&Arc::new(Schema::new(vec![target])), &batch)
}
#[test]
fn signed_layout_accepts_nested_names_metadata_and_nullability() {
    let child: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("old", DataType::Int32, true)].into(),
        vec![child],
        Some(NullBuffer::from(vec![true, false])),
    ));
    let target = DataType::Struct(
        vec![
            Field::new("new", DataType::Int32, false)
                .with_metadata([("iceberg.id".into(), "5".into())].into()),
        ]
        .into(),
    );
    let out = normalize_one(array, Field::new("root", target.clone(), true)).unwrap();
    assert_eq!(out.column(0).data_type(), &target);
    let structure = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(structure.is_null(1));
    assert_eq!(structure.column(0).null_count(), 0);
    assert_eq!(
        structure
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
}
#[test]
fn signed_layout_required_null_is_rejected_at_root_or_visible_child() {
    assert!(
        normalize_one(
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Field::new("v", DataType::Int32, false)
        )
        .is_err()
    );
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Field::new("f", DataType::Int32, true)].into(),
        vec![Arc::new(Int32Array::from(vec![None::<i32>]))],
        None,
    ));
    let target = DataType::Struct(vec![Field::new("f", DataType::Int32, false)].into());
    assert!(normalize_one(array, Field::new("v", target, true)).is_err());
}
#[test]
fn signed_layout_null_list_parent_masks_required_element_null() {
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("old", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
        Arc::new(Int32Array::from(vec![None, Some(8)])),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
    let out = normalize_one(array, Field::new("v", target, true)).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(list.is_null(0));
    assert_eq!(list.values().null_count(), 0);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(1),
        8
    );
}
#[test]
fn signed_layout_visible_null_list_element_is_rejected() {
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
        Arc::new(Int32Array::from(vec![None::<i32>])),
        None,
    ));
    assert!(
        normalize_one(
            array,
            Field::new(
                "v",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true
            )
        )
        .is_err()
    );
}
#[test]
fn signed_layout_dictionary_expands_exact_keys_and_value_nulls() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), None, Some(0), Some(2)]),
            Arc::new(StringArray::from(vec![Some("abc"), Some("x"), None])),
        )
        .unwrap(),
    );
    assert_eq!(decoded_dictionary_bytes(&dictionary).unwrap(), 4);
    let out = normalize_one(dictionary.clone(), Field::new("v", DataType::Utf8, true)).unwrap();
    let strings = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![Some("x"), None, Some("abc"), None]
    );
    assert!(normalize_one(dictionary, Field::new("v", DataType::Utf8, false)).is_err());
}
#[test]
fn signed_layout_does_not_convert_values_or_fixed_widths() {
    assert!(
        normalize_one(
            Arc::new(Int64Array::from(vec![1])),
            Field::new("v", DataType::Int32, true)
        )
        .is_err()
    );
    let array: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abcd".as_slice()].into_iter()).unwrap());
    assert!(normalize_one(array, Field::new("v", DataType::Binary, true)).is_err());
    let decimal: ArrayRef = Arc::new(
        arrow::array::Decimal128Array::from(vec![123])
            .with_precision_and_scale(4, 2)
            .unwrap(),
    );
    assert!(normalize_one(decimal, Field::new("v", DataType::Decimal128(5, 2), true)).is_err());
}
