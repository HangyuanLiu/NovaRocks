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
//! Independent ORIGINAL ARRAY_REPEAT eval, Arrow coercion and copy baselines.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::NullBuffer;
use novarocks_types::SlotId;
use std::sync::Arc;
fn raw(columns: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let slots = (0..columns.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let ids = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let id = match output {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::array::eval_array_repeat(&arena, id, &ids, &chunk)
}

fn ints(out: &ArrayRef) -> Vec<Option<i32>> {
    out.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn original_repeat_positive_nonpositive_count_null_and_source_null() {
    let source: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        None,
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(2),
        Some(1),
        Some(0),
        None,
        Some(-1),
        Some(i64::MIN),
    ]));
    let out = raw(vec![source, count], None).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 3, 3, 3, 3, 3]);
    assert_eq!(ints(&out), vec![Some(1), Some(1), None]);
    assert!(list.is_null(3));
    assert_eq!(list.null_count(), 1);
}
#[test]
fn original_repeat_default_arrow_count_cast_keeps_null_and_float_truncation() {
    let s: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let c: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2"),
        Some("invalid"),
        None,
        Some("-1"),
    ]));
    let out = raw(vec![s.clone(), c], None).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 2, 2, 2, 2]);
    assert!(l.is_null(1));
    assert!(l.is_null(2));
    assert_eq!(ints(&out), vec![Some(1), Some(1)]);
    let c: ArrayRef = Arc::new(Float64Array::from(vec![2.9, f64::NAN, f64::INFINITY, -1.9]));
    let out = raw(vec![s, c], None).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 2, 2, 2, 2]);
    assert_eq!(l.null_count(), 2);
}
#[test]
fn original_repeat_source_cast_null_expansion_and_explicit_field_metadata() {
    let field = Arc::new(
        Field::new("authored", DataType::Int64, true)
            .with_metadata([("frozen".into(), "identity".into())].into()),
    );
    let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
    let out = raw(
        vec![source, count.clone()],
        Some(DataType::List(field.clone())),
    )
    .unwrap();
    assert_eq!(out.data_type(), &DataType::List(field.clone()));
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        l.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7), None]
    );
    let out = raw(
        vec![new_null_array(&DataType::Null, 2), count],
        Some(DataType::List(field)),
    )
    .unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.null_count(), 0);
    assert_eq!(l.values().null_count(), 3);
}
#[test]
fn original_repeat_missing_or_nonlist_output_uses_original_source_field() {
    let source: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let count: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let a = raw(vec![source.clone(), count.clone()], None).unwrap();
    let b = raw(vec![source, count], Some(DataType::Utf8)).unwrap();
    assert_eq!(a.to_data(), b.to_data());
    assert_eq!(
        a.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
    );
}
#[test]
fn original_repeat_nested_nulls_and_slice_values_are_retained() {
    let fs = vec![Arc::new(
        Field::new("logical-child", DataType::Int32, true)
            .with_metadata([("owner".into(), "nested".into())].into()),
    )];
    let source: ArrayRef = Arc::new(StructArray::new(
        fs.clone().into(),
        vec![Arc::new(Int32Array::from(vec![Some(10), None, Some(30)]))],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let out = raw(
        vec![source.slice(1, 2), Arc::new(Int64Array::from(vec![2, 1]))],
        None,
    )
    .unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 2, 3]);
    let values = l.values().as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(values.data_type(), &DataType::Struct(fs.into()));
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(
        values
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(2),
        30
    );
    let out = raw(
        vec![
            new_empty_array(&DataType::Int32),
            new_empty_array(&DataType::Int64),
        ],
        None,
    )
    .unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0]
    );
}
#[test]
fn original_repeat_full_arrow_count_and_source_cast_errors_are_retained() {
    let s: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let bad: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x", DataType::Int32, false))].into(),
        vec![s.clone()],
        None,
    ));
    let expected = format!(
        "array_repeat failed to cast repeat count {:?} -> BIGINT: {}",
        bad.data_type(),
        arrow::compute::cast(&bad, &DataType::Int64).unwrap_err()
    );
    assert_eq!(raw(vec![s, bad.clone()], None).unwrap_err(), expected);
    let count: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let expected = format!(
        "array_repeat failed to cast source type {:?} -> {:?}: {}",
        bad.data_type(),
        DataType::Int64,
        arrow::compute::cast(&bad, &DataType::Int64).unwrap_err()
    );
    assert_eq!(
        raw(
            vec![bad, count],
            Some(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int64,
                true
            ))))
        )
        .unwrap_err(),
        expected
    );
}
