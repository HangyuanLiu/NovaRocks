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
//! Original MAP_KEYS/MAP_VALUES raw authors: sorting, data failures and field projection.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, ListArray, MapArray, StringArray, StructArray,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;
#[derive(Clone, Copy)]
enum Part {
    Keys,
    Values,
}
fn name(p: Part) -> &'static str {
    match p {
        Part::Keys => "map_keys",
        Part::Values => "map_values",
    }
}
fn setup(a: ArrayRef) -> (ExprArena, Vec<ExprId>, Chunk) {
    let mut arena = ExprArena::default();
    let slot = SlotId::new(1);
    let id = arena.push_typed(ExprNode::SlotId(slot), a.data_type().clone());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            a.data_type().clone(),
            true,
        )])),
        vec![a],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    (arena, vec![id], Chunk::new_with_chunk_schema(batch, cs))
}
fn call(
    p: Part,
    arena: &ExprArena,
    id: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    match p {
        Part::Keys => super::function::map::eval_map_keys(arena, id, args, chunk),
        Part::Values => super::function::map::eval_map_values(arena, id, args, chunk),
    }
}
fn raw(p: Part, a: ArrayRef, output: Option<DataType>) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(a);
    let id = output
        .map(|t| arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t))
        .unwrap_or(ExprId(usize::MAX));
    call(p, &arena, id, &args, &chunk)
}
fn map(
    keys: ArrayRef,
    values: ArrayRef,
    offsets: Vec<i32>,
    valid: Option<Vec<bool>>,
    sorted: bool,
) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(
                Field::new("source-key", keys.data_type().clone(), false)
                    .with_metadata([("key-fact".into(), "preserved".into())].into()),
            ),
            Arc::new(
                Field::new("source-value", values.data_type().clone(), true)
                    .with_metadata([("value-fact".into(), "preserved".into())].into()),
            ),
        ]
        .into(),
        vec![keys, values],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new(
            "source-entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(offsets.into()),
        entries,
        valid.map(NullBuffer::from),
        sorted,
    ))
}
fn list(a: &ArrayRef) -> &ListArray {
    a.as_any().downcast_ref().unwrap()
}
#[test]
fn original_map_parts_stable_key_sort_always_applies_even_when_sorted_fact_true() {
    for sorted in [false, true] {
        let a = map(
            Arc::new(Int32Array::from(vec![3, 1, 2, 1])),
            Arc::new(StringArray::from(vec!["three", "one-a", "two", "one-b"])),
            vec![0, 4],
            None,
            sorted,
        );
        let k = raw(Part::Keys, a.clone(), None).unwrap();
        assert_eq!(
            list(&k)
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 1, 2, 3]
        );
        let v = raw(Part::Values, a, None).unwrap();
        assert_eq!(
            list(&v)
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("one-a"), Some("one-b"), Some("two"), Some("three")]
        );
    }
}
#[test]
fn original_map_parts_null_parent_empty_slice_and_child_nulls() {
    let a = map(
        Arc::new(Int32Array::from(vec![2, 1, 4, 3])),
        Arc::new(StringArray::from(vec![
            None,
            Some("one"),
            Some("four"),
            None,
        ])),
        vec![0, 2, 2, 4],
        Some(vec![true, false, true]),
        false,
    );
    for p in [Part::Keys, Part::Values] {
        let out = raw(p, a.clone(), None).unwrap();
        assert_eq!(list(&out).value_offsets(), &[0, 2, 2, 4]);
        assert!(out.is_null(1));
        let s = raw(p, a.slice(1, 2), None).unwrap();
        assert_eq!(list(&s).value_offsets(), &[0, 0, 2]);
        assert!(s.is_null(0));
        assert!(raw(p, a.slice(0, 0), None).unwrap().is_empty());
    }
}
#[test]
fn original_map_parts_unsupported_key_compare_fails_only_when_comparison_needed() {
    for p in [Part::Keys, Part::Values] {
        let a = map(
            Arc::new(UInt32Array::from(vec![2, 1])),
            Arc::new(Int32Array::from(vec![20, 10])),
            vec![0, 2],
            None,
            false,
        );
        assert_eq!(
            raw(p, a.clone(), None).unwrap_err(),
            "map key ordered compare unsupported type: UInt32"
        );
        let singles = map(
            Arc::new(UInt32Array::from(vec![2, 1])),
            Arc::new(Int32Array::from(vec![20, 10])),
            vec![0, 1, 2],
            None,
            false,
        );
        assert!(raw(p, singles, None).is_ok());
        let null = map(
            Arc::new(UInt32Array::from(vec![2, 1])),
            Arc::new(Int32Array::from(vec![20, 10])),
            vec![0, 2],
            Some(vec![false]),
            false,
        );
        assert!(raw(p, null, None).is_ok());
    }
}
#[test]
fn original_map_parts_planned_output_field_metadata_is_exact_and_raw_fallback_is_original() {
    let a = map(
        Arc::new(Int32Array::from(vec![2, 1])),
        Arc::new(Int32Array::from(vec![20, 10])),
        vec![0, 2],
        None,
        false,
    );
    for p in [Part::Keys, Part::Values] {
        let f = Arc::new(
            Field::new("planned-item", DataType::Int32, true)
                .with_metadata([("planned-fact".into(), "preserved".into())].into()),
        );
        let out = raw(p, a.clone(), Some(DataType::List(f.clone()))).unwrap();
        assert_eq!(out.data_type(), &DataType::List(f));
        let fallback = raw(p, a.clone(), None).unwrap();
        assert_eq!(
            fallback.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }
}
#[test]
fn original_map_parts_nan_and_signed_zero_keep_insertion_sort_original_ties() {
    let a = map(
        Arc::new(Float64Array::from(vec![f64::NAN, 2.0, -0.0, 0.0])),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
        vec![0, 4],
        None,
        false,
    );
    let out = raw(Part::Values, a.clone(), None).unwrap();
    assert_eq!(
        list(&out)
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 3, 4, 2]
    );
    let out = raw(Part::Keys, a, None).unwrap();
    let vals = list(&out)
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(vals.value(0).to_bits(), f64::NAN.to_bits());
    assert_eq!(vals.value(1).to_bits(), (-0.0f64).to_bits());
    assert_eq!(vals.value(2).to_bits(), 0.0f64.to_bits());
}
#[test]
fn original_map_parts_wrong_carrier_output_error_precedence_and_unused_arguments() {
    for p in [Part::Keys, Part::Values] {
        assert_eq!(
            raw(p, Arc::new(Int32Array::from(vec![1])), None).unwrap_err(),
            format!("{} expects MapArray, got Int32", name(p))
        );
        let a = map(
            Arc::new(UInt32Array::from(vec![2, 1])),
            Arc::new(Int32Array::from(vec![2, 1])),
            vec![0, 2],
            None,
            false,
        );
        assert_eq!(
            raw(p, a, Some(DataType::Int32)).unwrap_err(),
            format!("{} output type must be List, got Int32", name(p))
        );
        let a = map(
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            vec![0, 1],
            None,
            false,
        );
        let (mut arena, mut args, chunk) = setup(a);
        let bad = arena.push_typed(ExprNode::SlotId(SlotId::new(987)), DataType::Int32);
        args.push(bad);
        assert!(call(p, &arena, ExprId(usize::MAX), &args, &chunk).is_ok());
        assert_eq!(
            call(p, &arena, ExprId(usize::MAX), &[bad], &chunk).unwrap_err(),
            arena.eval(bad, &chunk).unwrap_err()
        );
    }
}
#[test]
fn original_map_parts_empty_arity_preserves_exact_index_panic() {
    for p in [Part::Keys, Part::Values] {
        let (arena, _, chunk) = setup(Arc::new(Int32Array::from(vec![0])));
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            call(p, &arena, ExprId(usize::MAX), &[], &chunk)
        }))
        .unwrap_err();
        let text = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap();
        assert_eq!(text, "index out of bounds: the len is 0 but the index is 0");
    }
}
