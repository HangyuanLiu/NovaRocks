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
//! Independent original CARDINALITY raw contracts, before shared computation extraction.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, NullArray, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(array: ArrayRef) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slot = SlotId::new(1);
    let mut arena = ExprArena::default();
    let id = arena.push_typed(ExprNode::SlotId(slot), array.data_type().clone());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "source",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    (arena, vec![id], Chunk::new_with_chunk_schema(batch, cs))
}
fn raw(array: ArrayRef, output: Option<DataType>) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = setup(array);
    let id = output
        .map(|t| arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t))
        .unwrap_or(ExprId(usize::MAX));
    super::function::map::eval_cardinality(&arena, id, &args, &chunk)
}
fn fixture(value: ArrayRef, nullable: bool, sorted: bool) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("authored-key", DataType::Int32, false)),
            Arc::new(
                Field::new("authored-value", value.data_type().clone(), true)
                    .with_metadata([("value-fact".into(), "preserved".into())].into()),
            ),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])), value],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(
            Field::new("authored-entries", entries.data_type().clone(), false)
                .with_metadata([("entry-fact".into(), "preserved".into())].into()),
        ),
        OffsetBuffer::new(vec![0, 2, 2, 4, 5].into()),
        entries,
        nullable.then(|| NullBuffer::from(vec![true, true, false, true])),
        sorted,
    ))
}
fn nums() -> ArrayRef {
    Arc::new(Int32Array::from(vec![None, Some(7), None, Some(9), None]))
}
fn counts(out: &ArrayRef) -> Vec<Option<i32>> {
    out.as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn original_cardinality_counts_root_offsets_preserves_nulls_metadata_and_sorted() {
    for nullable in [false, true] {
        for sorted in [false, true] {
            let a = fixture(nums(), nullable, sorted);
            assert_eq!(
                counts(&raw(a, None).unwrap()),
                if nullable {
                    vec![Some(2), Some(0), None, Some(1)]
                } else {
                    vec![Some(2), Some(0), Some(2), Some(1)]
                }
            );
        }
    }
}
#[test]
fn original_cardinality_nested_and_null_children_do_not_affect_count() {
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 1, 2, 3, 4, 5].into()),
        nums(),
        Some(NullBuffer::from(vec![true, false, true, false, true])),
    ));
    for v in [list, Arc::new(NullArray::new(5)) as ArrayRef] {
        assert_eq!(
            counts(&raw(fixture(v, false, false), None).unwrap()),
            vec![Some(2), Some(0), Some(2), Some(1)]
        );
    }
}
#[test]
fn original_cardinality_slices_empty_and_all_null_parents() {
    let a = fixture(nums(), true, true);
    assert_eq!(
        counts(&raw(a.slice(1, 3), None).unwrap()),
        vec![Some(0), None, Some(1)]
    );
    assert!(raw(a.slice(0, 0), None).unwrap().is_empty());
    let map = a.as_any().downcast_ref::<MapArray>().unwrap();
    let all: ArrayRef = Arc::new(MapArray::new(
        match a.data_type() {
            DataType::Map(f, _) => f.clone(),
            _ => unreachable!(),
        },
        map.offsets().clone(),
        map.entries().clone(),
        Some(NullBuffer::from(vec![false; 4])),
        true,
    ));
    assert_eq!(counts(&raw(all, None).unwrap()), vec![None; 4]);
}
#[test]
fn original_cardinality_exact_wrong_carrier_and_output_cast_errors() {
    assert_eq!(
        raw(Arc::new(Int32Array::from(vec![1])), None).unwrap_err(),
        "cardinality expects ARRAY or MAP, got Int32"
    );
    let a = fixture(nums(), true, false);
    let cast = raw(a.clone(), Some(DataType::Int64)).unwrap();
    assert_eq!(
        cast.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2), Some(0), None, Some(1)]
    );
    let target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Int32, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let original = Arc::new(Int32Array::from(vec![Some(2), Some(0), None, Some(1)])) as ArrayRef;
    let cause = arrow::compute::cast(&original, &target)
        .unwrap_err()
        .to_string();
    assert_eq!(
        raw(a, Some(target)).unwrap_err(),
        format!("cardinality: failed to cast output: {cause}")
    );
}
#[test]
fn original_cardinality_only_first_argument_is_evaluated_and_error_is_unwrapped() {
    let (mut arena, mut args, chunk) = setup(fixture(nums(), true, false));
    let invalid = arena.push_typed(ExprNode::SlotId(SlotId::new(987)), DataType::Int32);
    args.push(invalid);
    assert_eq!(
        counts(
            &super::function::map::eval_cardinality(&arena, ExprId(usize::MAX), &args, &chunk)
                .unwrap()
        ),
        vec![Some(2), Some(0), None, Some(1)]
    );
    let expected = arena.eval(invalid, &chunk).unwrap_err();
    assert_eq!(
        super::function::map::eval_cardinality(&arena, ExprId(usize::MAX), &[invalid], &chunk)
            .unwrap_err(),
        expected
    );
}
#[test]
fn original_cardinality_zero_arguments_retains_original_index_panic() {
    let (arena, _, chunk) = setup(Arc::new(StringArray::from(vec!["source"])));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::function::map::eval_cardinality(&arena, ExprId(usize::MAX), &[], &chunk)
    }))
    .unwrap_err();
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap();
    assert_eq!(text, "index out of bounds: the len is 0 but the index is 0");
}

#[test]
fn original_cardinality_list_offsets_ignore_full_nested_child_and_metadata() {
    let child = fixture(nums(), true, true);
    let field = Arc::new(
        Field::new("authored-map-item", child.data_type().clone(), true)
            .with_metadata([("array-fact".into(), "retained".into())].into()),
    );
    let a: ArrayRef = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(vec![0_i32, 2, 2, 4].into()),
        child,
        Some(NullBuffer::from(vec![true, true, false])),
    ));
    assert_eq!(
        counts(&raw(a.clone(), None).unwrap()),
        vec![Some(2), Some(0), None]
    );
    assert_eq!(
        counts(&raw(a.slice(1, 2), None).unwrap()),
        vec![Some(0), None]
    );
    assert!(raw(a.slice(0, 0), None).unwrap().is_empty());
    let a: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("all-null-item", DataType::Null, true)),
        OffsetBuffer::new(vec![0_i32, 2, 3].into()),
        Arc::new(NullArray::new(3)),
        None,
    ));
    assert_eq!(counts(&raw(a, None).unwrap()), vec![Some(2), Some(1)]);
}
#[test]
fn original_cardinality_noncanonical_large_list_and_null_root_are_raw_errors() {
    let a: ArrayRef = Arc::new(arrow::array::LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0_i64, 1].into()),
        Arc::new(Int32Array::from(vec![1])),
        None,
    ));
    let expected = format!("cardinality expects ARRAY or MAP, got {:?}", a.data_type());
    assert_eq!(raw(a, None).unwrap_err(), expected);
    assert_eq!(
        raw(Arc::new(NullArray::new(1)), None).unwrap_err(),
        "cardinality expects ARRAY or MAP, got Null"
    );
}
