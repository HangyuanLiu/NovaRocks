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

//! Original array-element access, independent of any Functions owner.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, StringArray, new_empty_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
fn raw(columns: Vec<ArrayRef>, target: Option<DataType>) -> Result<ArrayRef, String> {
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
    let id = match target {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::array::eval_element_at(&arena, id, &ids, &chunk)
}
fn ints(out: &ArrayRef) -> Vec<Option<i32>> {
    out.as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn original_array_access_one_based_nonpositive_oob_parent_null_and_element_null() {
    let a = list(
        Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(20),
            Some(30),
            Some(40),
        ])),
        vec![0, 2, 3, 4, 5, 5, 5],
        Some(vec![true, true, true, false, true, true]),
    );
    let idx: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(2),
        Some(0),
        Some(-1),
        Some(1),
        Some(1),
        None,
    ]));
    assert_eq!(
        ints(&raw(vec![a, idx], None).unwrap()),
        vec![None, None, None, None, None, None]
    );
}
#[test]
fn original_array_access_int64_cast_overflow_becomes_null_and_boundary_lookup_stays_int32() {
    let a = list(
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        vec![0, 1, 2, 3, 4, 5],
        None,
    );
    let idx: ArrayRef = Arc::new(Int64Array::from(vec![
        1,
        i64::MAX,
        i64::MIN,
        i32::MAX as i64,
        0,
    ]));
    assert_eq!(
        ints(&raw(vec![a, idx], None).unwrap()),
        vec![Some(1), None, None, None, None]
    );
}
#[test]
fn original_array_access_raw_strict_flags_use_exact_old_errors_and_nulls_skip_checks() {
    let a = list(
        Arc::new(Int32Array::from(vec![7, 8, 9])),
        vec![0, 1, 2, 3],
        Some(vec![true, true, false]),
    );
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, true]));
    assert_eq!(
        raw(
            vec![
                a.clone(),
                Arc::new(Int32Array::from(vec![0, 1, 1])),
                flags.clone()
            ],
            None
        )
        .unwrap_err(),
        "Array subscript start at 1"
    );
    assert_eq!(
        raw(
            vec![
                a.clone(),
                Arc::new(Int32Array::from(vec![1, 2, 1])),
                flags.clone()
            ],
            None
        )
        .unwrap_err(),
        "Array subscript must be less than or equal to array length: 2 > 1"
    );
    assert_eq!(
        ints(
            &raw(
                vec![
                    a,
                    Arc::new(Int32Array::from(vec![None, Some(1), Some(0)])),
                    flags
                ],
                None
            )
            .unwrap()
        ),
        vec![None, Some(8), None]
    );
}
#[test]
fn original_array_access_utf8_unquotes_only_simple_wrapped_content() {
    let patterns = [
        Some("\"abc\""),
        Some("\"\""),
        Some("\"a\\b\""),
        Some("\"a\"b\""),
        Some("\"é\""),
        Some("plain"),
        None,
    ];
    let a = list(
        Arc::new(StringArray::from(patterns.to_vec())),
        (0..=patterns.len()).map(|i| i as i32).collect(),
        None,
    );
    let idx: ArrayRef = Arc::new(Int32Array::from(vec![1; patterns.len()]));
    let out = raw(vec![a, idx], None).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("abc"),
            Some(""),
            Some("\"a\\b\""),
            Some("\"a\"b\""),
            Some("é"),
            Some("plain"),
            None
        ]
    );
}
#[test]
fn original_array_access_list_check_precedes_index_cast_and_flag_check() {
    assert_eq!(
        raw(
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(Int32Array::from(vec![1]))
            ],
            None
        )
        .unwrap_err(),
        "element_at expects ListArray, got Int32"
    );
    let a = list(Arc::new(Int32Array::from(vec![1])), vec![0, 1], None);
    assert_eq!(
        raw(
            vec![
                a,
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![1]))
            ],
            None
        )
        .unwrap_err(),
        "element_at check flag must be BOOLEAN"
    );
}
#[test]
fn original_array_access_largeint_miss_uses_actual_arrow_58_nullable_indices() {
    let v = novarocks_types::largeint::array_from_i128(&[Some(i128::MIN)]).unwrap();
    let a = list(v, vec![0, 1, 1], None);
    let out = raw(vec![a, Arc::new(Int32Array::from(vec![2, 1]))], None).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out.null_count(), 2);
}
#[test]
fn original_array_access_empty_largeint_values_use_actual_arrow_58_null_output() {
    let a = list(
        new_empty_array(&DataType::FixedSizeBinary(16)),
        vec![0, 0],
        None,
    );
    let out = raw(vec![a, Arc::new(Int32Array::from(vec![1]))], None).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out.null_count(), 1);
    assert_eq!(out.data_type(), &DataType::FixedSizeBinary(16));
}
