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
//! Independent ALL_MATCH/ANY_MATCH actual dispatch oracles.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(
            Field::new("source-item", values.data_type().clone(), true)
                .with_metadata([("source-field".into(), "retained".into())].into()),
        ),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
pub(super) fn raw(name: &'static str, a: ArrayRef, arity: usize) -> Result<ArrayRef, String> {
    let slot = SlotId::new(19);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "a",
            a.data_type().clone(),
            true,
        )])),
        vec![a.clone()],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let child = arena.push_typed(ExprNode::SlotId(slot), a.data_type().clone());
    let mut args = if arity == 0 { vec![] } else { vec![child] };
    args.extend((1..arity).map(|_| ExprId(100_000)));
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Array(name),
            args,
        },
        DataType::Int32,
    );
    arena.eval(expr, &chunk)
}
fn output(a: ArrayRef) -> Vec<Option<bool>> {
    assert_eq!(a.data_type(), &DataType::Boolean);
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_array_match_original_three_values_short_circuit_empty_root_null_slice() {
    let a = list(
        Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            None,
            Some(false),
            None,
            Some(false),
        ])),
        vec![0, 2, 4, 6, 6, 7],
        Some(vec![true, true, true, true, false]),
    );
    assert_eq!(
        output(raw("all_match", a.clone(), 1).unwrap()),
        vec![Some(true), None, Some(false), Some(true), None]
    );
    assert_eq!(
        output(raw("any_match", a.clone(), 1).unwrap()),
        vec![Some(true), Some(true), None, Some(false), None]
    );
    assert_eq!(
        output(raw("all_match", a.slice(1, 3), 1).unwrap()),
        vec![None, Some(false), Some(true)]
    );
    assert_eq!(raw("any_match", a.slice(0, 0), 1).unwrap().len(), 0);
}
#[test]
fn legacy_array_match_original_numeric_boolean_normalizer_and_safe_string_nulls() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(-1), Some(0), None])),
        Arc::new(Int16Array::from(vec![Some(-1), Some(0), None])),
        Arc::new(Int32Array::from(vec![Some(-1), Some(0), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(0), None])),
        Arc::new(UInt8Array::from(vec![Some(1), Some(0), None])),
        Arc::new(UInt16Array::from(vec![Some(1), Some(0), None])),
        Arc::new(UInt32Array::from(vec![Some(1), Some(0), None])),
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
        Arc::new(Float32Array::from(vec![Some(f32::NAN), Some(-0.0), None])),
        Arc::new(Float64Array::from(vec![
            Some(f64::INFINITY),
            Some(-0.0),
            None,
        ])),
    ];
    for a in arrays {
        let a = list(a, vec![0, 1, 2, 3], None);
        assert_eq!(
            output(raw("all_match", a.clone(), 1).unwrap()),
            vec![Some(true), Some(false), None]
        );
        assert_eq!(
            output(raw("any_match", a, 1).unwrap()),
            vec![Some(true), Some(false), None]
        );
    }
    for a in [
        Arc::new(StringArray::from(vec!["true", "invalid", " Y ", ""])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec!["true", "invalid", " Y ", ""])),
    ] {
        let a = list(a, vec![0, 2, 4], None);
        assert_eq!(
            output(raw("all_match", a.clone(), 1).unwrap()),
            vec![None, None]
        );
        assert_eq!(
            output(raw("any_match", a, 1).unwrap()),
            vec![Some(true), Some(true)]
        );
    }
}
#[test]
fn legacy_array_match_original_largeint_and_null_element_shapes() {
    let a = list(
        novarocks_types::largeint::array_from_i128(&[
            Some(i128::MIN),
            Some(0),
            None,
            Some(i128::MAX),
        ])
        .unwrap(),
        vec![0, 2, 4],
        None,
    );
    assert_eq!(
        output(raw("all_match", a.clone(), 1).unwrap()),
        vec![Some(false), None]
    );
    assert_eq!(
        output(raw("any_match", a, 1).unwrap()),
        vec![Some(true), Some(true)]
    );
    let a = list(Arc::new(NullArray::new(2)), vec![0, 1, 2, 2], None);
    assert_eq!(
        output(raw("all_match", a.clone(), 1).unwrap()),
        vec![None, None, Some(true)]
    );
    assert_eq!(
        output(raw("any_match", a, 1).unwrap()),
        vec![None, None, Some(false)]
    );
}
#[test]
fn legacy_array_match_original_full_type_error_and_arity_before_children() {
    for name in ["all_match", "any_match"] {
        let a: ArrayRef = Arc::new(NullArray::new(1));
        assert_eq!(
            raw(name, a.clone(), 1).unwrap_err(),
            format!("{name} expects ListArray, got Null")
        );
        assert_eq!(
            raw(name, a.clone(), 0).unwrap_err(),
            format!("{name} expects 1 to 1 arguments, got 0")
        );
        assert_eq!(
            raw(name, a, 2).unwrap_err(),
            format!("{name} expects 1 to 1 arguments, got 2")
        );
        let values: ArrayRef = Arc::new(Date32Array::from(vec![None]));
        let expected = arrow::compute::cast(&values, &DataType::Boolean)
            .unwrap_err()
            .to_string();
        let a = list(values, vec![0, 1], Some(vec![false]));
        assert_eq!(
            raw(name, a, 1).unwrap_err(),
            format!("{name} failed to cast element to BOOLEAN: {expected}")
        );
    }
}
