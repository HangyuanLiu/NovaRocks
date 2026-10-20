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
//! Actual ARRAY_DIFFERENCE values, prepared contracts and typed arithmetic refusals.
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
            Field::new("raw-child", values.data_type().clone(), true)
                .with_metadata([("original-field".into(), "retained".into())].into()),
        ),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ))
}
pub(super) fn target(item: DataType) -> DataType {
    DataType::List(Arc::new(
        Field::new("target-child", item, true)
            .with_metadata([("output-field".into(), "exact".into())].into()),
    ))
}
pub(super) fn raw(
    a: ArrayRef,
    target: DataType,
    arity: usize,
) -> Result<ArrayRef, crate::runtime::fragment::ExecutionFailure> {
    let slot = SlotId::new(17);
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
            kind: FunctionKind::Array("array_difference"),
            args,
        },
        target,
    );
    arena.eval(expr, &chunk)
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_array_difference_original_integer_null_reset_slices_and_target_metadata() {
    let a = list(
        Arc::new(Int64Array::from(vec![
            Some(5),
            Some(-1),
            None,
            Some(4),
            Some(9),
            Some(88),
        ])),
        vec![0, 3, 3, 5, 6],
        Some(vec![true, true, true, false]),
    );
    let ty = target(DataType::Int64);
    let out = raw(a.clone(), ty.clone(), 1).unwrap();
    assert_eq!(out.data_type(), &ty);
    assert_eq!(ints(&out), vec![Some(0), Some(-6), None, Some(0), Some(5)]);
    assert!(out.is_null(3));
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0, 3, 3, 5, 5]
    );
    assert_eq!(
        ints(&raw(a.slice(2, 2), ty.clone(), 1).unwrap()),
        vec![Some(0), Some(5)]
    );
    assert_eq!(raw(a.slice(0, 0), ty, 1).unwrap().len(), 0);
}
#[test]
fn legacy_array_difference_original_float_decimal_and_arrow_cast_order() {
    let a = list(
        Arc::new(Float64Array::from(vec![
            Some(f64::INFINITY),
            Some(f64::INFINITY),
            None,
            Some(-0.0),
            Some(f64::NEG_INFINITY),
        ])),
        vec![0, 2, 5],
        None,
    );
    let out = raw(a, target(DataType::Float64), 1).unwrap();
    let output_list = out.as_any().downcast_ref::<ListArray>().unwrap();
    let v = output_list
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(v.value(0).to_bits(), 0f64.to_bits());
    assert!(v.value(1).is_nan());
    assert!(v.is_null(2));
    assert!(v.is_null(3));
    assert_eq!(v.value(4), f64::NEG_INFINITY);
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(125), Some(150), None, Some(-25)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let a = list(values, vec![0, 2, 4], None);
    let ty = target(DataType::Decimal128(10, 2));
    let out = raw(a.clone(), ty.clone(), 1).unwrap();
    assert_eq!(out.data_type(), &ty);
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(25), None, None]
    );
    let out = raw(a, target(DataType::Float64), 1).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0.0), Some(0.25), None, None]
    );
}
#[test]
fn array_difference_prepared_contracts_and_original_arrow_source_are_distinct() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let input: ArrayRef = Arc::new(NullArray::new(1));
    assert_eq!(
        raw(input.clone(), target(DataType::Int64), 1)
            .unwrap_err()
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
    for arity in [0, 2] {
        let failure = raw(input.clone(), target(DataType::Int64), arity).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            failure.failure_fact().reason(),
            crate::runtime::fragment::ExecutionReason::ExpressionContract.reason()
        );
    }
    let input = list(
        Arc::new(Int8Array::from(vec![None])),
        vec![0, 1],
        Some(vec![false]),
    );
    assert_eq!(
        raw(input, target(DataType::Int8), 1)
            .unwrap_err()
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
    let values: ArrayRef = Arc::new(Date32Array::from(vec![None]));
    let expected = arrow::compute::cast(&values, &DataType::Boolean).unwrap_err();
    let input = list(values, vec![0, 1], Some(vec![false]));
    let failure = raw(input, target(DataType::Boolean), 1).unwrap_err();
    assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(
        original_source::<arrow::error::ArrowError>(&failure).to_string(),
        expected.to_string()
    );
}

#[test]
fn array_difference_extreme_subtraction_is_a_typed_numeric_value_failure() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_functions::builtin::array_difference_core::DifferenceRowError;
    let inputs = [
        (
            list(
                Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)])),
                vec![0, 2],
                None,
            ),
            target(DataType::Int64),
        ),
        (
            list(
                Arc::new(
                    Decimal128Array::from(vec![Some(i128::MIN), Some(i128::MAX)])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
                vec![0, 2],
                None,
            ),
            target(DataType::Decimal128(38, 0)),
        ),
    ];
    for (input, ty) in inputs {
        let failure = raw(input, ty, 1).unwrap_err();
        assert_eq!(
            failure.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
        assert_eq!(original_source::<DifferenceRowError>(&failure).row, 0);
    }
}

fn original_source<'a, T: std::error::Error + 'static>(
    error: &'a (dyn std::error::Error + 'static),
) -> &'a T {
    let mut source = error;
    loop {
        if let Some(original) = source.downcast_ref::<T>() {
            return original;
        }
        source = source
            .source()
            .expect("original domain source must survive propagation");
    }
}
