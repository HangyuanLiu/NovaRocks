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

//! Independent raw ROUND/DROUND behavior frozen before core extraction.
use super::{eval_math_function, round::eval_round};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array, NullArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn fixture(inputs: Vec<ArrayRef>, rows: usize) -> (ExprArena, Vec<ExprId>, Chunk) {
    let inputs = if inputs.is_empty() {
        vec![Arc::new(Int32Array::from(vec![0; rows])) as ArrayRef]
    } else {
        inputs
    };
    let fields = inputs
        .iter()
        .enumerate()
        .map(|(i, v)| Field::new(format!("v{i}"), v.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=inputs.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let types = inputs
        .iter()
        .map(|v| v.data_type().clone())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let mut arena = ExprArena::default();
    let args = slots
        .into_iter()
        .zip(types)
        .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
        .collect();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}

fn raw_round(inputs: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let rows = inputs[0].len();
    let (mut arena, args, chunk) = fixture(inputs, rows);
    let expr = output.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Round,
                args: args.clone(),
            },
            ty,
        )
    });
    eval_round(&arena, expr, args[0], args.get(1).copied(), &chunk)
}
fn raw_dround(inputs: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let rows = inputs[0].len();
    let (arena, args, chunk) = fixture(inputs, rows);
    eval_math_function("dround", &arena, ExprId(usize::MAX), &args, &chunk)
}
fn decimal(values: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
fn float(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn digits(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn raws(out: &ArrayRef) -> Vec<Option<i128>> {
    out.as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .collect()
}
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        panic!("original panic payload must be textual")
    }
}
#[test]
fn legacy_round_raw_unary_saturates_nan_and_binary_retains_nonfinite() {
    let src = float(vec![
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-0.0),
        None,
    ]);
    let one = raw_round(vec![src.clone()], Some(DataType::Int64)).unwrap();
    assert_eq!(
        one.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(i64::MAX), Some(i64::MIN), Some(0), None]
    );
    let two = raw_round(vec![src, digits(vec![Some(0); 5])], Some(DataType::Float64)).unwrap();
    let a = two.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(a.value(0).is_nan());
    assert_eq!(a.value(1), f64::INFINITY);
    assert_eq!(a.value(2), f64::NEG_INFINITY);
    assert_eq!(a.value(3).to_bits(), (-0.0f64).to_bits());
    assert!(a.is_null(4));
}
#[test]
fn legacy_round_raw_minimum_digits_keep_original_checked_negation_panic() {
    let eval = || {
        raw_round(
            vec![float(vec![Some(1.25)]), digits(vec![Some(i64::MIN)])],
            Some(DataType::Float64),
        )
    };
    let result = std::panic::catch_unwind(eval);
    if cfg!(debug_assertions) {
        assert_eq!(
            panic_text(result.unwrap_err()),
            "attempt to negate with overflow"
        );
    } else {
        let out = result.unwrap().unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
    }
    let skipped = raw_round(
        vec![float(vec![None]), digits(vec![Some(i64::MIN)])],
        Some(DataType::Float64),
    )
    .unwrap();
    assert_eq!(skipped.null_count(), 1);
}
#[test]
fn legacy_round_raw_decimal_keeps_out_of_precision_raw_without_validation() {
    let bound = 10_i128.pow(38) - 1;
    let result = raw_round(
        vec![
            decimal(vec![Some(bound), Some(-bound), None], 0),
            digits(vec![Some(-1); 3]),
        ],
        Some(DataType::Decimal128(38, 0)),
    )
    .unwrap();
    assert_eq!(
        raws(&result),
        vec![Some(10_i128.pow(38)), Some(-10_i128.pow(38)), None]
    );
    assert_eq!(result.data_type(), &DataType::Decimal128(38, 0));
}
#[test]
fn legacy_round_raw_decimal_full_errors_preserve_factor_primary_and_adjust_stages() {
    let bound = 10_i128.pow(38) - 1;
    assert_eq!(
        raw_round(
            vec![decimal(vec![Some(bound)], 0)],
            Some(DataType::Decimal128(38, 1))
        )
        .unwrap_err(),
        "decimal overflow in round"
    );
    assert_eq!(
        raw_round(
            vec![decimal(vec![Some(1)], 38), digits(vec![Some(-38)])],
            Some(DataType::Decimal128(38, 38))
        )
        .unwrap_err(),
        "decimal overflow"
    );
    assert_eq!(
        raw_round(
            vec![decimal(vec![Some(bound)], 0), digits(vec![Some(-1)])],
            Some(DataType::Decimal128(38, 2))
        )
        .unwrap_err(),
        "decimal overflow in round adjust"
    );
    // Empty and SQL NULL rows do not request a dynamic rounding factor.
    let result = raw_round(
        vec![decimal(vec![None], 38), digits(vec![Some(-38)])],
        Some(DataType::Decimal128(38, 38)),
    )
    .unwrap();
    assert_eq!(raws(&result), vec![None]);
}
#[test]
fn legacy_round_raw_full_output_errors_and_unary_scale_projection_are_unchanged() {
    assert_eq!(
        raw_round(vec![float(vec![Some(1.0)])], None).unwrap_err(),
        "round: missing output type"
    );
    assert_eq!(
        raw_round(vec![decimal(vec![Some(1)], 0)], Some(DataType::Float64)).unwrap_err(),
        "round: expected Decimal128 output type, got Float64"
    );
    let result = raw_round(
        vec![decimal(vec![Some(155), Some(-155), None], 2)],
        Some(DataType::Decimal128(38, 1)),
    )
    .unwrap();
    assert_eq!(raws(&result), vec![Some(16), Some(-16), None]);
    // An all-NULL native digits carrier still runs Arrow's static cast preparation.
    let value: ArrayRef = Arc::new(NullArray::new(1));
    let input = decimal(vec![None], -39);
    let expected = arrow::compute::cast(&input, &DataType::Int64).unwrap_err();
    assert_eq!(
        raw_round(vec![value, input], Some(DataType::Float64)).unwrap_err(),
        format!("round: failed to cast decimals to Int64: {expected}")
    );
}
#[test]
fn legacy_dround_raw_original_numeric_digits_projection_and_extreme_negation_are_unchanged() {
    let result = raw_dround(vec![
        float(vec![Some(1.99), Some(-199.9), None]),
        float(vec![Some(1.9), Some(-2.9), Some(0.0)]),
    ])
    .unwrap();
    let out = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(
        out.iter().collect::<Vec<_>>(),
        vec![Some(1.9), Some(-100.0), None]
    );
    let result = std::panic::catch_unwind(|| {
        raw_dround(vec![float(vec![Some(1.0)]), digits(vec![Some(i64::MIN)])])
    });
    if cfg!(debug_assertions) {
        assert_eq!(
            panic_text(result.unwrap_err()),
            "attempt to negate with overflow"
        );
    } else {
        assert_eq!(
            result
                .unwrap()
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
    }
}
