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
use super::common::{NumericArrayView, cast_output, value_at_f64, value_at_i64};
use super::unary_ops::eval_unary_f64;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Float64Array};
use std::sync::Arc;

fn finite_or_null(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn eval_truncate_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_unary_f64(arena, expr, args, chunk, |v| v.trunc());
    }
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_view = NumericArrayView::new(&left)?;
    let right_view = NumericArrayView::new(&right)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let v = value_at_f64(&left_view, row, len);
        let d = value_at_i64(&right_view, row, len);
        let out = match (v, d) {
            (Some(x), Some(dec)) => {
                if dec >= 0 {
                    let factor = 10_f64.powi(dec as i32);
                    finite_or_null((x * factor).trunc() / factor)
                } else {
                    let factor = 10_f64.powi((-dec) as i32);
                    finite_or_null((x / factor).trunc() * factor)
                }
            }
            _ => None,
        };
        values.push(out);
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_truncate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_truncate_impl(arena, expr, args, chunk)
}

pub fn eval_dround(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_unary_f64(arena, expr, args, chunk, |v| v.round());
    }
    eval_truncate_impl(arena, expr, args, chunk)
}

#[cfg(test)]
mod legacy_dround_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{
        Decimal128Array, Float32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(inputs: Vec<ArrayRef>) -> Vec<Option<f64>> {
        let fields = inputs
            .iter()
            .enumerate()
            .map(|(i, array)| Field::new(format!("v{i}"), array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let slots = (1..=inputs.len())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let types = inputs
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect::<Vec<_>>();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math("dround"),
                args,
            },
            DataType::Float64,
        );
        let frozen = arena.into_immutable().unwrap();
        let result = ExprArena::from_immutable(&frozen)
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn floats(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }
    fn digits(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    #[test]
    fn legacy_dround_six_fixed_unary_profiles_keep_double_and_strict_null() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int16Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Float32Array::from(vec![Some(2.0), Some(-2.0), None])),
            floats(vec![Some(2.0), Some(-2.0), None]),
        ];
        for input in inputs {
            assert_eq!(evaluate(vec![input]), vec![Some(2.0), Some(-2.0), None]);
        }
    }

    #[test]
    fn legacy_dround_unary_rounds_ties_but_binary_zero_digits_truncates() {
        let input = floats(vec![Some(2.5), Some(-2.5), Some(3.5), Some(-3.5), None]);
        assert_eq!(
            evaluate(vec![input.clone()]),
            vec![Some(3.0), Some(-3.0), Some(4.0), Some(-4.0), None]
        );
        assert_eq!(
            evaluate(vec![input, digits(vec![Some(0); 5])]),
            vec![Some(2.0), Some(-2.0), Some(3.0), Some(-3.0), None]
        );
    }

    #[test]
    fn legacy_dround_signed_zero_and_nonfinite_results_are_preserved() {
        let input = floats(vec![
            Some(-0.0),
            Some(0.0),
            Some(-0.25),
            Some(0.25),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        for inputs in [vec![input.clone()], vec![input, digits(vec![Some(0); 7])]] {
            let result = evaluate(inputs);
            assert_eq!(result[0].unwrap().to_bits(), 0x8000000000000000);
            assert_eq!(result[1].unwrap().to_bits(), 0);
            assert_eq!(result[2].unwrap().to_bits(), 0x8000000000000000);
            assert_eq!(result[3].unwrap().to_bits(), 0);
            assert_eq!(&result[4..], &[None; 3]);
        }
    }

    #[test]
    fn legacy_dround_int32_extreme_digits_preserve_original_powi_formula() {
        let result = evaluate(vec![
            floats(vec![
                Some(1.0),
                Some(-1.0),
                Some(2.0),
                Some(1.0),
                Some(-1.0),
                Some(0.0),
                Some(-0.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
            ]),
            digits(vec![
                Some(308),
                Some(308),
                Some(308),
                Some(-308),
                Some(-308),
                Some(308),
                Some(-308),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(-i32::MAX),
            ]),
        ]);
        assert_eq!(&result[..3], &[Some(1.0), Some(-1.0), None]);
        assert_eq!(result[3].unwrap().to_bits(), 0);
        assert_eq!(result[4].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(result[5].unwrap().to_bits(), 0);
        assert_eq!(result[6].unwrap().to_bits(), 0x8000000000000000);
        // For MIN digits, the original i64 negation then i32 projection
        // feeds MIN to powi. Do not introduce a wider exponent protocol.
        assert_eq!(&result[7..], &[None; 3]);
    }

    #[test]
    fn legacy_dround_decimal_unary_decodes_negative_and_large_scales_before_rounding() {
        let negative_scale: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(-123), None])
                .with_precision_and_scale(38, -2)
                .unwrap(),
        );
        assert_eq!(
            evaluate(vec![negative_scale]),
            vec![Some(12_300.0), Some(-12_300.0), None]
        );
        let large_scale: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(-1), None])
                .with_precision_and_scale(38, 38)
                .unwrap(),
        );
        let result = evaluate(vec![large_scale]);
        assert_eq!(result[0].unwrap().to_bits(), 0);
        assert_eq!(result[1].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(result[2], None);
    }

    #[test]
    fn legacy_dround_binary_null_value_or_digits_stays_successful_null() {
        assert_eq!(
            evaluate(vec![
                floats(vec![None, Some(2.5), None]),
                digits(vec![Some(0), None, None])
            ]),
            vec![None; 3]
        );
    }
}
