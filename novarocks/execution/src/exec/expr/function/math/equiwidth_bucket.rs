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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::runtime::fragment::ExecutionFailure;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use std::sync::Arc;

enum IntArgArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl IntArgArray<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Int32(arr) => arr.len(),
            Self::Int64(arr) => arr.len(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int32(arr) => arr.is_null(row),
            Self::Int64(arr) => arr.is_null(row),
        }
    }

    fn value(&self, row: usize) -> i64 {
        match self {
            Self::Int32(arr) => arr.value(row) as i64,
            Self::Int64(arr) => arr.value(row),
        }
    }
}

fn downcast_int_arg_array<'a>(arr: &'a ArrayRef) -> Result<IntArgArray<'a>, ExecutionFailure> {
    if let Some(v) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(IntArgArray::Int32(v));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(IntArgArray::Int64(v));
    }
    Err(crate::exec::expr::expression_contract(
        "equiwidth_bucket expects BIGINT arguments",
    ))
}

fn const_i64_arg(arr: IntArgArray<'_>, name: &str) -> Result<i64, ExecutionFailure> {
    if arr.len() == 0 {
        return Err(crate::exec::expr::expression_contract(format_args!(
            "equiwidth_bucket: {name} must be constant"
        )));
    }
    if arr.is_null(0) {
        return Err(crate::exec::expr::expression_contract(format_args!(
            "equiwidth_bucket: {name} must be constant"
        )));
    }
    let first = arr.value(0);
    for row in 1..arr.len() {
        if arr.is_null(row) || arr.value(row) != first {
            return Err(crate::exec::expr::expression_contract(format_args!(
                "equiwidth_bucket: {name} must be constant"
            )));
        }
    }
    Ok(first)
}

pub fn eval_equiwidth_bucket<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, ExecutionFailure> {
    let size_eval = arena.eval(args[0], chunk)?;
    let min_eval = arena.eval(args[1], chunk)?;
    let max_eval = arena.eval(args[2], chunk)?;
    let buckets_eval = arena.eval(args[3], chunk)?;

    let size_arr = downcast_int_arg_array(&size_eval)?;
    let min_arr = downcast_int_arg_array(&min_eval)?;
    let max_arr = downcast_int_arg_array(&max_eval)?;
    let buckets_arr = downcast_int_arg_array(&buckets_eval)?;

    let min = const_i64_arg(min_arr, "argument[min]")?;
    let max = const_i64_arg(max_arr, "argument[max]")?;
    let buckets = const_i64_arg(buckets_arr, "argument[buckets]")?;

    if min >= max {
        return Err(super::common::invalid_value(
            "equiwidth_bucket requirement: min < max",
        ));
    }
    if buckets <= 0 {
        return Err(super::common::invalid_value(
            "equiwidth_bucket requirement: buckets > 0",
        ));
    }

    // The valid BIGINT interval can span more than i64::MAX. Widen
    // before subtraction; a final result outside BIGINT is numeric row data.
    let width = ((i128::from(max) - i128::from(min)) / i128::from(buckets)).max(1);
    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if size_arr.is_null(row) {
            out.push(None);
            continue;
        }
        let size = size_arr.value(row);
        if size < min {
            return Err(super::common::invalid_value(
                "equiwidth_bucket requirement: size >= min",
            ));
        }
        if size > max {
            return Err(super::common::invalid_value(
                "equiwidth_bucket requirement: size <= max",
            ));
        }
        let bucket = (i128::from(size) - i128::from(min)) / width;
        let bucket = i64::try_from(bucket).map_err(|_| super::common::numeric_range())?;
        out.push(Some(bucket));
    }
    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr))
}

#[cfg(test)]
mod legacy_equiwidth_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn integers(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    fn evaluate(inputs: [ArrayRef; 4]) -> Result<Vec<Option<i64>>, ExecutionFailure> {
        let slots = [
            SlotId::new(1),
            SlotId::new(2),
            SlotId::new(3),
            SlotId::new(4),
        ];
        let types = inputs
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let fields = types
            .iter()
            .enumerate()
            .map(|(index, ty)| Field::new(format!("v{index}"), ty.clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), Vec::from(inputs)).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math("equiwidth_bucket"),
                args,
            },
            DataType::Int64,
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &DataType::Int64);
        Ok(output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect())
    }

    fn row(
        size: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
        buckets: Option<i64>,
    ) -> [ArrayRef; 4] {
        [size, min, max, buckets].map(|value| integers(vec![value]))
    }

    #[test]
    fn integer_formula_keeps_inclusive_max_and_does_not_clamp_to_bucket_count() {
        let output = evaluate([
            integers(vec![Some(-5), Some(-4), Some(0), Some(5)]),
            integers(vec![Some(-5); 4]),
            integers(vec![Some(5); 4]),
            integers(vec![Some(6); 4]),
        ])
        .unwrap();
        assert_eq!(output, vec![Some(0), Some(1), Some(5), Some(10)]);
        assert_eq!(
            evaluate(row(Some(10), Some(0), Some(10), Some(3))).unwrap(),
            vec![Some(3)]
        );
    }

    #[test]
    fn direct_int32_inputs_and_mixed_integer_carriers_produce_int64_output() {
        let output = evaluate([
            Arc::new(Int32Array::from(vec![Some(0), Some(5), Some(10)])),
            integers(vec![Some(0); 3]),
            Arc::new(Int32Array::from(vec![Some(10); 3])),
            integers(vec![Some(2); 3]),
        ])
        .unwrap();
        assert_eq!(output, vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn size_null_is_successful_only_after_nonnull_valid_parameters() {
        assert_eq!(
            evaluate(row(None, Some(0), Some(10), Some(2))).unwrap(),
            vec![None]
        );
        for (index, name) in [
            (1, "argument[min]"),
            (2, "argument[max]"),
            (3, "argument[buckets]"),
        ] {
            let mut values = [None, Some(0), Some(10), Some(2)];
            values[index] = None;
            let error = evaluate(values.map(|value| integers(vec![value]))).unwrap_err();
            assert!(error.detail().contains(name));
            assert!(error.detail().contains("must be constant"));
        }
        assert!(
            evaluate(row(None, Some(10), Some(0), Some(2)))
                .unwrap_err()
                .detail()
                .contains("min < max")
        );
        assert!(
            evaluate(row(None, Some(0), Some(10), Some(0)))
                .unwrap_err()
                .detail()
                .contains("buckets > 0")
        );
    }

    #[test]
    fn invalid_range_bucket_count_and_outside_values_remain_errors() {
        for (min, max) in [(1, 1), (2, 1)] {
            assert!(
                evaluate(row(Some(1), Some(min), Some(max), Some(2)))
                    .unwrap_err()
                    .detail()
                    .contains("min < max")
            );
        }
        for buckets in [0, -1, i64::MIN] {
            assert!(
                evaluate(row(Some(1), Some(0), Some(10), Some(buckets)))
                    .unwrap_err()
                    .detail()
                    .contains("buckets > 0")
            );
        }
        assert!(
            evaluate(row(Some(-1), Some(0), Some(10), Some(2)))
                .unwrap_err()
                .detail()
                .contains("size >= min")
        );
        assert!(
            evaluate(row(Some(11), Some(0), Some(10), Some(2)))
                .unwrap_err()
                .detail()
                .contains("size <= max")
        );
    }

    #[test]
    fn finite_and_nonfinite_float_carriers_are_all_rejected_before_numeric_evaluation() {
        for value in [1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for index in 0..4 {
                let mut inputs = row(Some(1), Some(0), Some(10), Some(2));
                inputs[index] = Arc::new(Float64Array::from(vec![Some(value)]));
                assert!(
                    evaluate(inputs)
                        .unwrap_err()
                        .detail()
                        .contains("expects BIGINT arguments")
                );
            }
        }
    }

    #[test]
    fn varying_legal_bounds_fail_together_but_succeed_as_separate_single_row_batches() {
        let together = evaluate([
            integers(vec![Some(5), Some(6)]),
            integers(vec![Some(0), Some(1)]),
            integers(vec![Some(10), Some(11)]),
            integers(vec![Some(2), Some(2)]),
        ])
        .unwrap_err();
        assert!(together.detail().contains("argument[min] must be constant"));
        assert_eq!(
            evaluate(row(Some(5), Some(0), Some(10), Some(2))).unwrap(),
            vec![Some(1)]
        );
        assert_eq!(
            evaluate(row(Some(6), Some(1), Some(11), Some(2))).unwrap(),
            vec![Some(1)]
        );
        for index in 1..4 {
            let mut inputs = [
                integers(vec![Some(1); 2]),
                integers(vec![Some(0); 2]),
                integers(vec![Some(10); 2]),
                integers(vec![Some(2); 2]),
            ];
            inputs[index] = integers(match index {
                1 => vec![Some(0), Some(1)],
                2 => vec![Some(10), Some(11)],
                _ => vec![Some(2), Some(3)],
            });
            assert!(
                evaluate(inputs)
                    .unwrap_err()
                    .detail()
                    .contains("must be constant")
            );
        }
    }

    #[test]
    fn extreme_signed_inputs_with_representable_differences_preserve_exact_integer_results() {
        // These differences fit BIGINT; their ordinary integer results stay exact.
        assert_eq!(
            evaluate(row(
                Some(i64::MIN),
                Some(i64::MIN),
                Some(-1),
                Some(i64::MAX)
            ))
            .unwrap(),
            vec![Some(0)]
        );
        assert_eq!(
            evaluate(row(Some(-1), Some(i64::MIN), Some(-1), Some(i64::MAX))).unwrap(),
            vec![Some(i64::MAX)]
        );
        assert_eq!(
            evaluate(row(Some(i64::MAX), Some(0), Some(i64::MAX), Some(i64::MAX))).unwrap(),
            vec![Some(i64::MAX)]
        );
    }
    #[test]
    fn full_bigint_span_uses_wide_subtraction_and_checks_final_output() {
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            evaluate(row(
                Some(i64::MAX),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(i64::MAX)
            ))
            .unwrap(),
            vec![Some(i64::MAX)]
        );
        assert_eq!(
            evaluate(row(Some(i64::MIN), Some(i64::MIN), Some(i64::MAX), Some(1))).unwrap(),
            vec![Some(0)]
        );
        assert_eq!(
            evaluate(row(Some(i64::MAX), Some(i64::MIN), Some(i64::MAX), Some(1))).unwrap(),
            vec![Some(1)]
        );
        // Floor width is one for this interval/count, so the inclusive maximum
        // produces 2^63, outside the frozen BIGINT result carrier.
        let failure = evaluate(row(Some(0), Some(i64::MIN), Some(0), Some(i64::MAX))).unwrap_err();
        assert_eq!(
            failure.failure_fact(),
            novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
}
