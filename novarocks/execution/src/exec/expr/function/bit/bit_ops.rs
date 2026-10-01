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
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int64Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use novarocks_types::largeint;
use std::sync::Arc;

fn to_i64_array(array: &ArrayRef, fn_name: &str, arg_idx: usize) -> Result<Int64Array, String> {
    let casted = cast(array, &DataType::Int64).map_err(|e| {
        format!(
            "{}: failed to cast arg{} to BIGINT: {}",
            fn_name, arg_idx, e
        )
    })?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| format!("{}: arg{} is not BIGINT", fn_name, arg_idx))
}

fn to_i128_values(
    array: &ArrayRef,
    fn_name: &str,
    arg_idx: usize,
) -> Result<Vec<Option<i128>>, String> {
    match array.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| format!("{}: arg{} is not LARGEINT", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(largeint::i128_from_be_bytes(arr.value(row))?));
                }
            }
            Ok(out)
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| format!("{}: arg{} is not UINT64", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row) as i128));
                }
            }
            Ok(out)
        }
        DataType::Decimal128(_, scale) if *scale == 0 => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{}: arg{} is not DECIMAL128", fn_name, arg_idx))?;
            let mut out = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(arr.value(row)));
                }
            }
            Ok(out)
        }
        DataType::Null => Ok(vec![None; array.len()]),
        _ => {
            let casted = to_i64_array(array, fn_name, arg_idx)?;
            let mut out = Vec::with_capacity(casted.len());
            for row in 0..casted.len() {
                if casted.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(casted.value(row) as i128));
                }
            }
            Ok(out)
        }
    }
}

fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| format!("{}: failed to cast output: {}", fn_name, e))
}

fn cast_largeint_output(
    values: &[Option<i128>],
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    match output_type {
        None => largeint::array_from_i128(values),
        Some(t) if largeint::is_largeint_data_type(t) => largeint::array_from_i128(values),
        Some(t) => {
            let out_i64: Vec<Option<i64>> = values.iter().map(|v| v.map(|x| x as i64)).collect();
            let out = Arc::new(Int64Array::from(out_i64)) as ArrayRef;
            cast_output(out, Some(t), fn_name)
        }
    }
}

fn use_largeint_path(arena: &ExprArena, expr: ExprId, args: &[ExprId]) -> bool {
    if arena
        .data_type(expr)
        .map(largeint::is_largeint_data_type)
        .unwrap_or(false)
    {
        return true;
    }
    args.iter().any(|arg| {
        arena
            .data_type(*arg)
            .map(largeint::is_largeint_data_type)
            .unwrap_or(false)
    })
}

fn eval_unary_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64) -> i64,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i64_array(&array, fn_name, 0)?;

    let mut out = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(values.value(row))));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_unary_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128) -> i128,
{
    let array = arena.eval(args[0], chunk)?;
    let values = to_i128_values(&array, fn_name, 0)?;
    let out: Vec<Option<i128>> = values.into_iter().map(|v| v.map(&func)).collect();
    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

fn eval_binary_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64, i64) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(left.value(row), right.value(row))));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_binary_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128, i128) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        out.push(match (left[row], right[row]) {
            (Some(l), Some(r)) => Some(func(l, r)),
            _ => None,
        });
    }

    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

fn eval_shift_i64<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i64, u32) -> i64,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i64_array(&left, fn_name, 0)?;
    let right = to_i64_array(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left.is_null(row) || right.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(func(left.value(row), right.value(row) as u32)));
        }
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    cast_output(out, arena.data_type(expr), fn_name)
}

fn eval_shift_i128<F>(
    fn_name: &str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(i128, u32) -> i128,
{
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left = to_i128_values(&left, fn_name, 0)?;
    let right = to_i128_values(&right, fn_name, 1)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        out.push(match (left[row], right[row]) {
            (Some(l), Some(r)) => Some(func(l, r as u32)),
            _ => None,
        });
    }

    cast_largeint_output(&out, arena.data_type(expr), fn_name)
}

pub fn eval_bit_shift_left(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128("bit_shift_left", arena, expr, args, chunk, |a, b| {
            a.wrapping_shl(b)
        });
    }
    eval_shift_i64("bit_shift_left", arena, expr, args, chunk, |a, b| {
        a.wrapping_shl(b)
    })
}

pub fn eval_bit_shift_right(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128("bit_shift_right", arena, expr, args, chunk, |a, b| {
            a.wrapping_shr(b)
        });
    }
    eval_shift_i64("bit_shift_right", arena, expr, args, chunk, |a, b| {
        a.wrapping_shr(b)
    })
}

pub fn eval_bit_shift_right_logical(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_shift_i128(
            "bit_shift_right_logical",
            arena,
            expr,
            args,
            chunk,
            |a, b| ((a as u128).wrapping_shr(b)) as i128,
        );
    }
    eval_shift_i64(
        "bit_shift_right_logical",
        arena,
        expr,
        args,
        chunk,
        |a, b| ((a as u64).wrapping_shr(b)) as i64,
    )
}

pub fn eval_bitand(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitand", arena, expr, args, chunk, |a, b| a & b);
    }
    eval_binary_i64("bitand", arena, expr, args, chunk, |a, b| a & b)
}

pub fn eval_bitnot(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_unary_i128("bitnot", arena, expr, args, chunk, |a| !a);
    }
    eval_unary_i64("bitnot", arena, expr, args, chunk, |a| !a)
}

pub fn eval_bitor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitor", arena, expr, args, chunk, |a, b| a | b);
    }
    eval_binary_i64("bitor", arena, expr, args, chunk, |a, b| a | b)
}

pub fn eval_bitxor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if use_largeint_path(arena, expr, args) {
        return eval_binary_i128("bitxor", arena, expr, args, chunk, |a, b| a ^ b);
    }
    eval_binary_i64("bitxor", arena, expr, args, chunk, |a, b| a ^ b)
}

#[cfg(test)]
mod legacy_shift_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Int8Array, Int16Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, left: ArrayRef, right: ArrayRef) -> ArrayRef {
        let left_type = left.data_type().clone();
        let right_type = right.data_type().clone();
        assert_eq!(right_type, DataType::Int64);
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", left_type.clone(), true),
            Field::new("count", right_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), left_type.clone());
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), right_type);
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: vec![left, right],
            },
            left_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(output.data_type(), &left_type);
        output
    }
    fn narrow(ty: &DataType, values: &[Option<i64>]) -> ArrayRef {
        match ty {
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
            _ => panic!("fixture is not a signed integer carrier"),
        }
    }
    fn counts(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }
    fn assert_signed(output: &ArrayRef, expected: &[Option<i64>]) {
        let cast = cast(output, &DataType::Int64).unwrap();
        let values = cast.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), expected);
    }
    fn assert_large(output: &ArrayRef, expected: &[Option<i128>]) {
        // This legacy carrier oracle does not authenticate the logical root
        // domain. The new pure owner must independently require LARGEINT FVT.
        assert_eq!(output.data_type(), &DataType::FixedSizeBinary(16));
        let values = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(values.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!values.is_null(row));
                    assert_eq!(values.value(row), expected.to_be_bytes().as_slice());
                    assert_eq!(
                        largeint::i128_from_be_bytes(values.value(row)).unwrap(),
                        *expected
                    );
                }
                None => assert!(values.is_null(row)),
            }
        }
    }
    #[test]
    fn signed_narrow_left_computes_in_i64_before_checked_output_conversion() {
        for (ty, width) in [
            (DataType::Int8, 8),
            (DataType::Int16, 16),
            (DataType::Int32, 32),
        ] {
            let output = evaluate(
                "bit_shift_left",
                narrow(&ty, &[Some(1); 5]),
                counts(&[
                    Some(width - 2),
                    Some(width - 1),
                    Some(width),
                    Some(63),
                    Some(64),
                ]),
            );
            assert_signed(
                &output,
                &[Some(1_i64 << (width - 2)), None, None, None, Some(1)],
            );
        }
    }
    #[test]
    fn signed_narrow_logical_right_does_not_mask_before_i64_shift() {
        for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
            let output = evaluate(
                "bit_shift_right_logical",
                narrow(&ty, &[Some(-1); 6]),
                counts(&[Some(0), Some(1), Some(63), Some(64), Some(65), Some(-1)]),
            );
            assert_signed(&output, &[Some(-1), None, Some(1), Some(-1), None, Some(1)]);
        }
    }
    #[test]
    fn signed_widths_arithmetic_right_keep_sign_and_wrap_count_modulo64() {
        for (ty, min) in [
            (DataType::Int8, i8::MIN as i64),
            (DataType::Int16, i16::MIN as i64),
            (DataType::Int32, i32::MIN as i64),
            (DataType::Int64, i64::MIN),
        ] {
            let output = evaluate(
                "bit_shift_right",
                narrow(&ty, &[Some(min); 5]),
                counts(&[Some(1), Some(63), Some(64), Some(-64), Some(i64::MIN)]),
            );
            assert_signed(
                &output,
                &[Some(min / 2), Some(-1), Some(min), Some(min), Some(min)],
            );
        }
    }
    #[test]
    fn i64_negative_and_large_counts_use_u32_projection_then_modulo64() {
        let rhs = counts(&[
            Some(-1),
            Some(-64),
            Some(i64::MIN),
            Some(1_i64 << 32),
            Some((1_i64 << 32) + 1),
        ]);
        assert_signed(
            &evaluate(
                "bit_shift_left",
                narrow(&DataType::Int64, &[Some(1); 5]),
                rhs.clone(),
            ),
            &[Some(i64::MIN), Some(1), Some(1), Some(1), Some(2)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_right",
                narrow(&DataType::Int64, &[Some(-8); 5]),
                rhs.clone(),
            ),
            &[Some(-1), Some(-8), Some(-8), Some(-8), Some(-4)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_right_logical",
                narrow(&DataType::Int64, &[Some(-1); 5]),
                rhs,
            ),
            &[Some(1), Some(-1), Some(-1), Some(-1), Some(i64::MAX)],
        );
        assert_signed(
            &evaluate(
                "bit_shift_left",
                narrow(
                    &DataType::Int64,
                    &[Some(i64::MAX), Some(i64::MIN), Some(-1)],
                ),
                counts(&[Some(1); 3]),
            ),
            &[Some(-2), Some(0), Some(-2)],
        );
    }
    #[test]
    fn largeint_left_uses_modulo128_and_exact_big_endian_output() {
        let left = largeint::array_from_i128(&[Some(1); 7]).unwrap();
        let rhs = counts(&[
            Some(127),
            Some(128),
            Some(129),
            Some(-1),
            Some(-128),
            Some(i64::MIN),
            Some((1_i64 << 32) + 129),
        ]);
        assert_large(
            &evaluate("bit_shift_left", left, rhs),
            &[
                Some(i128::MIN),
                Some(1),
                Some(2),
                Some(i128::MIN),
                Some(1),
                Some(1),
                Some(2),
            ],
        );
        assert_large(
            &evaluate(
                "bit_shift_left",
                largeint::array_from_i128(&[Some(i128::MIN)]).unwrap(),
                counts(&[Some(1)]),
            ),
            &[Some(0)],
        );
    }
    #[test]
    fn largeint_arithmetic_and_logical_right_preserve_distinct_128bit_rules() {
        let rhs = counts(&[
            Some(127),
            Some(128),
            Some(129),
            Some(-1),
            Some(-128),
            Some(i64::MIN),
        ]);
        assert_large(
            &evaluate(
                "bit_shift_right",
                largeint::array_from_i128(&[Some(i128::MIN); 6]).unwrap(),
                rhs.clone(),
            ),
            &[
                Some(-1),
                Some(i128::MIN),
                Some(i128::MIN / 2),
                Some(-1),
                Some(i128::MIN),
                Some(i128::MIN),
            ],
        );
        assert_large(
            &evaluate(
                "bit_shift_right_logical",
                largeint::array_from_i128(&[Some(-1); 6]).unwrap(),
                rhs,
            ),
            &[
                Some(1),
                Some(-1),
                Some(i128::MAX),
                Some(1),
                Some(-1),
                Some(-1),
            ],
        );
    }
    #[test]
    fn nulls_and_independently_sliced_carriers_keep_original_row_alignment() {
        let left = Arc::new(Int8Array::from(vec![
            Some(99),
            Some(1),
            None,
            Some(-1),
            Some(2),
            Some(99),
        ])) as ArrayRef;
        let rhs = counts(&[Some(9), Some(7), Some(2), None, Some(64), Some(9)]);
        let output = evaluate("bit_shift_left", left.slice(1, 4), rhs.slice(1, 4));
        assert_signed(&output, &[None, None, None, Some(2)]);
        let left =
            largeint::array_from_i128(&[Some(99), Some(1), None, Some(-1), Some(99)]).unwrap();
        let rhs = counts(&[Some(9), Some(129), Some(1), None, Some(9)]);
        assert_large(
            &evaluate("bit_shift_left", left.slice(1, 3), rhs.slice(1, 3)),
            &[Some(2), None, None],
        );
    }
}
