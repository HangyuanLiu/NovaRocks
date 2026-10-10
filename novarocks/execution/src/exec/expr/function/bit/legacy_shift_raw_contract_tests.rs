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
//! Independent raw v1 shift contracts; run before installing the shared core.
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::ExprNode;
use crate::exec::expr::function::FunctionKind;
use arrow::array::{NullArray, StringArray, StructArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::panic::{AssertUnwindSafe, catch_unwind};

const NAMES: [&str; 3] = [
    "bit_shift_left",
    "bit_shift_right",
    "bit_shift_right_logical",
];
fn fixture(inputs: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let types = inputs
        .iter()
        .map(|v| v.data_type().clone())
        .collect::<Vec<_>>();
    let fields = types
        .iter()
        .enumerate()
        .map(|(i, ty)| Field::new(format!("v{i}"), ty.clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=inputs.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let mut arena = ExprArena::default();
    let args = slots
        .into_iter()
        .zip(types)
        .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
        .collect();
    (
        arena,
        args,
        Chunk::new_with_chunk_schema(batch, chunk_schema),
    )
}
fn run(
    name: &'static str,
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    match name {
        "bit_shift_left" => eval_bit_shift_left(arena, expr, args, chunk),
        "bit_shift_right" => eval_bit_shift_right(arena, expr, args, chunk),
        "bit_shift_right_logical" => eval_bit_shift_right_logical(arena, expr, args, chunk),
        _ => unreachable!(),
    }
}
fn evaluate(
    name: &'static str,
    inputs: Vec<ArrayRef>,
    output: Option<DataType>,
) -> Result<ArrayRef, String> {
    let (mut arena, args, chunk) = fixture(inputs);
    let expr = output.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Bit(name),
                args: args.clone(),
            },
            ty,
        )
    });
    run(name, &arena, expr, &args, &chunk)
}
fn counts(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn assert_i64(out: ArrayRef, expected: Vec<Option<i64>>) {
    assert_eq!(out.data_type(), &DataType::Int64);
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        expected
    );
}
fn assert_large(out: ArrayRef, expected: Vec<Option<i128>>) {
    assert_eq!(out.data_type(), &DataType::FixedSizeBinary(16));
    let values = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    for (row, value) in expected.iter().enumerate() {
        match value {
            Some(value) => {
                assert!(!values.is_null(row));
                assert_eq!(values.value(row), value.to_be_bytes());
            }
            None => assert!(values.is_null(row)),
        }
    }
}
#[test]
fn legacy_shift_raw_unsigned_decimal_and_null_admission_stays_carrier_specific() {
    for name in NAMES {
        assert_large(
            evaluate(
                name,
                vec![
                    Arc::new(UInt64Array::from(vec![u64::MAX])),
                    counts(vec![Some(0)]),
                ],
                Some(DataType::FixedSizeBinary(16)),
            )
            .unwrap(),
            vec![Some(u64::MAX as i128)],
        );
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(10_i128.pow(37)), None])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_large(
            evaluate(
                name,
                vec![decimal, counts(vec![Some(0), Some(0)])],
                Some(DataType::FixedSizeBinary(16)),
            )
            .unwrap(),
            vec![Some(10_i128.pow(37)), None],
        );
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12345), Some(-12345), None])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );
        assert_large(
            evaluate(
                name,
                vec![decimal, counts(vec![Some(0); 3])],
                Some(DataType::FixedSizeBinary(16)),
            )
            .unwrap(),
            vec![Some(123), Some(-123), None],
        );
        // The original LARGEINT path normalizes its count with the same
        // i128 reader, including unsigned, decimal-zero-scale and 16-byte raw
        // counts. No intermediate BIGINT cast may null these large counts.
        let left = counts(vec![Some(1)]);
        let huge_count = largeint::array_from_i128(&[Some(i128::MAX)]).unwrap();
        let expected = match name {
            "bit_shift_left" => i128::MIN,
            _ => 0,
        };
        assert_large(
            evaluate(name, vec![left, huge_count], None).unwrap(),
            vec![Some(expected)],
        );
        let huge_count: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(10_i128.pow(37))])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_large(
            evaluate(
                name,
                vec![counts(vec![Some(1)]), huge_count],
                Some(DataType::FixedSizeBinary(16)),
            )
            .unwrap(),
            vec![Some(1)],
        );
        let nulls: ArrayRef = Arc::new(NullArray::new(3));
        assert_i64(
            evaluate(name, vec![nulls.clone(), counts(vec![Some(0); 3])], None).unwrap(),
            vec![None; 3],
        );
        assert_large(
            evaluate(
                name,
                vec![nulls, counts(vec![Some(0); 3])],
                Some(DataType::FixedSizeBinary(16)),
            )
            .unwrap(),
            vec![None; 3],
        );
    }
}
#[test]
fn legacy_shift_raw_count_arrow_cast_and_largeint_output_truncation_are_frozen() {
    let rhs: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(100), Some(-100), None])
            .with_precision_and_scale(18, 2)
            .unwrap(),
    );
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1; 3]));
    assert_i64(
        evaluate("bit_shift_left", vec![input, rhs], None).unwrap(),
        vec![Some(2), Some(i64::MIN), None],
    );
    for name in NAMES {
        let large = largeint::array_from_i128(&[Some(i128::MAX), Some(i128::MIN), None]).unwrap();
        assert_i64(
            evaluate(
                name,
                vec![large.clone(), counts(vec![Some(0); 3])],
                Some(DataType::Int64),
            )
            .unwrap(),
            vec![Some(-1), Some(0), None],
        );
        let strings = evaluate(
            name,
            vec![large, counts(vec![Some(0); 3])],
            Some(DataType::Utf8),
        )
        .unwrap();
        assert_eq!(
            strings
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("-1"), Some("0"), None]
        );
    }
}
fn unsupported() -> ArrayRef {
    let fields = vec![Arc::new(Field::new("item", DataType::Int64, true))].into();
    Arc::new(StructArray::new(fields, vec![counts(vec![Some(2)])], None))
}
#[test]
fn legacy_shift_raw_full_argument_and_output_errors_use_original_arrow_source() {
    for name in NAMES {
        for index in [0, 1] {
            let bad = unsupported();
            let cause = cast(&bad, &DataType::Int64).unwrap_err();
            let inputs = if index == 0 {
                vec![bad, counts(vec![Some(1)])]
            } else {
                vec![counts(vec![Some(1)]), bad]
            };
            assert_eq!(
                evaluate(name, inputs, None).unwrap_err(),
                format!("{name}: failed to cast arg{index} to BIGINT: {cause}")
            );
        }
        let target = unsupported().data_type().clone();
        let raw: ArrayRef = counts(vec![Some(1)]);
        let cause = cast(&raw, &target).unwrap_err();
        assert_eq!(
            evaluate(name, vec![raw, counts(vec![Some(0)])], Some(target)).unwrap_err(),
            format!("{name}: failed to cast output: {cause}")
        );
    }
}
#[test]
fn legacy_shift_raw_evaluates_right_child_before_left_conversion_failure() {
    for name in NAMES {
        let (mut arena, mut args, chunk) = fixture(vec![unsupported()]);
        let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int64);
        let expected = arena.eval(missing, &chunk).unwrap_err();
        args.push(missing);
        assert_eq!(
            run(name, &arena, ExprId(usize::MAX), &args, &chunk).unwrap_err(),
            expected
        );
    }
}
#[test]
fn legacy_shift_raw_empty_args_keep_original_index_panic() {
    let (arena, _, chunk) = fixture(vec![counts(vec![Some(0)])]);
    for name in NAMES {
        let payload = catch_unwind(AssertUnwindSafe(|| {
            run(name, &arena, ExprId(usize::MAX), &[], &chunk)
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
