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

//! Legacy-only carrier, cast and broadcasting contracts frozen before extraction.
//! Register as a cfg(test) child of function::math, keeping existing goldens unchanged.
use super::{eval_e, eval_log, eval_pi, eval_sign};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float64Array, Int32Array,
    Int64Array, NullArray, StringArray, UInt64Array,
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
fn evaluate(
    name: &'static str,
    inputs: Vec<ArrayRef>,
    output: Option<DataType>,
) -> Result<ArrayRef, String> {
    let rows = inputs.first().map_or(3, |v| v.len());
    let (mut arena, args, chunk) = fixture(inputs, rows);
    let expr = output.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args: args.clone(),
            },
            ty,
        )
    });
    match name {
        "log" => eval_log(&arena, expr, &args, &chunk),
        "sign" => eval_sign(&arena, expr, &args, &chunk),
        "e" => eval_e(&arena, expr, &[], &chunk),
        "pi" => eval_pi(&arena, expr, &[], &chunk),
        _ => unreachable!(),
    }
}
#[test]
fn legacy_elementary_raw_sign_keeps_int64_source_and_exact_text_projection() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-2.0),
        Some(-0.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        None,
    ]));
    let raw = evaluate("sign", vec![values.clone()], None).unwrap();
    assert_eq!(raw.data_type(), &DataType::Int64);
    assert_eq!(
        raw.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-1), Some(0), Some(0), Some(1), None]
    );
    let strings = evaluate("sign", vec![values.clone()], Some(DataType::Utf8)).unwrap();
    assert_eq!(
        strings
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("-1"), Some("0"), Some("0"), Some("1"), None]
    );
    // Freeze the full error using an independent Arrow cast of the original
    // Int64 carrier; replacing that source with Float64 changes this oracle.
    let target = DataType::Struct(vec![Arc::new(Field::new("item", DataType::Int64, true))].into());
    let error = arrow::compute::cast(&raw, &target).unwrap_err();
    assert_eq!(
        evaluate("sign", vec![values], Some(target)).unwrap_err(),
        format!("math: failed to cast output: {error}")
    );
}
#[test]
fn legacy_elementary_raw_unsupported_carriers_keep_full_errors() {
    let cases: Vec<(ArrayRef, &str)> = vec![
        (
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            "unsupported numeric type: UInt64",
        ),
        (
            Arc::new(StringArray::from(vec!["1", "2", "3"])),
            "unsupported numeric type: Utf8",
        ),
        (
            Arc::new(BooleanArray::from(vec![true, false, true])),
            "unsupported numeric type: Boolean",
        ),
        (
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1)); 3])
                    .with_precision_and_scale(60, 2)
                    .unwrap(),
            ),
            "unsupported numeric type: Decimal256(60, 2)",
        ),
    ];
    for (values, error) in cases {
        for name in ["log", "sign"] {
            assert_eq!(
                evaluate(name, vec![values.clone()], Some(DataType::Float64)).unwrap_err(),
                error
            );
        }
        let numeric: ArrayRef = Arc::new(Float64Array::from(vec![2.0; 3]));
        assert_eq!(
            evaluate(
                "log",
                vec![numeric.clone(), values.clone()],
                Some(DataType::Float64)
            )
            .unwrap_err(),
            error
        );
        assert_eq!(
            evaluate("log", vec![values, numeric], Some(DataType::Float64)).unwrap_err(),
            error
        );
    }
}
#[test]
fn legacy_elementary_raw_null_and_decimal_inputs_preserve_reader_extensions() {
    for rows in [0, 1, 513] {
        for name in ["log", "sign"] {
            let result = evaluate(
                name,
                vec![Arc::new(NullArray::new(rows))],
                Some(DataType::Float64),
            )
            .unwrap();
            assert_eq!(result.len(), rows);
            assert_eq!(result.null_count(), rows);
        }
        let result = evaluate(
            "log",
            vec![
                Arc::new(NullArray::new(rows)),
                Arc::new(Float64Array::from(vec![2.0; rows])),
            ],
            Some(DataType::Float64),
        )
        .unwrap();
        assert_eq!(result.len(), rows);
        assert_eq!(result.null_count(), rows);
    }
    // Binary LOG reads raw Decimal128 even though its selected pure catalogue
    // accepts only the six primitive carriers in already-coerced arguments.
    for scale in [-2, 0, 38] {
        let base: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(2), Some(2), None])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        let value: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(8), Some(1), Some(8)])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        let result = evaluate("log", vec![base, value], None).unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);
        let array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        let divisor = 10_f64.powi(scale as i32);
        assert_eq!(
            array.value(0).to_bits(),
            (8_f64 / divisor).log(2_f64 / divisor).to_bits()
        );
        assert_eq!(
            array.value(1).to_bits(),
            (1_f64 / divisor).log(2_f64 / divisor).to_bits()
        );
        assert!(array.is_null(2));
    }
}
#[test]
fn legacy_elementary_raw_literal_pool_and_one_row_reader_broadcast_are_exact() {
    for rows in [0, 1, 513] {
        for pool in [false, true] {
            let (mut arena, _, chunk) = fixture(vec![], rows);
            let argument = if pool {
                let value = crate::exec::expr::pure_differential::constant(
                    novarocks_type_contract::FunctionValueType::new(DataType::Float64, false),
                    Arc::new(Float64Array::from(vec![-0.0])),
                );
                arena.push_typed(ExprNode::Constant(value), DataType::Float64)
            } else {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Float64(-0.0)),
                    DataType::Float64,
                )
            };
            let result = eval_sign(&arena, ExprId(usize::MAX), &[argument], &chunk).unwrap();
            assert_eq!(result.data_type(), &DataType::Int64);
            assert_eq!(result.len(), rows);
            assert!(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .all(|v| v == Some(0))
            );
        }
    }
    // Directly freeze the existing low-level one-row view extension. The
    // following shell extraction must carry this mapping into its core port.
    for values in [
        Arc::new(Float64Array::from(vec![Some(-0.0)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![None])),
    ] {
        let view = super::common::NumericArrayView::new(&values).unwrap();
        for row in 0..513 {
            let actual = super::common::value_at_f64(&view, row, 513);
            if values.is_null(0) {
                assert_eq!(actual, None);
            } else {
                assert_eq!(actual.unwrap().to_bits(), (-0.0_f64).to_bits());
            }
        }
    }
}
