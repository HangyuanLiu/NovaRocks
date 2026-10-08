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

//! Independent original unary carrier/cast oracles before computation extraction.
//! Register as a cfg(test) child of function::math, keeping existing goldens unchanged.
use super::eval_math_function;
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
    eval_math_function(name, &arena, expr, &args, &chunk)
}

fn doubles(array: &ArrayRef) -> Vec<Option<f64>> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_unary_raw_twenty_operations_keep_float64_source_before_target_projection() {
    use std::f64::consts::{FRAC_PI_2, FRAC_PI_3, FRAC_PI_4, FRAC_PI_6, PI};
    let cases = [
        ("acos", 0.5, FRAC_PI_3),
        ("asin", 0.5, FRAC_PI_6),
        ("atan", 1.0, FRAC_PI_4),
        ("cbrt", -8.0, -2.0),
        ("ceil", -1.2, -1.0),
        ("cos", PI, -1.0),
        ("cot", FRAC_PI_4, 1.0),
        ("degress", PI, 180.0),
        ("dlog1", 0.0, 0.0),
        ("exp", 0.0, 1.0),
        ("floor", -1.2, -2.0),
        ("ln", 1.0, 0.0),
        ("log10", 100.0, 2.0),
        ("log2", 8.0, 3.0),
        ("radians", 180.0, PI),
        ("positive", -2.0, -2.0),
        ("sin", FRAC_PI_2, 1.0),
        ("sqrt", 4.0, 2.0),
        ("square", -3.0, 9.0),
        ("tan", FRAC_PI_4, 1.0),
    ];
    assert_eq!(cases.len(), 20);
    for (name, value, expected) in cases {
        let out = evaluate(
            name,
            vec![Arc::new(Float64Array::from(vec![Some(value), None]))],
            None,
        )
        .unwrap();
        assert_eq!(out.data_type(), &DataType::Float64);
        let values = doubles(&out);
        assert_eq!(values[1], None);
        assert!(
            (values[0].unwrap() - expected).abs() <= expected.abs().max(1.0) * 1e-14,
            "{name}"
        );
    }
    for (alias, canonical) in [
        ("ceiling", "ceil"),
        ("dceil", "ceil"),
        ("dfloor", "floor"),
        ("dexp", "exp"),
        ("dlog10", "log10"),
        ("dsqrt", "sqrt"),
    ] {
        let source: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.25), Some(-0.0), None]));
        assert_eq!(
            evaluate(alias, vec![source.clone()], None)
                .unwrap()
                .to_data(),
            evaluate(canonical, vec![source], None).unwrap().to_data()
        );
    }
}
#[test]
fn legacy_unary_raw_positive_keeps_actual_source_for_identity_and_text_cast() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MAX),
        Some(9007199254740993),
        None,
    ]));
    let raw = evaluate("positive", vec![source.clone()], None).unwrap();
    assert_eq!(raw.to_data(), source.to_data());
    let strings = evaluate("positive", vec![source.clone()], Some(DataType::Utf8)).unwrap();
    assert_eq!(
        strings
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("9223372036854775807"), Some("9007199254740993"), None]
    );
    let target = DataType::Struct(vec![Arc::new(Field::new("item", DataType::Int64, true))].into());
    let original = arrow::compute::cast(&source, &target).unwrap_err();
    assert_eq!(
        evaluate("positive", vec![source], Some(target)).unwrap_err(),
        format!("math: failed to cast output: {original}")
    );
    let decimal: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(12345), None])
            .with_precision_and_scale(18, 2)
            .unwrap(),
    );
    assert_eq!(
        evaluate("positive", vec![decimal.clone()], None)
            .unwrap()
            .to_data(),
        decimal.to_data()
    );
}
#[test]
fn legacy_unary_raw_null_reader_and_positive_broad_arrow_domains_are_distinct() {
    for name in ["sqrt", "acos", "ceil", "floor"] {
        let out = evaluate(name, vec![Arc::new(NullArray::new(3))], None).unwrap();
        assert_eq!(out.data_type(), &DataType::Float64);
        assert_eq!(out.null_count(), 3);
        let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(
            evaluate(name, vec![boolean], None).unwrap_err(),
            "unsupported numeric type: Boolean"
        );
        let text: ArrayRef = Arc::new(StringArray::from(vec!["3"]));
        assert_eq!(
            evaluate(name, vec![text], None).unwrap_err(),
            "unsupported numeric type: Utf8"
        );
    }
    let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    assert_eq!(
        doubles(&evaluate("positive", vec![boolean], Some(DataType::Float64)).unwrap()),
        vec![Some(1.0), Some(0.0), None]
    );
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("1.5"),
        Some("NaN"),
        Some("not a number"),
        None,
    ]));
    assert_eq!(
        doubles(&evaluate("positive", vec![text], Some(DataType::Float64)).unwrap()),
        vec![Some(1.5), None, None, None]
    );
}
#[test]
fn legacy_unary_raw_safe_projection_and_scalar_reader_do_not_use_saturating_casts() {
    let source: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(i64::MAX as f64),
        Some(i64::MIN as f64),
        Some(-0.0),
        Some(f64::NAN),
        None,
    ]));
    for name in ["ceil", "floor"] {
        let out = evaluate(name, vec![source.clone()], Some(DataType::Int64)).unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(i64::MIN), Some(0), None, None]
        );
    }
    let single: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0)]));
    let view = super::common::NumericArrayView::new(&single).unwrap();
    for row in 0..513 {
        assert_eq!(
            super::common::value_at_f64(&view, row, 513)
                .unwrap()
                .to_bits(),
            (-0.0f64).to_bits()
        );
    }
}
