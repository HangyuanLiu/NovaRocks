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
//! Original analytic author: packed physical input, real aggregate binding,
//! original partition geometry, original shared BY update/final math.
use super::*;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::analytic::WindowAggregateBinding;
use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, StringArray};
use arrow::datatypes::{Field, Fields};
use novarocks_types::SlotId;
fn original(value: ArrayRef, key: ArrayRef, running: bool) -> Option<ArrayRef> {
    original_result(value, key, running).unwrap()
}
fn original_result(
    value: ArrayRef,
    key: ArrayRef,
    running: bool,
) -> Result<Option<ArrayRef>, String> {
    let value_type = value.data_type().clone();
    let key_type = key.data_type().clone();
    let rows = value.len();
    assert_eq!(rows, key.len());
    let input_schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", value_type.clone(), true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("key", key_type.clone(), true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(3),
                Field::new("seq", DataType::Int32, false),
                None,
                None,
            ),
        ])
        .unwrap(),
    );
    let input = Chunk::try_new_with_columns(
        input_schema,
        vec![
            value,
            key,
            Arc::new(Int32Array::from(
                (0..rows)
                    .map(|n| i32::try_from(n).unwrap())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let mut arena = ExprArena::default();
    let v = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), value_type.clone());
    let k = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), key_type.clone());
    let seq = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
    let packed = arena.push_typed(
        ExprNode::StructExpr { fields: vec![v, k] },
        DataType::Struct(Fields::from(vec![
            Field::new("value", value_type.clone(), true),
            Field::new("key", key_type.clone(), true),
        ])),
    );
    let functions = crate::exec::expr::agg::test_builtin_execution_function_set();
    let mut specs = Vec::new();
    for (kind, name) in [
        (WindowFunctionKind::MaxBy, "max_by"),
        (WindowFunctionKind::MinBy, "min_by"),
    ] {
        let resolved = functions
            .catalog()
            .resolve_aggregate_trusted(name, &[value_type.clone(), key_type.clone()])
            .unwrap();
        specs.push(WindowFunctionSpec {
            kind,
            args: vec![packed],
            return_type: value_type.clone(),
            aggregate_binding: Some(WindowAggregateBinding {
                function_name: name.into(),
                resolved,
            }),
        });
    }
    let output_schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(4),
                Field::new("max_value", value_type.clone(), true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(5),
                Field::new("min_value", value_type, true),
                None,
                None,
            ),
        ])
        .unwrap(),
    );
    let frame = running.then_some(WindowFrame {
        start: None,
        end: Some(WindowBoundary::CurrentRow),
        window_type: WindowType::Rows,
    });
    let state = AnalyticSharedState::new(
        Arc::new(arena),
        vec![],
        if running { vec![seq] } else { vec![] },
        specs,
        frame,
        vec![
            AnalyticOutputColumn::Window(0),
            AnalyticOutputColumn::Window(1),
        ],
        output_schema,
        functions,
        1,
    )
    .unwrap();
    let output = state.compute_outputs(&[input], None)?;
    if rows == 0 {
        assert!(
            output.is_empty(),
            "original analytic author emits no zero-row chunk"
        );
        return Ok(None);
    }
    let columns = output[0].columns().to_vec();
    Ok(Some(Arc::new(arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new("max", columns[0].data_type().clone(), true)),
            columns[0].clone(),
        ),
        (
            Arc::new(Field::new("min", columns[1].data_type().clone(), true)),
            columns[1].clone(),
        ),
    ]))))
}
fn ints(out: &ArrayRef, column: usize) -> Vec<Option<i32>> {
    out.as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap()
        .column(column)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn decimals(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
#[test]
fn by_window_original_required_int32_decimal_key_null_winner_running_and_full() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(4), Some(9), None, Some(999)]));
    let keys = decimals(
        vec![
            Some(4_008_000_000_000),
            Some(9_006_000_000_000),
            Some(6_000_000_000),
            None,
        ],
        18,
        9,
    );
    for running in [false, true] {
        let out = original(values.clone(), keys.clone(), running).unwrap();
        assert_eq!(
            ints(&out, 0),
            if running {
                vec![Some(4), Some(9), Some(9), Some(9)]
            } else {
                vec![Some(9); 4]
            }
        );
        assert_eq!(
            ints(&out, 1),
            if running {
                vec![Some(4), Some(4), None, None]
            } else {
                vec![None; 4]
            }
        );
    }
}
#[test]
fn by_window_original_required_decimal_value_utf8_key_keeps_null_winner() {
    let values = decimals(vec![Some(100), None, Some(999)], 7, 2);
    let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("z"), None]));
    for running in [false, true] {
        let out = original(values.clone(), keys.clone(), running).unwrap();
        let s = out
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let max = s
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let min = s
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(max.data_type(), &DataType::Decimal128(7, 2));
        assert_eq!(
            max.iter().collect::<Vec<_>>(),
            if running {
                vec![Some(100), None, None]
            } else {
                vec![None; 3]
            }
        );
        assert_eq!(min.iter().collect::<Vec<_>>(), vec![Some(100); 3]);
    }
}
#[test]
fn by_window_original_ties_first_winner_all_null_keys_and_empty() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(2)]));
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), Some(7)]));
    let out = original(values.clone(), keys, false).unwrap();
    assert_eq!(ints(&out, 0), vec![None; 2]);
    assert_eq!(ints(&out, 1), vec![None; 2]);
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
    let out = original(values.clone(), keys, false).unwrap();
    assert_eq!(ints(&out, 0), vec![None; 2]);
    assert_eq!(ints(&out, 1), vec![None; 2]);
    let empty = original(
        values.slice(0, 0),
        Arc::new(Int32Array::from(Vec::<i32>::new())),
        false,
    );
    assert!(empty.is_none());
}

#[test]
fn by_window_original_nan_second_key_is_whole_call_data_in_full_and_running_frames() {
    for running in [false, true] {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let keys: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![1.0, f64::NAN]));
        let actual = original_result(values, keys, running).unwrap_err();
        assert_eq!(
            actual,
            "window function #0: update aggregate state: float comparison is not ordered"
        );
    }
}

#[path = "analytic_by_window_empty_struct_baseline_tests.rs"]
mod empty_struct_baseline_tests;

#[path = "analytic_by_window_crosscall_order_baseline_tests.rs"]
mod crosscall_order_baseline_tests;
