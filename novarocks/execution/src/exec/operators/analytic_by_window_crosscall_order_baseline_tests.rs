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
//! Actual original function-major order across two real input partitions.
use super::*;
use arrow::array::Float64Array;
#[test]
fn by_window_original_function_major_error_order_across_partitions() {
    let schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("key0", DataType::Float64, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(3),
                Field::new("key1", DataType::Float64, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(4),
                Field::new("partition", DataType::Int32, false),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(5),
                Field::new("seq", DataType::Int32, false),
                None,
                None,
            ),
        ])
        .unwrap(),
    );
    let chunk = Chunk::try_new_with_columns(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 11, 20, 21])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 1.0, f64::NAN])),
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 1.0, 2.0])),
            Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
        ],
    )
    .unwrap();
    let mut arena = ExprArena::default();
    let value = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
    let key0 = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Float64);
    let key1 = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Float64);
    let partition = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
    let seq = arena.push_typed(ExprNode::SlotId(SlotId::new(5)), DataType::Int32);
    let packed_type = DataType::Struct(Fields::from(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("key", DataType::Float64, true),
    ]));
    let packed0 = arena.push_typed(
        ExprNode::StructExpr {
            fields: vec![value, key0],
        },
        packed_type.clone(),
    );
    let packed1 = arena.push_typed(
        ExprNode::StructExpr {
            fields: vec![value, key1],
        },
        packed_type,
    );
    let functions = crate::exec::expr::agg::test_builtin_execution_function_set();
    let specs = [
        ("max_by", WindowFunctionKind::MaxBy, packed0),
        ("min_by", WindowFunctionKind::MinBy, packed1),
    ]
    .into_iter()
    .map(|(name, kind, packed)| {
        let resolved = functions
            .catalog()
            .resolve_aggregate_trusted(name, &[DataType::Int32, DataType::Float64])
            .unwrap();
        WindowFunctionSpec {
            kind,
            args: vec![packed],
            return_type: DataType::Int32,
            aggregate_binding: Some(WindowAggregateBinding {
                function_name: name.into(),
                resolved,
            }),
        }
    })
    .collect();
    let output = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(6),
                Field::new("first", DataType::Int32, true),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("second", DataType::Int32, true),
                None,
                None,
            ),
        ])
        .unwrap(),
    );
    let state = AnalyticSharedState::new(
        Arc::new(arena),
        vec![partition],
        vec![seq],
        specs,
        Some(WindowFrame {
            start: None,
            end: Some(WindowBoundary::CurrentRow),
            window_type: WindowType::Rows,
        }),
        vec![
            AnalyticOutputColumn::Window(0),
            AnalyticOutputColumn::Window(1),
        ],
        output,
        functions,
        1,
    )
    .unwrap();
    let error = state.compute_outputs(&[chunk], None).unwrap_err();
    assert_eq!(
        error,
        "window function #0: update aggregate state: float comparison is not ordered"
    );
}
