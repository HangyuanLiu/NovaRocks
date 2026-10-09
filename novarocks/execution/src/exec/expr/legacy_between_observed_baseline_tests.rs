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

//! Independent original native BETWEEN expansion, error order and repeated use.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function};
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
// Exactly the original Native Adapter operation author, before extraction.
fn expand(
    arena: &mut ExprArena,
    operand: ExprId,
    low: ExprId,
    high: ExprId,
    negated: bool,
) -> ExprId {
    let left = arena.push_typed(
        if negated {
            ExprNode::Lt(operand, low)
        } else {
            ExprNode::Ge(operand, low)
        },
        DataType::Boolean,
    );
    let right = arena.push_typed(
        if negated {
            ExprNode::Gt(operand, high)
        } else {
            ExprNode::Le(operand, high)
        },
        DataType::Boolean,
    );
    arena.push_typed(
        if negated {
            ExprNode::Or(left, right)
        } else {
            ExprNode::And(left, right)
        },
        DataType::Boolean,
    )
}
fn chunk(arrays: Vec<ArrayRef>) -> Chunk {
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(i.to_string(), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let ids = (0..batch.num_columns())
        .map(|i| SlotId::new(u32::try_from(i + 17).unwrap()))
        .collect::<Vec<_>>();
    let cs = ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &ids).unwrap();
    Chunk::new_with_chunk_schema(batch, cs)
}
pub(super) fn original(arrays: Vec<ArrayRef>, negated: bool) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let children = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(u32::try_from(i + 17).unwrap())),
                a.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let root = expand(&mut arena, children[0], children[1], children[2], negated);
    arena.eval(root, &chunk(arrays))
}
#[test]
fn between_original_positive_negated_three_valued_boundaries_slices_and_empty() {
    let operand: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(3),
        Some(0),
        Some(9),
        Some(3),
        Some(3),
        None,
        Some(3),
        Some(3),
    ]));
    let low: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(1),
        None,
        None,
        Some(1),
        Some(1),
        Some(5),
        Some(3),
    ]));
    let high: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(5),
        None,
        Some(5),
        Some(5),
        None,
        Some(5),
        Some(1),
        Some(3),
    ]));
    let expected = vec![
        Some(true),
        Some(false),
        Some(false),
        None,
        None,
        None,
        Some(false),
        Some(true),
    ];
    for negated in [false, true] {
        for (start, len) in [(0, 8), (1, 5), (2, 0)] {
            let out = original(
                vec![
                    operand.slice(start, len),
                    low.slice(start, len),
                    high.slice(start, len),
                ],
                negated,
            )
            .unwrap();
            let wanted = BooleanArray::from(
                expected[start..start + len]
                    .iter()
                    .map(|v| v.map(|b| if negated { !b } else { b }))
                    .collect::<Vec<_>>(),
            );
            assert_eq!(out.to_data(), wanted.to_data());
        }
    }
}
#[test]
fn between_original_observed_decimal_mixed_precision_and_scale() {
    let make = |p, s, values| {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    let a = make(7, 2, vec![Some(12000), Some(9000), None]);
    let low = make(6, 1, vec![Some(1000); 3]);
    let high = make(4, 0, vec![Some(150); 3]);
    for negated in [false, true] {
        let out = original(vec![a.clone(), low.clone(), high.clone()], negated).unwrap();
        assert_eq!(
            out.to_data(),
            BooleanArray::from(vec![Some(!negated), Some(negated), None]).to_data()
        );
    }
}
fn throwing(arena: &mut ExprArena, message: String) -> ExprId {
    let condition = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let text = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(message)),
        DataType::Utf8,
    );
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::lookup_function("assert_true").unwrap(),
            args: vec![condition, text],
        },
        DataType::Boolean,
    );
    arena.push_typed(
        ExprNode::Cast(
            call,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        ),
        DataType::Int64,
    )
}
#[test]
fn between_original_lower_data_error_precedes_upper_full_error_without_clipping() {
    let c = chunk(vec![Arc::new(Int64Array::from(vec![3]))]);
    for negated in [false, true] {
        let mut arena = ExprArena::default();
        let value = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int64);
        let message = format!("first lower {}", "x".repeat(2048));
        let low = throwing(&mut arena, message.clone());
        let high = throwing(&mut arena, "unreached upper".into());
        let root = expand(&mut arena, value, low, high, negated);
        assert_eq!(arena.eval(root, &c).unwrap_err(), message);
    }
}
#[test]
fn between_original_value_boolean_dominance_does_not_skip_upper_evaluation() {
    let c = chunk(vec![Arc::new(Int64Array::from(vec![0]))]);
    for negated in [false, true] {
        let mut arena = ExprArena::default();
        let value = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int64);
        let low = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let high = throwing(
            &mut arena,
            "required upper even after dominating lower".into(),
        );
        let root = expand(&mut arena, value, low, high, negated);
        assert_eq!(
            arena.eval(root, &c).unwrap_err(),
            "required upper even after dominating lower"
        );
    }
}
#[test]
fn between_original_first_comparison_error_precedes_upper_expression() {
    let c = chunk(vec![Arc::new(arrow::array::Date32Array::from(vec![0]))]);
    let mut arena = ExprArena::default();
    let operand = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Date32);
    let low = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("not-a-date".into())),
        DataType::Utf8,
    );
    let high = throwing(&mut arena, "unreached upper".into());
    let root = expand(&mut arena, operand, low, high, false);
    assert_eq!(
        arena.eval(root, &c).unwrap_err(),
        "invalid date literal 'not-a-date'"
    );
}
#[derive(Debug)]
struct ObservedSeed {
    values: Int64Array,
    reads: Arc<AtomicUsize>,
}
// SAFETY: Every buffer, layout and lifetime method delegates to the immutable
// real Int64Array. Any borrows that same carrier while recording actual reader entry.
unsafe impl Array for ObservedSeed {
    fn as_any(&self) -> &dyn std::any::Any {
        self.reads.fetch_add(1, Ordering::Relaxed);
        &self.values
    }
    fn to_data(&self) -> arrow::array::ArrayData {
        self.values.to_data()
    }
    fn into_data(self) -> arrow::array::ArrayData {
        self.values.into_data()
    }
    fn data_type(&self) -> &DataType {
        self.values.data_type()
    }
    fn slice(&self, o: usize, l: usize) -> ArrayRef {
        Arc::new(ObservedSeed {
            values: self.values.slice(o, l),
            reads: self.reads.clone(),
        })
    }
    fn len(&self) -> usize {
        self.values.len()
    }
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    fn offset(&self) -> usize {
        self.values.offset()
    }
    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.values.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.values.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.values.get_array_memory_size()
    }
}
#[test]
fn between_original_repeated_rand_operand_is_not_arena_memoized() {
    let reads = Arc::new(AtomicUsize::new(0));
    let seed: ArrayRef = Arc::new(ObservedSeed {
        values: Int64Array::from(vec![17; 3]),
        reads: reads.clone(),
    });
    let c = chunk(vec![seed]);
    let mut arena = ExprArena::default();
    let seed = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int64);
    let operand = arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::lookup_function("rand").unwrap(),
            args: vec![seed],
        },
        DataType::Float64,
    );
    let low = arena.push_typed(
        ExprNode::Literal(LiteralValue::Float64(-1.0)),
        DataType::Float64,
    );
    let high = arena.push_typed(
        ExprNode::Literal(LiteralValue::Float64(2.0)),
        DataType::Float64,
    );
    let root = expand(&mut arena, operand, low, high, false);
    reads.store(0, Ordering::Relaxed);
    assert_eq!(
        arena.eval(root, &c).unwrap().to_data(),
        BooleanArray::from(vec![true; 3]).to_data()
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        2,
        "actual original NumericArrayView seed reader invoked twice"
    );
}
