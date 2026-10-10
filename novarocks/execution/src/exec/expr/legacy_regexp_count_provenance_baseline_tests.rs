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

//! Immutable pre-extraction v1 regexp_count behavioral oracles.
//! These tests use the legacy dispatcher directly and require no pure owner.
//! All three SQL overloads and both original raw integer carriers are frozen.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, NullArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;

fn column(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{}", i + 1), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn eval(name: &str, columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                c.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    eval_string_function(name, &arena, args[0], &args, &chunk(columns))
}
fn assert_rows(output: ArrayRef, expected: Vec<Option<i32>>) {
    assert_eq!(output.data_type(), &DataType::Int64);
    assert_eq!(
        output.to_data(),
        Int64Array::from(
            expected
                .into_iter()
                .map(|v| v.map(i64::from))
                .collect::<Vec<_>>()
        )
        .to_data()
    );
}

#[test]
fn legacy_regexp_count_baseline_literal_pool_and_column_do_not_share_error_policy() {
    let pattern = "(bad";
    let expected = format!(
        "Invalid regex expression: {pattern}. Detail message: {}",
        regex::Regex::new(pattern).unwrap_err()
    );
    let mut arena = ExprArena::default();
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    let literal = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(pattern.into())),
        DataType::Utf8,
    );
    let source_chunk = chunk(vec![column(vec![Some("a")])]);
    assert_eq!(
        eval_string_function(
            "regexp_count",
            &arena,
            source,
            &[source, literal],
            &source_chunk
        )
        .unwrap_err(),
        expected
    );
    assert_rows(
        eval(
            "regexp_count",
            vec![column(vec![Some("a")]), column(vec![Some(pattern)])],
        )
        .unwrap(),
        vec![None],
    );
    let mut scalar_arena = ExprArena::default();
    let source = scalar_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
    // Same checked pooled value can represent literal or folded expression;
    // v1 tests the concrete ExprNode::Literal node, not constant equality.
    let pool = super::pure_differential::constant(
        novarocks_functions::FunctionValueType::new(DataType::Utf8, false),
        column(vec![Some(pattern)]),
    );
    let pooled = scalar_arena.push_typed(ExprNode::Constant(pool), DataType::Utf8);
    assert_rows(
        eval_string_function(
            "regexp_count",
            &scalar_arena,
            source,
            &[source, pooled],
            &source_chunk,
        )
        .unwrap(),
        vec![None],
    );
}
