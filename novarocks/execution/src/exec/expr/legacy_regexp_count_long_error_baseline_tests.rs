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

//! Independent long-message oracle for the original raw REGEXP_COUNT dispatcher.
//! Run unchanged before extraction and after installing the shared shell.
use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::eval_string_function;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;

#[test]
fn legacy_regexp_count_long_invalid_literal_preserves_entire_original_error_string() {
    // An unclosed character class is invalid without relying on nesting limits.
    let pattern = format!("[{}", "a".repeat(1024));
    let original_error = regex::Regex::new(&pattern).unwrap_err();
    let expected = format!("Invalid regex expression: {pattern}. Detail message: {original_error}");
    assert!(
        expected.len() > 512,
        "fixture must exceed the pure row-error limit"
    );
    for values in [vec![Some("a")], vec![None, Some("a")]] {
        let strings: ArrayRef = Arc::new(StringArray::from(values));
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![strings]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(pattern.clone())),
            DataType::Utf8,
        );
        let actual =
            eval_string_function("regexp_count", &arena, source, &[source, literal], &chunk)
                .unwrap_err();
        assert!(actual.len() > 512);
        assert_eq!(actual.as_bytes(), expected.as_bytes());
    }
}
