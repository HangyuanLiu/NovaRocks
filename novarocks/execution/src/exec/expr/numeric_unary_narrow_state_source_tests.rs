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

//! Real original Cast -> typed zero/Sub -> Cast NULL, plus the sole prepared recipe.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, PreparedNativeNegateRecipe,
    Selection, native_negate_computed_result_type,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("native NEGATE fixture never waits")
    }
}
#[test]
fn numeric_unary_narrow_source_raw_cast_negate_widen_null_and_recipe_metadata() {
    let source_type = FunctionValueType::new(DataType::Int64, false);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            source_type.try_to_field("source").unwrap(),
        ])),
        vec![Arc::new(Int64Array::from(vec![i64::from(i8::MIN)])) as ArrayRef],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(false);
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let narrow = arena.push_typed(
        ExprNode::Cast(source, DecimalOverflowPolicy::OutputNull),
        DataType::Int8,
    );
    let zero = arena.push_typed(ExprNode::Literal(LiteralValue::Int8(0)), DataType::Int8);
    let negate = arena.push_typed(
        ExprNode::Sub(zero, narrow, DecimalOverflowPolicy::OutputNull),
        DataType::Int8,
    );
    let widen = arena.push_typed(
        ExprNode::Cast(negate, DecimalOverflowPolicy::OutputNull),
        DataType::Int64,
    );
    let input = arena.eval(narrow, &chunk).unwrap();
    assert_eq!(input.data_type(), &DataType::Int8);
    assert!(
        !input.is_null(0),
        "the original narrowing source itself is valid -128"
    );
    let original = arena.eval(negate, &chunk).unwrap();
    assert_eq!(original.data_type(), &DataType::Int8);
    assert!(
        original.is_null(0),
        "0 - -128 = 128 casts to successful Int8 NULL"
    );
    let original_widened = arena.eval(widen, &chunk).unwrap();
    assert_eq!(original_widened.data_type(), &DataType::Int64);
    assert!(
        original_widened.is_null(0),
        "original widening preserves NULL"
    );
    let narrow_type = FunctionValueType::new(DataType::Int8, false);
    let computed = native_negate_computed_result_type(&narrow_type, &Control).unwrap();
    assert_eq!(computed.data_type, DataType::Int8);
    assert!(computed.nullable);
    assert!(
        !native_negate_computed_result_type(&source_type, &Control)
            .unwrap()
            .nullable,
        "Int64 has row overflow, not successful own NULL"
    );
    let recipe = PreparedNativeNegateRecipe::try_new(&narrow_type, &computed, &Control).unwrap();
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(1),
            &Control,
        )
        .unwrap();
    assert!(actual.errors().is_empty());
    assert_eq!(actual.values().to_data(), original.to_data());
}
