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
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::expr::ExprId;
use arrow::array::{ArrayRef, StructArray, new_null_array};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::exec::expr::struct_expr::{assert_struct_child_type, finish_struct};
fn eval_new_struct<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    let output_type = arena.data_type(expr).cloned().ok_or_else(|| {
        crate::exec::expr::expression_contract(format_args!("{} missing output type", fn_name))
    })?;
    let struct_fields = match output_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(crate::exec::expr::expression_contract(format_args!(
                "{} output type must be Struct, got {:?}",
                fn_name, other
            )));
        }
    };
    if struct_fields.len() != args.len() {
        return Err(crate::exec::expr::expression_contract(format_args!(
            "{} field count mismatch: expected {}, got {}",
            fn_name,
            struct_fields.len(),
            args.len()
        )));
    }

    let num_rows = chunk.len();
    let mut arrays = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let mut array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(crate::exec::expr::expression_contract(format_args!(
                "{} field length mismatch at {}: expected {}, got {}",
                fn_name,
                idx,
                num_rows,
                array.len()
            )));
        }
        let expected_type = struct_fields[idx].data_type();
        if array.data_type() == &DataType::Null && expected_type != &DataType::Null {
            array = new_null_array(expected_type, num_rows);
        }
        if array.data_type() != expected_type
            && check_exact(expected_type, array.data_type()).is_ok()
        {
            array = retag_column(&array, expected_type)
                .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?;
        }
        assert_struct_child_type(fn_name, idx, expected_type, array.data_type())?;
        arrays.push(array);
    }
    finish_struct(struct_fields, arrays)
}

pub fn eval_row<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_new_struct(arena, expr, args, chunk, "row")
}

pub fn eval_struct<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    eval_new_struct(arena, expr, args, chunk, "struct")
}

pub fn eval_named_struct<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Err(crate::exec::expr::expression_contract(format_args!(
            "named_struct expects an even number of arguments >= 2, got {}",
            args.len()
        )));
    }
    let values: Vec<ExprId> = args.iter().skip(1).step_by(2).copied().collect();
    eval_new_struct(arena, expr, &values, chunk, "named_struct")
}
