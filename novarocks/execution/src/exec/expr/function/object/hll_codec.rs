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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int64Builder, LargeBinaryArray, LargeStringArray, StringArray,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::cardinality_from_serialized_hll;
use crate::exec::expr::{ExprId, expression_contract};
use novarocks_functions::aggregate_scalar::BinaryResultBuilder;
use novarocks_types::value::hll::encode_hll_empty;

pub fn eval_hll_empty<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    _arena: &__Eval,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if !_args.is_empty() {
        return Err(expression_contract("hll_empty requires no arguments"));
    }
    let mut builder = BinaryResultBuilder::new();
    for _ in 0..chunk.len() {
        builder.append_value(encode_hll_empty())?;
    }
    Ok(Arc::new(builder.finish()?) as ArrayRef)
}

pub fn eval_hll_serialize<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(format_args!(
            "hll_serialize expects 1 argument, got {}",
            args.len()
        )));
    }
    let input = arena.eval(args[0], chunk)?;
    copy_as_binary(&input)
}

pub fn eval_hll_deserialize<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(format_args!(
            "hll_deserialize expects 1 argument, got {}",
            args.len()
        )));
    }
    let input = arena.eval(args[0], chunk)?;
    copy_as_binary(&input)
}

pub fn eval_hll_cardinality<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if args.len() != 1 {
        return Err(expression_contract(format_args!(
            "hll_cardinality expects 1 argument, got {}",
            args.len()
        )));
    }

    let input = arena.eval(args[0], chunk)?;
    let mut out = Int64Builder::with_capacity(input.len());
    for row in 0..input.len() {
        if input.is_null(row) {
            out.append_null();
        } else {
            let bytes = serialized_hll_bytes(input.as_ref(), row)?;
            out.append_value(cardinality_from_serialized_hll(bytes)?);
        }
    }

    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn serialized_hll_bytes(
    array: &dyn Array,
    row: usize,
) -> crate::runtime::fragment::ExecutionResult<&[u8]> {
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(values.value(row));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(values.value(row));
    }
    Err(expression_contract(format_args!(
        "hll_cardinality expects Binary or LargeBinary input, got {:?}",
        array.data_type()
    )))
}

fn copy_as_binary(input: &ArrayRef) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        let mut builder = BinaryResultBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null()?;
            } else {
                builder.append_value(arr.value(row))?;
            }
        }
        return Ok(Arc::new(builder.finish()?) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        let mut builder = BinaryResultBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null()?;
            } else {
                builder.append_value(arr.value(row).as_bytes())?;
            }
        }
        return Ok(Arc::new(builder.finish()?) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        let mut builder = BinaryResultBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null()?;
            } else {
                builder.append_value(arr.value(row))?;
            }
        }
        return Ok(Arc::new(builder.finish()?) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeStringArray>() {
        let mut builder = BinaryResultBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null()?;
            } else {
                builder.append_value(arr.value(row).as_bytes())?;
            }
        }
        return Ok(Arc::new(builder.finish()?) as ArrayRef);
    }

    Err(expression_contract(format_args!(
        "hll_serialize/hll_deserialize expects VARCHAR/BINARY input, got {:?}",
        input.data_type()
    )))
}
