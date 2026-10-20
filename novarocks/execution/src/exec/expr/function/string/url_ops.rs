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

use arrow::array::{Array, ArrayRef, StringArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

fn extract_host_impl(url: &str) -> Option<&str> {
    let scheme_pos = url.find("://")?;
    let mut authority = &url[(scheme_pos + 3)..];
    let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority = &authority[..end];
    if authority.is_empty() {
        return None;
    }
    if let Some(at_idx) = authority.rfind('@') {
        authority = &authority[(at_idx + 1)..];
    }
    if authority.is_empty() {
        return None;
    }

    if authority.starts_with('[') {
        let closing = authority.find(']')?;
        return Some(&authority[1..closing]);
    }

    let host = if let Some(colon_idx) = authority.rfind(':') {
        let port = &authority[(colon_idx + 1)..];
        if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            &authority[..colon_idx]
        } else {
            authority
        }
    } else {
        authority
    };
    if host.is_empty() { None } else { Some(host) }
}

pub fn eval_url_encode<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let [argument] = args else {
        return Err(expression_contract(
            "url_encode requires exactly one prepared argument",
        ));
    };
    let input = arena.eval(*argument, chunk)?;
    novarocks_functions::builtin::evaluate_required_url_encode(&input, chunk.len())
        .map_err(ExecutionFailure::from)
}
pub fn eval_url_decode<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let [argument] = args else {
        return Err(expression_contract(
            "url_decode requires exactly one prepared argument",
        ));
    };
    let input = arena.eval(*argument, chunk)?;
    novarocks_functions::builtin::evaluate_required_url_decode(&input, chunk.len())
        .map_err(ExecutionFailure::from)
}

pub fn eval_url_extract_host<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let [argument] = args else {
        return Err(expression_contract(
            "url_extract_host requires exactly one prepared argument",
        ));
    };
    let arr = arena.eval(*argument, chunk)?;
    let arr = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("url_extract_host expects string"))?;

    let rows = arr.len().max(chunk.len());
    if arr.len() != 1 && arr.len() != rows {
        return Err(expression_contract(format_args!(
            "url_extract_host input length mismatch: {} vs {rows}",
            arr.len()
        )));
    }

    let mut out = Vec::with_capacity(rows);
    let mut output_offset = 0;
    for row in 0..rows {
        let idx = if arr.len() == 1 { 0 } else { row };
        if arr.is_null(idx) {
            out.push(None);
            continue;
        }
        let host = extract_host_impl(arr.value(idx));
        if let Some(host) = host {
            output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                output_offset,
                host.len(),
            )?;
        }
        out.push(host.map(str::to_owned));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
