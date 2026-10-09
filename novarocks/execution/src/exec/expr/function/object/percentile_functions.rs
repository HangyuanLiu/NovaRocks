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

use arrow::array::{Array, ArrayRef, BinaryBuilder, Float64Builder};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::percentile;

pub fn eval_percentile_hash(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BinaryBuilder::new();
    for row in 0..input.len() {
        builder.append_value(novarocks_functions::percentile_hash_core::row(&input, row)?);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_percentile_empty(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for _ in 0..chunk.len() {
        builder.append_value(percentile::encode_empty_state());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_percentile_approx_raw(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let payloads = arena.eval(args[0], chunk)?;
    let quantiles = arena.eval(args[1], chunk)?;
    let mut builder = Float64Builder::with_capacity(payloads.len());

    for row in 0..payloads.len() {
        let payload = payload_bytes_at(&payloads, row, "percentile_approx_raw")?;
        let quantile = numeric_value_at(&quantiles, row, "percentile_approx_raw")?;
        match (payload, quantile) {
            (Some(payload), Some(quantile)) => {
                let state = percentile::decode_state(payload)?;
                if let Some(value) = percentile::quantile_from_state(&state, Some(quantile))? {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn numeric_value_at(
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<f64>, String> {
    novarocks_functions::percentile_input::numeric_value_at(
        array,
        row,
        novarocks_functions::percentile_input::PercentileInputDiagnostic::LegacyLabel(context),
    )
}
pub fn payload_bytes_at<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<&'a [u8]>, String> {
    novarocks_functions::percentile_input::payload_bytes_at(
        array,
        row,
        novarocks_functions::percentile_input::PercentileInputDiagnostic::LegacyLabel(context),
    )
}
