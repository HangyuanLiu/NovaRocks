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
#[cfg(test)]
#[cfg(test)]
use crate::exec::expr::ExprArena;
use std::sync::Arc;

#[cfg(test)]
use crate::exec::hll::HllHandle;
use arrow::array::ArrayRef;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;

pub fn eval_ds_hll_count_distinct_state<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    novarocks_functions::builtin::ds_hll_state_core::validate_raw_call(
        args.len(),
        arena.data_type(expr),
    )?;
    let values = arena.eval(args[0], chunk)?;
    let log_ks = if args.len() >= 2 {
        Some(arena.eval(args[1], chunk)?)
    } else {
        None
    };
    let target_types = if args.len() >= 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    novarocks_functions::builtin::ds_hll_state_core::evaluate(&values, log_ks, target_types)
        .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)
}

#[cfg(test)]
#[path = "legacy_ds_hll_scalar_baseline_tests.rs"]
mod legacy_ds_hll_scalar_baseline_tests;
