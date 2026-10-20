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
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::{Array, ArrayRef, MapBuilder, StringArray, StringBuilder};
use std::sync::Arc;

pub fn eval_str_to_map<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    let _ = expr;
    if args.len() != 3 {
        return Err(expression_contract(
            "str_to_map requires three prepared arguments",
        ));
    }
    let str_arr = arena.eval(args[0], chunk)?;
    let entry_delim_arr = arena.eval(args[1], chunk)?;
    let kv_delim_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("str_to_map expects string"))?;
    let e_arr = entry_delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("str_to_map expects string"))?;
    let kv_arr = kv_delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("str_to_map expects string"))?;

    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    let len = s_arr.len();
    if e_arr.len() != len || kv_arr.len() != len {
        return Err(expression_contract(
            "str_to_map prepared argument lengths differ",
        ));
    }
    let mut entry_offset = 0;
    let mut key_offset = 0;
    let mut value_offset = 0;
    for i in 0..len {
        if s_arr.is_null(i) || e_arr.is_null(i) || kv_arr.is_null(i) {
            builder.append(false).map_err(map_constructor_failure)?;
            continue;
        }
        let s = s_arr.value(i);
        let entry_delim = e_arr.value(i);
        let kv_delim = kv_arr.value(i);
        let mut row_entries = 0usize;
        for entry in s.split(entry_delim).filter(|entry| !entry.is_empty()) {
            let mut parts = entry.splitn(2, kv_delim);
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            // A proof of each actual format extent precedes either append.
            novarocks_functions::aggregate_scalar::checked_map_output_offset(0, row_entries + 1)?;
            entry_offset =
                novarocks_functions::aggregate_scalar::checked_map_output_offset(entry_offset, 1)?;
            key_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                key_offset,
                key.len(),
            )?;
            value_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                value_offset,
                value.len(),
            )?;
            row_entries += 1;
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).map_err(map_constructor_failure)?;
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[derive(Debug)]
struct MapConstructorFailure(arrow::error::ArrowError);
impl std::fmt::Display for MapConstructorFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for MapConstructorFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for MapConstructorFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match &self.0 {
            arrow::error::ArrowError::MemoryError(_) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::StringMapAllocation.reason(),
            ),
            _ => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::StringMapConstructor.reason(),
            ),
        }
    }
}
fn map_constructor_failure(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(MapConstructorFailure(source))
}
#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn actual_unpaired_map_entries_keep_sdk_cause_without_text_classification() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        builder.keys().append_value("ResourceExhausted");
        let failure = builder
            .append(true)
            .map_err(map_constructor_failure)
            .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 519);
        assert!(failure.failure_fact().resource().is_none());
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<arrow::error::ArrowError>()
        );
    }
}
