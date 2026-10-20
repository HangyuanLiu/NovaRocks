use crate::exec::expr::expression_contract;
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
use arrow::array::{ArrayRef, MapArray};
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::OffsetBuffer;
use std::sync::Arc;

pub(super) const MAP_KEY_NULL_MESSAGE: &str = "map key must not be NULL";

#[derive(Debug)]
struct MapNullKey;
impl std::fmt::Display for MapNullKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(MAP_KEY_NULL_MESSAGE)
    }
}
impl std::error::Error for MapNullKey {}
impl novarocks_failure_contract::FailureProjection for MapNullKey {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            crate::runtime::fragment::ExecutionReason::MapNullKey.reason(),
        )
    }
}
pub(super) fn null_key() -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(MapNullKey)
}

pub(super) fn row_index(row: usize, len: usize) -> usize {
    novarocks_functions::builtin::map_lookup_core::row_index(row, len)
}
pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    novarocks_functions::builtin::collection_arrow::cast_output(out, output_type, fn_name)
        .map_err(crate::runtime::fragment::ExecutionFailure::from)
}

pub(super) fn compare_key_to_target(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_row: usize,
) -> crate::runtime::fragment::ExecutionResult<bool> {
    let target_idx = row_index(target_row, targets.len());
    compare_keys_at(keys, key_idx, targets, target_idx)
}

pub(super) fn compare_keys_at(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> crate::runtime::fragment::ExecutionResult<bool> {
    novarocks_functions::builtin::map_lookup_core::compare_keys_required(
        keys, key_idx, targets, target_idx,
    )
    .map_err(crate::runtime::fragment::ExecutionFailure::from)
}

pub(super) fn output_list_field(
    output_type: Option<&DataType>,
    default_item_type: &DataType,
    fn_name: &str,
) -> Result<Arc<Field>, crate::runtime::fragment::ExecutionFailure> {
    match output_type {
        Some(DataType::List(field)) => Ok(field.clone()),
        Some(other) => Err(crate::exec::expr::expression_contract(format_args!(
            "{} output type must be List, got {:?}",
            fn_name, other
        ))),
        None => Ok(Arc::new(Field::new(
            "item",
            default_item_type.clone(),
            true,
        ))),
    }
}

pub fn sorted_map_offsets_and_indices(
    map: &MapArray,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), crate::runtime::fragment::ExecutionFailure> {
    novarocks_functions::builtin::map_projection_core::sorted_map_offsets_and_indices(map)
        .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)
}
pub(super) fn output_map_field(
    output_type: Option<&DataType>,
    key_type: &DataType,
    value_type: &DataType,
    fn_name: &str,
) -> crate::runtime::fragment::ExecutionResult<(Arc<Field>, bool)> {
    match output_type {
        Some(DataType::Map(field, ordered)) => Ok((field.clone(), *ordered)),
        Some(other) => Err(expression_contract(format_args!(
            "{} output type must be Map, got {:?}",
            fn_name, other
        ))),
        None => {
            let entry_fields = Fields::from(vec![
                Arc::new(Field::new("key", key_type.clone(), true)),
                Arc::new(Field::new("value", value_type.clone(), true)),
            ]);
            Ok((
                Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
                false,
            ))
        }
    }
}

#[derive(Debug)]
struct MapLengthMismatch {
    row: usize,
    keys: usize,
    values: usize,
}
impl std::fmt::Display for MapLengthMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Key and value arrays must be the same length at row {}: keys={} values={}",
            self.row, self.keys, self.values
        )
    }
}
impl std::error::Error for MapLengthMismatch {}
impl novarocks_failure_contract::FailureProjection for MapLengthMismatch {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            crate::runtime::fragment::ExecutionReason::MapInputLengthMismatch.reason(),
        )
    }
}
pub(super) fn length_mismatch(
    row: usize,
    keys: usize,
    values: usize,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(MapLengthMismatch { row, keys, values })
}

#[cfg(test)]
mod map_length_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn differing_actual_parent_lengths_are_data_but_output_declarations_are_contracts() {
        let error = length_mismatch(3, 2, 1);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert!(error.to_string().contains("row 3: keys=2 values=1"));
        let error = output_map_field(
            Some(&DataType::Int32),
            &DataType::Int32,
            &DataType::Int32,
            "map",
        )
        .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
