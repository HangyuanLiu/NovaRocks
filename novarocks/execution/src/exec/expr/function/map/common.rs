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

pub(super) fn row_index(row: usize, len: usize) -> usize {
    novarocks_functions::builtin::map_lookup_core::row_index(row, len)
}
pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    novarocks_functions::builtin::map_lookup_core::cast_output(out, output_type)
        .map_err(|cause| format!("{fn_name}: failed to cast output: {cause}"))
}

pub(super) fn compare_key_to_target(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_row: usize,
) -> Result<bool, String> {
    let target_idx = row_index(target_row, targets.len());
    compare_keys_at(keys, key_idx, targets, target_idx)
}

pub(super) fn compare_keys_at(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> Result<bool, String> {
    novarocks_functions::builtin::map_lookup_core::compare_keys_at(
        keys, key_idx, targets, target_idx,
    )
}

pub(super) fn output_list_field(
    output_type: Option<&DataType>,
    default_item_type: &DataType,
    fn_name: &str,
) -> Result<Arc<Field>, String> {
    match output_type {
        Some(DataType::List(field)) => Ok(field.clone()),
        Some(other) => Err(format!(
            "{} output type must be List, got {:?}",
            fn_name, other
        )),
        None => Ok(Arc::new(Field::new(
            "item",
            default_item_type.clone(),
            true,
        ))),
    }
}

pub fn sorted_map_offsets_and_indices(
    map: &MapArray,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), String> {
    novarocks_functions::builtin::map_projection_core::sorted_map_offsets_and_indices(map)
}
pub(super) fn output_map_field(
    output_type: Option<&DataType>,
    key_type: &DataType,
    value_type: &DataType,
    fn_name: &str,
) -> Result<(Arc<Field>, bool), String> {
    match output_type {
        Some(DataType::Map(field, ordered)) => Ok((field.clone(), *ordered)),
        Some(other) => Err(format!(
            "{} output type must be Map, got {:?}",
            fn_name, other
        )),
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
