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
//! Original MAP_AGG computation, parameterized only by its actual state allocator.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork, TrackedAggScalarValue,
    build_scalar_array, tracked_scalar_from_array, tracked_scalar_to_output,
};
use crate::aggregate_scalar_fingerprint::tracked_key_fingerprint;
use allocator_api2::vec::Vec as ScalarVec;
use arrow_array::{Array, ArrayRef, MapArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Fields};
use hashbrown::{HashSet, hash_map::DefaultHashBuilder};
use std::sync::Arc;

#[derive(Debug)]
pub struct MapAggState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub seen_keys: HashSet<ScalarVec<u8, A>, DefaultHashBuilder, A>,
    pub entries: ScalarVec<(TrackedAggScalarValue<A>, Option<TrackedAggScalarValue<A>>), A>,
}
impl<A: ScalarStateAllocator> MapAggState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            seen_keys: HashSet::with_hasher_in(DefaultHashBuilder::default(), allocator.clone()),
            entries: ScalarVec::new_in(allocator.clone()),
            allocator,
        }
    }
}
/// The same first-wins computation used by both update and partial-state merge.
pub fn append_entry<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    key: TrackedAggScalarValue<A>,
    value: Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    append_entry_observed(state, key, value, work, &mut |_| Ok(()))
}
pub fn append_entry_observed<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    key: TrackedAggScalarValue<A>,
    value: Option<TrackedAggScalarValue<A>>,
    work: &mut ScalarWork<'_, '_>,
    inserted_key: &mut dyn FnMut(usize) -> Result<(), ScalarStateError>,
) -> Result<(), ScalarStateError> {
    let key_fp = tracked_key_fingerprint(&key, &state.allocator, work)?;
    work.flush()?;
    let duplicate = state.seen_keys.contains(&key_fp);
    work.flush()?;
    if duplicate {
        return Ok(());
    }
    work.flush()?;
    state.seen_keys.try_reserve(1).map_err(|_| {
        state
            .allocator
            .scalar_allocation_error("reserve map_agg key set")
    })?;
    work.flush()?;
    state.entries.try_reserve(1).map_err(|_| {
        state
            .allocator
            .scalar_allocation_error("reserve map_agg entries")
    })?;
    work.flush()?;
    let key_capacity = key_fp.capacity();
    let inserted = state.seen_keys.insert(key_fp);
    debug_assert!(inserted);
    state.entries.push((key, value));
    inserted_key(key_capacity)?;
    work.flush()?;
    Ok(())
}
/// The original packed-input shape check and child-array clone order.
pub struct MapUpdateInput {
    pub keys: ArrayRef,
    pub values: ArrayRef,
}
pub fn update_input(array: &ArrayRef) -> Result<MapUpdateInput, ScalarStateError> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "map_agg expects struct input".to_string())?;
    if struct_arr.num_columns() != 2 {
        return Err("map_agg expects 2 arguments".to_string().into());
    }
    Ok(MapUpdateInput {
        keys: struct_arr.column(0).clone(),
        values: struct_arr.column(1).clone(),
    })
}
/// Parent StructArray NULLs are intentionally ignored, matching the original body.
pub fn update_row<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    input: &MapUpdateInput,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    update_from_arrays_observed(
        state,
        &input.keys,
        row,
        &input.values,
        row,
        work,
        &mut |_| Ok(()),
    )
}
/// Distinct actual carrier addresses preserve Column, Selected and Constant inputs.
pub fn update_from_arrays_observed<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    keys: &ArrayRef,
    key_row: usize,
    values: &ArrayRef,
    value_row: usize,
    work: &mut ScalarWork<'_, '_>,
    inserted_key: &mut dyn FnMut(usize) -> Result<(), ScalarStateError>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    let Some(key) = tracked_scalar_from_array(keys, key_row, &state.allocator, work)? else {
        return Ok(());
    };
    let value = tracked_scalar_from_array(values, value_row, &state.allocator, work)?;
    append_entry_observed(state, key, value, work, inserted_key)
}
pub struct MapMergeInput<'a> {
    map: &'a MapArray,
    keys: ArrayRef,
    values: ArrayRef,
}
pub fn merge_input(array: &ArrayRef) -> Result<MapMergeInput<'_>, ScalarStateError> {
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "map_agg merge input must be MapArray".to_string())?;
    Ok(MapMergeInput {
        map,
        keys: map.keys().clone(),
        values: map.values().clone(),
    })
}
pub fn merge_row<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    input: &MapMergeInput<'_>,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), ScalarStateError> {
    merge_row_observed(state, input, row, work, &mut |_| Ok(()))
}
pub fn merge_row_observed<A: ScalarStateAllocator>(
    state: &mut MapAggState<A>,
    input: &MapMergeInput<'_>,
    row: usize,
    work: &mut ScalarWork<'_, '_>,
    inserted_key: &mut dyn FnMut(usize) -> Result<(), ScalarStateError>,
) -> Result<(), ScalarStateError> {
    work.step()?;
    if input.map.is_null(row) {
        return Ok(());
    }
    let offsets = input.map.value_offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    for idx in start..end {
        work.step()?;
        let Some(key) = tracked_scalar_from_array(&input.keys, idx, &state.allocator, work)? else {
            continue;
        };
        let value = tracked_scalar_from_array(&input.values, idx, &state.allocator, work)?;
        append_entry_observed(state, key, value, work, inserted_key)?;
    }
    Ok(())
}
pub fn build_array<'state, A: ScalarStateAllocator, I>(
    target_type: &DataType,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError>
where
    I: ExactSizeIterator<Item = &'state MapAggState<A>>,
    A: 'state,
{
    build_array_checked_states(target_type, group_states.map(Ok), work)
}
pub fn build_array_checked_states<'state, A: ScalarStateAllocator, I>(
    target_type: &DataType,
    group_states: I,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ArrayRef, ScalarStateError>
where
    I: ExactSizeIterator<Item = Result<&'state MapAggState<A>, ScalarStateError>>,
    A: 'state,
{
    let (map_field, field_defs, ordered) = parse_map_type(target_type)?;
    let key_type = field_defs[0].data_type();
    let value_type = field_defs[1].data_type();
    work.flush()?;
    let mut key_values = Vec::<Option<AggScalarValue>>::new();
    let mut value_values = Vec::<Option<AggScalarValue>>::new();
    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    work.flush()?;
    offsets.push(0_i32);
    let mut current: i64 = 0;
    for state in group_states {
        let state = state?;
        work.step()?;
        for (key, value) in &state.entries {
            work.step()?;
            key_values.push(Some(tracked_scalar_to_output(key, work)?));
            value_values.push(
                value
                    .as_ref()
                    .map(|value| tracked_scalar_to_output(value, work))
                    .transpose()?,
            );
            current += 1;
            if current > i32::MAX as i64 {
                return Err("map_agg offset overflow".to_string().into());
            }
        }
        offsets.push(current as i32);
    }
    let mut out_keys = build_scalar_array(key_type, key_values, work)?;
    let mut out_values = build_scalar_array(value_type, value_values, work)?;
    if out_keys.data_type() != field_defs[0].data_type() {
        work.flush()?;
        out_keys = cast(&out_keys, field_defs[0].data_type())
            .map_err(|e| format!("map_agg failed to cast output key: {}", e))?;
        work.flush()?;
    }
    if out_values.data_type() != field_defs[1].data_type() {
        work.flush()?;
        out_values = cast(&out_values, field_defs[1].data_type())
            .map_err(|e| format!("map_agg failed to cast output value: {}", e))?;
        work.flush()?;
    }
    work.flush()?;
    let entries = StructArray::new(field_defs, vec![out_keys, out_values], None);
    let out = MapArray::try_new(
        map_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
        ordered,
    )
    .map_err(|e| format!("map_agg: {}", e))?;
    work.flush()?;
    Ok(Arc::new(out))
}
pub fn parse_map_type(ty: &DataType) -> Result<(Arc<Field>, Fields, bool), ScalarStateError> {
    let DataType::Map(field, ordered) = ty else {
        return Err(format!("map_agg output type must be MAP, got {:?}", ty).into());
    };
    let DataType::Struct(fields) = field.data_type().clone() else {
        return Err("map_agg map entries type must be STRUCT".to_string().into());
    };
    if fields.len() != 2 {
        return Err("map_agg map entries type must have 2 fields"
            .to_string()
            .into());
    }
    Ok((field.clone(), fields, *ordered))
}

#[cfg(test)]
#[path = "aggregate_map_core_tests.rs"]
mod tests;
