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

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder};
#[cfg(test)]
use arrow::array::{ListArray, StructArray};
use arrow::datatypes::DataType;

use crate::exec::expr::agg::{AggregateAllocator, RetainedMemoryPolicy};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, TrackedAggScalarValue};

#[cfg(test)]
use super::common::{build_scalar_array, scalar_from_array, tracked_scalar_from_array};
pub(super) struct ApproxTopKAgg;

#[repr(transparent)]
struct ApproxTopKState(novarocks_functions::approx_top_k_core::ApproxTopKState<AggregateAllocator>);
impl ApproxTopKState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        Self(
            novarocks_functions::approx_top_k_core::ApproxTopKState::new(AggregateAllocator::new(
                tracker,
            )),
        )
    }
}
impl std::ops::Deref for ApproxTopKState {
    type Target = novarocks_functions::approx_top_k_core::ApproxTopKState<AggregateAllocator>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ApproxTopKState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
fn update_one(
    state: &mut ApproxTopKState,
    value: Option<TrackedAggScalarValue>,
    count: i64,
) -> Result<(), String> {
    novarocks_functions::approx_top_k_core::update_one(&mut state.0, value, count)
}
fn enforce_counter_limit(state: &mut ApproxTopKState) {
    novarocks_functions::approx_top_k_core::enforce_counter_limit(&mut state.0)
}
fn serialize_state(state: &ApproxTopKState) -> Vec<u8> {
    novarocks_functions::approx_top_k_core::serialize_state(&state.0)
}
#[cfg(test)]
struct DecodedTopKEntry {
    value: Option<AggScalarValue>,
    count: i64,
}

#[cfg(test)]
fn decode_optional_scalar(bytes: &[u8]) -> Result<Option<AggScalarValue>, String> {
    fn need_len(bytes: &[u8], pos: usize, need: usize, label: &str) -> Result<(), String> {
        if pos + need > bytes.len() {
            return Err(format!("approx_top_k decode {}: buffer too short", label));
        }
        Ok(())
    }

    fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<u32, String> {
        need_len(bytes, *pos, 4, label)?;
        let v = u32::from_le_bytes(
            bytes[*pos..*pos + 4]
                .try_into()
                .map_err(|_| format!("approx_top_k decode {}: invalid u32", label))?,
        );
        *pos += 4;
        Ok(v)
    }

    fn decode_value(bytes: &[u8], pos: &mut usize) -> Result<AggScalarValue, String> {
        need_len(bytes, *pos, 1, "tag")?;
        let tag = bytes[*pos];
        *pos += 1;
        match tag {
            1 => {
                need_len(bytes, *pos, 1, "bool")?;
                let v = bytes[*pos] != 0;
                *pos += 1;
                Ok(AggScalarValue::Bool(v))
            }
            2 => {
                need_len(bytes, *pos, 8, "int64")?;
                let v = i64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode int64: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Int64(v))
            }
            3 => {
                need_len(bytes, *pos, 8, "float64")?;
                let bits = u64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode float64: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Float64(f64::from_bits(bits)))
            }
            4 => {
                let len = read_u32(bytes, pos, "utf8_len")? as usize;
                need_len(bytes, *pos, len, "utf8")?;
                let v = std::str::from_utf8(&bytes[*pos..*pos + len])
                    .map_err(|e| format!("approx_top_k decode utf8: {}", e))?
                    .to_string();
                *pos += len;
                Ok(AggScalarValue::Utf8(v))
            }
            5 => {
                need_len(bytes, *pos, 4, "date32")?;
                let v = i32::from_le_bytes(
                    bytes[*pos..*pos + 4]
                        .try_into()
                        .map_err(|_| "approx_top_k decode date32: invalid bytes".to_string())?,
                );
                *pos += 4;
                Ok(AggScalarValue::Date32(v))
            }
            6 => {
                need_len(bytes, *pos, 8, "timestamp")?;
                let v = i64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode timestamp: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Timestamp(v))
            }
            7 => {
                need_len(bytes, *pos, 16, "decimal128")?;
                let v =
                    i128::from_le_bytes(bytes[*pos..*pos + 16].try_into().map_err(|_| {
                        "approx_top_k decode decimal128: invalid bytes".to_string()
                    })?);
                *pos += 16;
                Ok(AggScalarValue::Decimal128(v))
            }
            8 => {
                let len = read_u32(bytes, pos, "struct_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "struct_item_flag")?;
                    let present = bytes[*pos];
                    *pos += 1;
                    if present == 0 {
                        out.push(None);
                    } else {
                        out.push(Some(decode_value(bytes, pos)?));
                    }
                }
                Ok(AggScalarValue::Struct(out))
            }
            9 => {
                let len = read_u32(bytes, pos, "map_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "map_key_flag")?;
                    let key_present = bytes[*pos];
                    *pos += 1;
                    let key = if key_present == 0 {
                        None
                    } else {
                        Some(decode_value(bytes, pos)?)
                    };
                    need_len(bytes, *pos, 1, "map_value_flag")?;
                    let value_present = bytes[*pos];
                    *pos += 1;
                    let value = if value_present == 0 {
                        None
                    } else {
                        Some(decode_value(bytes, pos)?)
                    };
                    out.push((key, value));
                }
                Ok(AggScalarValue::Map(out))
            }
            10 => {
                let len = read_u32(bytes, pos, "list_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "list_item_flag")?;
                    let present = bytes[*pos];
                    *pos += 1;
                    if present == 0 {
                        out.push(None);
                    } else {
                        out.push(Some(decode_value(bytes, pos)?));
                    }
                }
                Ok(AggScalarValue::List(out))
            }
            other => Err(format!("approx_top_k decode: unsupported tag {}", other)),
        }
    }

    if bytes.is_empty() {
        return Err("approx_top_k decode: empty payload".to_string());
    }
    let mut pos = 0usize;
    let present = bytes[pos];
    pos += 1;
    if present == 0 {
        if pos != bytes.len() {
            return Err("approx_top_k decode: trailing bytes on null value".to_string());
        }
        return Ok(None);
    }
    let value = decode_value(bytes, &mut pos)?;
    if pos != bytes.len() {
        return Err("approx_top_k decode: trailing bytes".to_string());
    }
    Ok(Some(value))
}

#[cfg(test)]
fn deserialize_state(bytes: &[u8]) -> Result<(usize, usize, Vec<DecodedTopKEntry>), String> {
    if bytes.len() < 12 {
        return Err("approx_top_k merge payload too short".to_string());
    }
    let mut pos = 0usize;
    let k = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode k failed".to_string())?,
    ) as usize;
    pos += 4;
    let counter_num = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode counter_num failed".to_string())?,
    ) as usize;
    pos += 4;
    let entry_num = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode entry_num failed".to_string())?,
    ) as usize;
    pos += 4;

    let mut entries = Vec::with_capacity(entry_num);
    for _ in 0..entry_num {
        if pos + 4 > bytes.len() {
            return Err("approx_top_k decode entry len failed".to_string());
        }
        let value_len = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| "approx_top_k decode value_len failed".to_string())?,
        ) as usize;
        pos += 4;
        if pos + value_len + 8 > bytes.len() {
            return Err("approx_top_k decode value/count out of bounds".to_string());
        }
        let value = decode_optional_scalar(&bytes[pos..pos + value_len])?;
        pos += value_len;
        let count = i64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| "approx_top_k decode count failed".to_string())?,
        );
        pos += 8;
        entries.push(DecodedTopKEntry { value, count });
    }

    if pos != bytes.len() {
        return Err("approx_top_k decode trailing bytes".to_string());
    }
    Ok((k, counter_num, entries))
}

fn output_topk_array(
    output_type: &DataType,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    novarocks_functions::approx_top_k_core::output_topk_array(
        output_type,
        group_states.iter().map(|&base| unsafe {
            &(*((base as *mut u8).add(offset) as *const ApproxTopKState)).0
        }),
    )
}
impl AggregateFunction for ApproxTopKAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "approx_top_k input type missing".to_string())?;
        if input_is_intermediate {
            let output_type = func
                .types
                .as_ref()
                .and_then(|t| t.output_type.clone())
                .ok_or_else(|| "approx_top_k output type missing".to_string())?;
            let intermediate_type = func
                .types
                .as_ref()
                .and_then(|t| t.intermediate_type.clone())
                .unwrap_or(DataType::Binary);
            return Ok(AggSpec {
                kind: AggKind::ApproxTopK,
                output_type,
                intermediate_type,
                input_arg_type: None,
                count_all: false,
            });
        }

        let output_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .ok_or_else(|| "approx_top_k output type missing".to_string())?;
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or(DataType::Binary);
        let input_arg_type = match input_type {
            DataType::Struct(fields) => fields.first().map(|f| f.data_type().clone()),
            other => Some(other.clone()),
        };
        Ok(AggSpec {
            kind: AggKind::ApproxTopK,
            output_type,
            intermediate_type,
            input_arg_type,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::ApproxTopK => (
                std::mem::size_of::<ApproxTopKState>(),
                std::mem::align_of::<ApproxTopKState>(),
            ),
            other => unreachable!("unexpected kind for approx_top_k: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "approx_top_k input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "approx_top_k merge input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "approx_top_k merge input must be BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked approx_top_k requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked approx_top_k requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(ptr as *mut ApproxTopKState, ApproxTopKState::new(tracker));
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ApproxTopKState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        let _ = ptr;
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("approx_top_k input view mismatch".to_string());
        };

        novarocks_functions::approx_top_k_core::check_update_array(array)?;
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ApproxTopKState) };
            novarocks_functions::approx_top_k_core::update_row(&mut state.0, array, row)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(array) = input else {
            return Err("approx_top_k merge input view mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let payload = array.value(row);
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ApproxTopKState) };
            novarocks_functions::approx_top_k_core::merge_payload(&mut state.0, payload)?;
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const ApproxTopKState) };
                builder.append_value(serialize_state(state));
            }
            return Ok(Arc::new(builder.finish()));
        }
        output_topk_array(&spec.output_type, offset, group_states)
    }
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;

    fn tracked_utf8(state: &ApproxTopKState, value: &str) -> Option<TrackedAggScalarValue> {
        Some(
            super::super::common::tracked_scalar_from_value(
                AggScalarValue::Utf8(value.to_string()),
                &state.allocator,
            )
            .unwrap(),
        )
    }

    #[test]
    fn counter_limit_bounds_state_and_keeps_cached_payload_exact() {
        let tracker = MemTracker::new_root("approx-top-k-test");
        let mut state = ApproxTopKState::new(tracker.clone());
        state.initialized = true;
        state.k = 2;
        state.counter_num = 2;

        let value = tracked_utf8(&state, "aa");
        update_one(&mut state, value, 1).unwrap();
        let value = tracked_utf8(&state, "bb");
        update_one(&mut state, value, 1).unwrap();
        assert_eq!(state.counts.len(), 2);
        let before_evict = tracker.current();

        let value = tracked_utf8(&state, "cc");
        update_one(&mut state, value, 1).unwrap();
        assert_eq!(state.counts.len(), 2);
        assert!(state.counts.values().any(|entry| entry.count == 2));

        state.counter_num = 1;
        enforce_counter_limit(&mut state);
        assert_eq!(state.counts.len(), 1);
        assert!(tracker.current() < before_evict);
        drop(state);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn weighted_summary_merge_remains_bounded() {
        let tracker = MemTracker::new_root("approx-top-k-merge-test");
        let mut source = ApproxTopKState::new(tracker.clone());
        source.initialized = true;
        source.k = 2;
        source.counter_num = 2;
        for value in ["a", "b", "c", "a", "a"] {
            let value = tracked_utf8(&source, value);
            update_one(&mut source, value, 1).unwrap();
        }
        let (_, counter_num, entries) =
            deserialize_state(&serialize_state(&source)).expect("decode summary");
        let mut merged = ApproxTopKState::new(tracker.clone());
        merged.initialized = true;
        merged.k = 2;
        merged.counter_num = counter_num;
        for entry in entries {
            let value = entry
                .value
                .map(|value| {
                    super::super::common::tracked_scalar_from_value(value, &merged.allocator)
                })
                .transpose()
                .unwrap();
            update_one(&mut merged, value, entry.count).unwrap();
        }

        assert!(merged.counts.len() <= merged.counter_num);
        drop(source);
        drop(merged);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "approx_top_k_original_baseline_tests.rs"]
mod original_baseline_tests;
