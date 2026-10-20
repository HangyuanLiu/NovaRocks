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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, ListArray, StringArray, StringBuilder, StructArray,
};
use arrow::datatypes::DataType;
use base64::Engine;
use serde_json::json;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
#[derive(Debug)]
enum DictMergeFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Json(serde_json::Error),
    Base64(base64::DecodeError),
    Utf8(std::str::Utf8Error),
}
impl std::fmt::Display for DictMergeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::Json(source) => write!(f, "dict_merge internal JSON operation failed: {source}"),
            Self::Base64(source) => {
                write!(f, "dict_merge internal base64 decoding failed: {source}")
            }
            Self::Utf8(source) => write!(f, "dict_merge internal UTF8 state is invalid: {source}"),
        }
    }
}
impl std::error::Error for DictMergeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) => None,
            Self::Json(e) => Some(e),
            Self::Base64(e) => Some(e),
            Self::Utf8(e) => Some(e),
        }
    }
}
impl FailureProjection for DictMergeFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::Contract(_) => ExecutionReason::DictMergeContract,
            Self::Json(_) => ExecutionReason::DictMergeJson,
            Self::Base64(_) => ExecutionReason::DictMergeBase64,
            Self::Utf8(_) => ExecutionReason::DictMergeUtf8,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
fn dict_merge_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(DictMergeFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
fn dict_merge_utf8(source: std::str::Utf8Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(DictMergeFailure::Utf8(source))
}
fn dict_merge_json(source: serde_json::Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(DictMergeFailure::Json(source))
}
#[derive(Debug)]
struct DictMergeThresholdFailure {
    value: i64,
    source: Option<std::num::TryFromIntError>,
    intermediate: bool,
}
impl std::fmt::Display for DictMergeThresholdFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dict_merge threshold {} is outside 0..={}",
            self.value,
            i32::MAX
        )
    }
}
impl std::error::Error for DictMergeThresholdFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as &dyn std::error::Error)
    }
}
impl FailureProjection for DictMergeThresholdFailure {
    fn failure_fact(&self) -> FailureFact {
        if self.intermediate {
            FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::DictMergeStateThreshold.reason(),
            )
        } else {
            FailureFact::new(
                FailureClass::Data,
                ExecutionReason::DictMergeThreshold.reason(),
            )
        }
    }
}
#[derive(Debug)]
struct DictMergeOutputExtent {
    total: u128,
    largest: u128,
    layout: u16,
    limit: u64,
    source: Option<std::num::TryFromIntError>,
}
impl std::fmt::Display for DictMergeOutputExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "dict_merge output extent {} (largest {}) exceeds layout {} limit {}",
            self.total, self.largest, self.layout, self.limit
        )
    }
}
impl std::error::Error for DictMergeOutputExtent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as &dyn std::error::Error)
    }
}
impl FailureProjection for DictMergeOutputExtent {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
        };
        let minimum = self.largest > u128::from(self.limit);
        FailureFact::new(
            FailureClass::Representation,
            ExecutionReason::DictMergeOutputExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: if minimum {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: std::num::NonZeroU16::new(self.layout).unwrap(),
            },
            required: u64::try_from(if minimum { self.largest } else { self.total }).ok(),
            extent_limit: Some(self.limit),
            representable_prefix: None,
        })
    }
}
fn dict_payload_count(count: usize) -> Result<i32, ExecutionFailure> {
    i32::try_from(count).map_err(|source| {
        ExecutionFailure::from_domain(DictMergeOutputExtent {
            total: count as u128,
            largest: count as u128,
            layout: 25,
            limit: i32::MAX as u64,
            source: Some(source),
        })
    })
}
#[derive(Default)]
struct DictPayloadExtent {
    total: u128,
    largest: u128,
}
impl DictPayloadExtent {
    fn append(&mut self, bytes: usize, binary: bool) -> Result<(), ExecutionFailure> {
        self.total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            ExecutionFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.largest = self.largest.max(bytes as u128);
        if self.total > i32::MAX as u128 {
            return Err(ExecutionFailure::from_domain(DictMergeOutputExtent {
                total: self.total,
                largest: self.largest,
                layout: if binary { 24 } else { 23 },
                limit: i32::MAX as u64,
                source: None,
            }));
        }
        Ok(())
    }
}

pub(super) struct DictMergeAgg;

const DEFAULT_DICT_THRESHOLD: i32 = 255;

#[derive(Debug)]
struct DictMergeState {
    allocator: AggregateAllocator,
    values: AggregateHashSet<AggregateVec<u8>>,
    over_limit: bool,
    threshold: i32,
    threshold_initialized: bool,
}

impl DictMergeState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            values: aggregate_hash_set(allocator.clone()),
            allocator,
            over_limit: false,
            threshold: 0,
            threshold_initialized: false,
        }
    }
}

impl DictMergeState {
    fn max_values_allowed(&self) -> usize {
        self.threshold() as usize + 1
    }

    fn threshold(&self) -> i32 {
        if self.threshold_initialized {
            self.threshold
        } else {
            DEFAULT_DICT_THRESHOLD
        }
    }

    fn set_threshold_from_i64(&mut self, threshold: i64) -> Result<(), ExecutionFailure> {
        self.set_threshold(threshold, false)
    }
    fn set_threshold(&mut self, value: i64, intermediate: bool) -> Result<(), ExecutionFailure> {
        let threshold = i32::try_from(value).map_err(|source| {
            ExecutionFailure::from_domain(DictMergeThresholdFailure {
                value,
                source: Some(source),
                intermediate,
            })
        })?;
        if threshold < 0 {
            return Err(ExecutionFailure::from_domain(DictMergeThresholdFailure {
                value,
                source: None,
                intermediate,
            }));
        }
        self.threshold = threshold;
        self.threshold_initialized = true;
        if self.values.len() > self.max_values_allowed() {
            self.over_limit = true;
        }
        Ok(())
    }

    fn set_threshold_if_present(&mut self, threshold: Option<i64>) -> Result<(), ExecutionFailure> {
        if let Some(v) = threshold {
            self.set_threshold_from_i64(v)
        } else {
            Ok(())
        }
    }

    fn insert_value(&mut self, value: &str) -> Result<(), ExecutionFailure> {
        if self.over_limit {
            return Ok(());
        }
        if self
            .values
            .iter()
            .any(|existing| existing.as_slice() == value.as_bytes())
        {
            return Ok(());
        }
        self.values.try_reserve(1).map_err(|source| {
            self.allocator
                .allocation_error("reserve dictionary hash set", source)
        })?;
        self.values
            .insert(aggregate_bytes(self.allocator.clone(), value.as_bytes())?);
        if self.values.len() > self.max_values_allowed() {
            self.over_limit = true;
        }
        Ok(())
    }

    fn sorted_values(&self) -> Result<Vec<String>, ExecutionFailure> {
        let mut values = self
            .values
            .iter()
            .map(|value| {
                std::str::from_utf8(value.as_slice())
                    .map(str::to_owned)
                    .map_err(dict_merge_utf8)
            })
            .collect::<Result<Vec<_>, _>>()?;
        values.sort_unstable();
        Ok(values)
    }
}

fn build_fake_values(threshold: i32) -> Result<Vec<String>, ExecutionFailure> {
    let count = threshold as usize + 1;
    dict_payload_count(count)?;
    let mut out = Vec::with_capacity(count);
    for idx in 0..=threshold {
        out.push(idx.to_string());
    }
    out.sort();
    Ok(out)
}

fn extract_threshold_row(
    array: &ArrayRef,
    row: usize,
) -> crate::runtime::fragment::ExecutionResult<Option<i64>> {
    let view = IntArrayView::new(array)?;
    Ok(view.value_at(row))
}

fn update_from_value_array_row(
    state: &mut DictMergeState,
    value_array: &ArrayRef,
    row: usize,
) -> Result<(), ExecutionFailure> {
    match value_array.data_type() {
        DataType::Utf8 => {
            let arr = value_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    dict_merge_contract("dict_merge failed to downcast value array to StringArray")
                })?;
            if !arr.is_null(row) {
                state.insert_value(arr.value(row))?;
            }
            Ok(())
        }
        DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
            let list = value_array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    dict_merge_contract("dict_merge failed to downcast value array to ListArray")
                })?;
            if list.is_null(row) {
                return Ok(());
            }
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| dict_merge_contract("dict_merge list items must be UTF8"))?;
            for idx in start..end {
                if !values.is_null(idx) {
                    state.insert_value(values.value(idx))?;
                }
            }
            Ok(())
        }
        other => Err(dict_merge_contract(format_args!(
            "dict_merge expects string or array<string> value input, got {:?}",
            other
        ))),
    }
}

fn encode_base64_no_pad(input: &str) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(input.as_bytes())
}

fn decode_base64_no_pad(input: &str) -> Result<String, ExecutionFailure> {
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(input)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|source| ExecutionFailure::from_domain(DictMergeFailure::Base64(source)))?;
    String::from_utf8(bytes).map_err(|source| dict_merge_utf8(source.utf8_error()))
}

fn serialize_intermediate_state(state: &DictMergeState) -> Result<String, ExecutionFailure> {
    let values = if state.over_limit {
        Vec::<String>::new()
    } else {
        state.sorted_values()?
    };
    serde_json::to_string(&json!({
        "t": state.threshold(),
        "o": state.over_limit,
        "v": values,
    }))
    .map_err(dict_merge_json)
}

fn merge_from_intermediate_json(
    state: &mut DictMergeState,
    value: &serde_json::Value,
) -> Result<bool, ExecutionFailure> {
    let Some(obj) = value.as_object() else {
        return Ok(false);
    };
    let Some(threshold_raw) = obj.get("t").and_then(|v| v.as_i64()) else {
        return Ok(false);
    };
    state.set_threshold(threshold_raw, true)?;

    if obj.get("o").and_then(|v| v.as_bool()).unwrap_or(false) {
        state.over_limit = true;
        return Ok(true);
    }

    if let Some(values) = obj.get("v").and_then(|v| v.as_array()) {
        for item in values {
            if state.over_limit {
                break;
            }
            let Some(text) = item.as_str() else {
                return Err(dict_merge_contract(
                    "dict_merge intermediate values must be strings",
                ));
            };
            state.insert_value(text)?;
        }
    }
    Ok(true)
}

fn merge_from_final_dict_json(
    state: &mut DictMergeState,
    value: &serde_json::Value,
) -> Result<bool, ExecutionFailure> {
    let Some(list) = value
        .get("2")
        .and_then(|v| v.get("lst"))
        .and_then(|v| v.as_array())
    else {
        return Ok(false);
    };

    if list.len() < 2 {
        return Err(dict_merge_contract(
            "dict_merge final dict payload is malformed",
        ));
    }
    let value_type = list[0].as_str().unwrap_or_default();
    if !value_type.eq_ignore_ascii_case("str") {
        return Err(dict_merge_contract(format_args!(
            "dict_merge final dict payload type mismatch: expected str, got {}",
            value_type
        )));
    }

    for item in list.iter().skip(2) {
        if state.over_limit {
            break;
        }
        let encoded = item
            .as_str()
            .ok_or_else(|| dict_merge_contract("dict_merge final dict values must be strings"))?;
        let decoded = decode_base64_no_pad(encoded)?;
        state.insert_value(&decoded)?;
    }
    Ok(true)
}

fn merge_serialized_state(state: &mut DictMergeState, raw: &str) -> Result<(), ExecutionFailure> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(dict_merge_json)?;

    if merge_from_intermediate_json(state, &value)? {
        return Ok(());
    }
    if merge_from_final_dict_json(state, &value)? {
        return Ok(());
    }

    Err(dict_merge_contract(
        "dict_merge merge payload format is not recognized",
    ))
}

fn finalize_dict_json(state: &DictMergeState) -> Result<Option<String>, ExecutionFailure> {
    if !state.over_limit && state.values.is_empty() {
        return Ok(None);
    }

    let values = if state.over_limit {
        build_fake_values(state.threshold())?
    } else {
        state.sorted_values()?
    };

    if values.is_empty() {
        return Ok(None);
    }

    let value_count = dict_payload_count(values.len())?;
    let metadata_len = values.len().checked_add(2).ok_or_else(|| {
        ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    let mut string_lst = Vec::with_capacity(metadata_len);
    string_lst.push(json!("str"));
    string_lst.push(json!(value_count));
    for value in &values {
        string_lst.push(json!(encode_base64_no_pad(value)));
    }

    let mut id_lst = Vec::with_capacity(metadata_len);
    id_lst.push(json!("i32"));
    id_lst.push(json!(value_count));
    for idx in 0..values.len() {
        id_lst.push(json!((idx + 1) as i32));
    }

    serde_json::to_string(&json!({
        "2": {"lst": string_lst},
        "3": {"lst": id_lst},
    }))
    .map(Some)
    .map_err(dict_merge_json)
}

fn append_serialized_intermediate(
    builder_utf8: &mut Option<StringBuilder>,
    builder_binary: &mut Option<BinaryBuilder>,
    payload: &str,
) {
    if let Some(builder) = builder_utf8.as_mut() {
        builder.append_value(payload);
    }
    if let Some(builder) = builder_binary.as_mut() {
        builder.append_value(payload.as_bytes());
    }
}

fn append_final_payload(
    builder_utf8: &mut Option<StringBuilder>,
    builder_binary: &mut Option<BinaryBuilder>,
    payload: Option<String>,
) {
    match payload {
        Some(value) => {
            if let Some(builder) = builder_utf8.as_mut() {
                builder.append_value(&value);
            }
            if let Some(builder) = builder_binary.as_mut() {
                builder.append_value(value.as_bytes());
            }
        }
        None => {
            if let Some(builder) = builder_utf8.as_mut() {
                builder.append_null();
            }
            if let Some(builder) = builder_binary.as_mut() {
                builder.append_null();
            }
        }
    }
}

impl AggregateFunction for DictMergeAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        let sig = super::super::agg_type_signature(func).ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "dict_merge type signature missing",
            )
        })?;
        let output_type = sig.output_type.as_ref().cloned().unwrap_or(DataType::Utf8);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or(DataType::Utf8);

        if input_is_intermediate {
            if let Some(t) = input_type {
                match t {
                    DataType::Utf8 | DataType::Binary => {}
                    DataType::Struct(fields) if fields.len() == 2 => {}
                    other => {
                        return Err(
                            crate::exec::expr::agg::functions::aggregate_binding_contract(
                                format_args!(
                                    "dict_merge merge expects varchar/binary or struct input, got {:?}",
                                    other
                                ),
                            ),
                        );
                    }
                }
            }
            return Ok(AggSpec {
                kind: AggKind::DictMerge,
                output_type,
                intermediate_type,
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let input_type = input_type.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "dict_merge input type missing",
            )
        })?;
        let DataType::Struct(fields) = input_type else {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                    "dict_merge expects 2-arg struct input, got {:?}",
                    input_type
                )),
            );
        };
        if fields.len() != 2 {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                    "dict_merge expects 2 arguments, got {}",
                    fields.len()
                )),
            );
        }

        let value_type = fields[0].data_type();
        match value_type {
            DataType::Utf8 => {}
            DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {}
            other => {
                return Err(
                    crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                        "dict_merge first argument must be string or array<string>, got {:?}",
                        other
                    )),
                );
            }
        }

        let threshold_type = fields[1].data_type();
        match threshold_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {}
            other => {
                return Err(
                    crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                        "dict_merge threshold argument must be integer, got {:?}",
                        other
                    )),
                );
            }
        }

        Ok(AggSpec {
            kind: AggKind::DictMerge,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<DictMergeState>(),
            std::mem::align_of::<DictMergeState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract("dict_merge input missing")
        })?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "dict_merge merge input missing",
            )
        })?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut DictMergeState,
                DictMergeState::new(process_mem_tracker()),
            );
        }
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let tracker = tracker.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "allocation-tracked dict_merge state requires a memory tracker",
            )
        })?;
        unsafe { std::ptr::write(ptr.cast::<DictMergeState>(), DictMergeState::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut DictMergeState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
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
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Any(array) = input else {
            return Err(dict_merge_contract("dict_merge input type mismatch"));
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| dict_merge_contract("dict_merge expects struct input"))?;
        if struct_arr.num_columns() != 2 {
            return Err(dict_merge_contract(format_args!(
                "dict_merge expects 2 arguments, got {}",
                struct_arr.num_columns()
            )));
        }

        let value_arr = struct_arr.column(0).clone();
        let threshold_arr = struct_arr.column(1).clone();

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DictMergeState) };
            state.set_threshold_if_present(extract_threshold_row(&threshold_arr, row)?)?;
            if state.over_limit {
                continue;
            }
            update_from_value_array_row(state, &value_arr, row)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Any(array) = input else {
            return Err(dict_merge_contract("dict_merge merge input type mismatch"));
        };

        let (value_arr, threshold_arr_opt): (ArrayRef, Option<ArrayRef>) =
            if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
                if struct_arr.num_columns() != 2 {
                    return Err(dict_merge_contract(format_args!(
                        "dict_merge merge struct expects 2 arguments, got {}",
                        struct_arr.num_columns()
                    )));
                }
                (
                    struct_arr.column(0).clone(),
                    Some(struct_arr.column(1).clone()),
                )
            } else {
                ((*array).clone(), None)
            };

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DictMergeState) };
            if let Some(threshold_arr) = threshold_arr_opt.as_ref() {
                state.set_threshold_if_present(extract_threshold_row(threshold_arr, row)?)?;
            }
            if state.over_limit {
                continue;
            }

            match value_arr.data_type() {
                DataType::Utf8 => {
                    let arr = value_arr
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            dict_merge_contract("dict_merge merge expects UTF8 payload")
                        })?;
                    if arr.is_null(row) {
                        continue;
                    }
                    merge_serialized_state(state, arr.value(row))?;
                }
                DataType::Binary => {
                    let arr = value_arr
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            dict_merge_contract("dict_merge merge expects Binary payload")
                        })?;
                    if arr.is_null(row) {
                        continue;
                    }
                    let raw = std::str::from_utf8(arr.value(row)).map_err(dict_merge_utf8)?;
                    merge_serialized_state(state, raw)?;
                }
                DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
                    update_from_value_array_row(state, &value_arr, row)?;
                }
                other => {
                    return Err(dict_merge_contract(format_args!(
                        "dict_merge merge input type mismatch: expected UTF8/Binary/List<Utf8>, got {:?}",
                        other
                    )));
                }
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        if output_intermediate {
            let mut utf8_builder =
                matches!(spec.intermediate_type, DataType::Utf8).then(StringBuilder::new);
            let mut binary_builder =
                matches!(spec.intermediate_type, DataType::Binary).then(BinaryBuilder::new);
            if utf8_builder.is_none() && binary_builder.is_none() {
                return Err(dict_merge_contract(format_args!(
                    "dict_merge intermediate output type must be Utf8 or Binary, got {:?}",
                    spec.intermediate_type
                )));
            }

            let mut output_extent = DictPayloadExtent::default();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const DictMergeState) };
                let payload = serialize_intermediate_state(state)?;
                output_extent.append(payload.len(), binary_builder.is_some())?;
                append_serialized_intermediate(&mut utf8_builder, &mut binary_builder, &payload);
            }

            if let Some(mut builder) = utf8_builder {
                return Ok(Arc::new(builder.finish()));
            }
            if let Some(mut builder) = binary_builder {
                return Ok(Arc::new(builder.finish()));
            }
            return Err(dict_merge_contract(
                "dict_merge failed to build intermediate output",
            ));
        }

        let mut utf8_builder = matches!(spec.output_type, DataType::Utf8).then(StringBuilder::new);
        let mut binary_builder =
            matches!(spec.output_type, DataType::Binary).then(BinaryBuilder::new);
        if utf8_builder.is_none() && binary_builder.is_none() {
            return Err(dict_merge_contract(format_args!(
                "dict_merge output type must be Utf8 or Binary, got {:?}",
                spec.output_type
            )));
        }

        let mut output_extent = DictPayloadExtent::default();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const DictMergeState) };
            let payload = finalize_dict_json(state)?;
            if let Some(value) = &payload {
                output_extent.append(value.len(), binary_builder.is_some())?;
            }
            append_final_payload(&mut utf8_builder, &mut binary_builder, payload);
        }

        if let Some(mut builder) = utf8_builder {
            return Ok(Arc::new(builder.finish()));
        }
        if let Some(mut builder) = binary_builder {
            return Ok(Arc::new(builder.finish()));
        }
        Err(dict_merge_contract("dict_merge failed to build output"))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_DICT_THRESHOLD, DictMergeState, finalize_dict_json, merge_serialized_state,
        serialize_intermediate_state,
    };
    use crate::runtime::mem_tracker::MemTracker;

    #[test]
    fn finalize_payload_uses_sorted_base64_strings() {
        let mut state = DictMergeState::new(MemTracker::new_root("dict-finalize-test"));
        state
            .set_threshold_from_i64(DEFAULT_DICT_THRESHOLD as i64)
            .unwrap();
        state.insert_value("shanghai").unwrap();
        state.insert_value("beijing").unwrap();

        let output = finalize_dict_json(&state)
            .expect("finalize")
            .expect("non-null output");
        let value: serde_json::Value = serde_json::from_str(&output).expect("parse json");

        let string_list = value
            .get("2")
            .and_then(|v| v.get("lst"))
            .and_then(|v| v.as_array())
            .expect("string list");
        assert_eq!(string_list[0], serde_json::json!("str"));
        assert_eq!(string_list[1], serde_json::json!(2));
        assert_eq!(string_list[2], serde_json::json!("YmVpamluZw"));
        assert_eq!(string_list[3], serde_json::json!("c2hhbmdoYWk"));
    }

    #[test]
    fn merge_intermediate_payload_recovers_values() {
        let mut left = DictMergeState::new(MemTracker::new_root("dict-left-test"));
        left.set_threshold_from_i64(255).unwrap();
        left.insert_value("beijing").unwrap();
        let raw = serialize_intermediate_state(&left).expect("serialize");

        let mut right = DictMergeState::new(MemTracker::new_root("dict-right-test"));
        right.set_threshold_from_i64(255).unwrap();
        merge_serialized_state(&mut right, &raw).expect("merge");

        assert!(
            right
                .values
                .iter()
                .any(|value| value.as_slice() == b"beijing")
        );
    }
}

#[cfg(test)]
mod typed_dictionary_tests {
    use super::*;
    use crate::runtime::fragment::ExecutionFailureCause;
    use novarocks_failure_contract::RepresentationBoundary;
    #[test]
    fn threshold_input_and_internal_state_refusals_have_distinct_owners_and_do_not_mutate() {
        let mut state = DictMergeState::new(MemTracker::new_root("dictionary-threshold-source"));
        state.set_threshold_from_i64(3).unwrap();
        for value in [-1, i64::from(i32::MAX) + 1] {
            let input = state.set_threshold_from_i64(value).unwrap_err();
            assert_eq!(input.failure_fact().class(), FailureClass::Data);
            assert_eq!(
                input.failure_fact().reason(),
                ExecutionReason::DictMergeThreshold.reason()
            );
            assert_eq!(state.threshold(), 3);
            let internal = state.set_threshold(value, true).unwrap_err();
            assert_eq!(internal.failure_fact().class(), FailureClass::Invariant);
            assert_eq!(
                internal.failure_fact().reason(),
                ExecutionReason::DictMergeStateThreshold.reason()
            );
            assert_eq!(state.threshold(), 3);
            let ExecutionFailureCause::Domain(shared) = input.cause() else {
                panic!("threshold cause erased")
            };
            let original = shared.downcast_ref::<DictMergeThresholdFailure>().unwrap();
            assert_eq!(original.value, value);
            assert_eq!(original.source.is_some(), value > i64::from(i32::MAX));
        }
        state.set_threshold_from_i64(i64::from(i32::MAX)).unwrap();
        assert_eq!(state.max_values_allowed(), i32::MAX as usize + 1);
        let error = build_fake_values(i32::MAX).unwrap_err();
        let repr = error.failure_fact().representation().unwrap();
        assert_eq!(repr.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(repr.required, Some(i32::MAX as u64 + 1));
        assert_eq!(repr.extent_limit, Some(i32::MAX as u64));
    }
    #[test]
    fn actual_decoders_keep_json_base64_and_utf8_causes_without_the_input_owner() {
        let mut state = DictMergeState::new(MemTracker::new_root("dictionary-decode-source"));
        let input = Arc::new("{".to_owned());
        let weak = Arc::downgrade(&input);
        let error = merge_serialized_state(&mut state, &input).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::DictMergeJson.reason()
        );
        let ExecutionFailureCause::Domain(shared) = error.cause() else {
            panic!("JSON cause erased")
        };
        assert!(matches!(
            shared.downcast_ref::<DictMergeFailure>(),
            Some(DictMergeFailure::Json(_))
        ));
        let error = decode_base64_no_pad("%invalid%").unwrap_err();
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::DictMergeBase64.reason()
        );
        let error = decode_base64_no_pad("/w").unwrap_err();
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::DictMergeUtf8.reason()
        );
        assert!(state.values.is_empty());
    }
    #[test]
    fn actual_payload_extent_distinguishes_one_large_payload_and_multiple_small_payloads() {
        for binary in [false, true] {
            let mut batch = DictPayloadExtent::default();
            batch.append(i32::MAX as usize, binary).unwrap();
            let error = batch.append(1, binary).unwrap_err();
            let repr = error.failure_fact().representation().unwrap();
            assert_eq!(repr.boundary, RepresentationBoundary::SingleOutput);
            assert_eq!(repr.required, Some(i32::MAX as u64 + 1));
            let error = DictPayloadExtent::default()
                .append(i32::MAX as usize + 1, binary)
                .unwrap_err();
            let repr = error.failure_fact().representation().unwrap();
            assert_eq!(repr.boundary, RepresentationBoundary::MinimumUnit);
            assert_eq!(repr.layout.code.get(), if binary { 24 } else { 23 });
        }
        assert_eq!(dict_payload_count(i32::MAX as usize).unwrap(), i32::MAX);
    }
}
