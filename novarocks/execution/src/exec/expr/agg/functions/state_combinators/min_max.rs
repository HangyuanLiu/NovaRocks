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

//! Minimum/maximum state combinator aggregate functions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, Int8Array, StructArray};
use arrow::datatypes::DataType;

use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::expr::agg::{
    AggregateAllocator, AggregateHashMap, AggregateVec, RetainedMemoryPolicy, aggregate_bytes,
    aggregate_hash_map,
};
use crate::exec::mv::state_codec::{
    MultisetEntry, encode_multiset, visit_key_bytes_at, visit_multiset_with_key_type,
};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};

pub(in crate::exec::expr::agg::functions) struct MinMaxStateAgg;
pub(in crate::exec::expr::agg::functions) struct MinMaxStateSignedAgg;

struct MinMaxState {
    allocator: AggregateAllocator,
    counts: AggregateHashMap<AggregateVec<u8>, i64>,
}

impl MinMaxState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            counts: aggregate_hash_map(allocator.clone()),
            allocator,
        }
    }
}

impl AggregateFunction for MinMaxStateAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        build_min_max_state_spec(func, input_type, input_is_intermediate, false)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        min_max_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        build_min_max_state_input_view(min_max_name_for_kind(&spec.kind), spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        build_min_max_state_merge_view(min_max_name_for_kind(&spec.kind), array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked min/max state requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        init_min_max_state(ptr, tracker)
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        drop_min_max_state(ptr);
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        update_min_max_state(
            min_max_name_for_kind(&spec.kind),
            spec,
            offset,
            state_ptrs,
            input,
        )
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        merge_min_max_state(
            min_max_name_for_kind(&spec.kind),
            spec,
            offset,
            state_ptrs,
            input,
        )
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        build_min_max_state_array(spec, offset, group_states)
    }
}

impl AggregateFunction for MinMaxStateSignedAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        build_min_max_state_spec(func, input_type, input_is_intermediate, true)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        min_max_state_layout_for(kind)
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        build_min_max_state_input_view(min_max_name_for_kind(&spec.kind), spec, array)
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        build_min_max_state_merge_view(min_max_name_for_kind(&spec.kind), array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked min/max state requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        init_min_max_state(ptr, tracker)
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        drop_min_max_state(ptr);
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        update_min_max_state(
            min_max_name_for_kind(&spec.kind),
            spec,
            offset,
            state_ptrs,
            input,
        )
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        merge_min_max_state(
            min_max_name_for_kind(&spec.kind),
            spec,
            offset,
            state_ptrs,
            input,
        )
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        build_min_max_state_array(spec, offset, group_states)
    }
}

fn build_min_max_state_spec(
    func: &AggFunction,
    input_type: Option<&DataType>,
    input_is_intermediate: bool,
    signed: bool,
) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
    let name = min_max_name(func, signed);
    let logical_input_type;
    if input_is_intermediate {
        logical_input_type = func
            .types
            .as_ref()
            .and_then(|t| t.input_arg_type.as_ref())
            .cloned()
            .ok_or_else(|| {
                crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                    "{name} merge requires original logical input type"
                ))
            })?;
        validate_key_type(name, merge_key_type(&logical_input_type))?;
    } else {
        let input_type = input_type.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                "{name} input type missing"
            ))
        })?;
        if signed {
            validate_signed_input_type(name, input_type)?;
        } else {
            validate_key_type(name, input_type)?;
        }
        logical_input_type = input_type.clone();
    }

    Ok(AggSpec {
        kind: min_max_kind_for_name(name),
        output_type: DataType::Binary,
        intermediate_type: DataType::Binary,
        input_arg_type: Some(logical_input_type),
        count_all: false,
    })
}

fn min_max_kind_for_name(name: &str) -> AggKind {
    match name {
        "max_state" => AggKind::MaxState,
        "min_state_signed" => AggKind::MinStateSigned,
        "max_state_signed" => AggKind::MaxStateSigned,
        _ => AggKind::MinState,
    }
}

fn min_max_name_for_kind(kind: &AggKind) -> &'static str {
    match kind {
        AggKind::MinState => "min_state",
        AggKind::MaxState => "max_state",
        AggKind::MinStateSigned => "min_state_signed",
        AggKind::MaxStateSigned => "max_state_signed",
        other => unreachable!("unexpected kind for min/max state: {:?}", other),
    }
}

fn min_max_name(func: &AggFunction, signed: bool) -> &str {
    let base = func
        .name
        .split_once('|')
        .map(|(base, _)| base)
        .unwrap_or(func.name.as_str());
    match base {
        "max_state" => "max_state",
        "max_state_signed" => "max_state_signed",
        _ if signed => "min_state_signed",
        _ => "min_state",
    }
}

fn validate_signed_input_type(
    name: &str,
    input_type: &DataType,
) -> crate::runtime::fragment::ExecutionResult<()> {
    let DataType::Struct(fields) = input_type else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!(
                "{name} expects struct input (value, change_op), got {:?}",
                input_type
            ),
        ));
    };
    if fields.len() != 2 {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!(
                "{name} expects 2 arguments, got struct with {} fields",
                fields.len()
            ),
        ));
    }
    validate_key_type(name, fields[0].data_type())?;
    if fields[1].data_type() != &DataType::Int8 {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!(
                "{name} change_op must be Int8, got {:?}",
                fields[1].data_type()
            ),
        ));
    }
    Ok(())
}

fn validate_key_type(
    name: &str,
    data_type: &DataType,
) -> crate::runtime::fragment::ExecutionResult<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Date32
        | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
        | DataType::Utf8
        | DataType::LargeUtf8 => Ok(()),
        other => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("{name} unsupported key type {other:?}"),
        )),
    }
}

fn min_max_state_layout_for(kind: &AggKind) -> (usize, usize) {
    match kind {
        AggKind::MinState
        | AggKind::MaxState
        | AggKind::MinStateSigned
        | AggKind::MaxStateSigned => (
            std::mem::size_of::<MinMaxState>(),
            std::mem::align_of::<MinMaxState>(),
        ),
        other => unreachable!("unexpected kind for min/max state: {:?}", other),
    }
}

fn build_min_max_state_input_view<'a>(
    name: &str,
    spec: &AggSpec,
    array: &'a Option<ArrayRef>,
) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
    let arr = array.as_ref().ok_or_else(|| {
        crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
            "{name} input missing"
        ))
    })?;
    match spec.kind {
        AggKind::MinState | AggKind::MaxState => validate_key_type(name, arr.data_type())?,
        AggKind::MinStateSigned | AggKind::MaxStateSigned => {
            validate_signed_input_type(name, arr.data_type())?
        }
        _ => {
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                format_args!("{name} input kind mismatch"),
            ));
        }
    }
    Ok(AggInputView::Any(arr))
}

fn build_min_max_state_merge_view<'a>(
    name: &str,
    array: &'a Option<ArrayRef>,
) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
    let arr = array.as_ref().ok_or_else(|| {
        crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
            "{name} merge input missing"
        ))
    })?;
    let binary = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
            "{name} merge input must be BinaryArray"
        ))
    })?;
    Ok(AggInputView::Binary(binary))
}

fn init_min_max_state(
    ptr: *mut u8,
    tracker: Option<Arc<MemTracker>>,
) -> crate::runtime::fragment::ExecutionResult<()> {
    let tracker = tracker.ok_or_else(|| {
        crate::exec::expr::agg::functions::aggregate_binding_contract(
            "allocation-tracked min/max state requires an aggregate memory tracker",
        )
    })?;
    unsafe {
        std::ptr::write(ptr as *mut MinMaxState, MinMaxState::new(tracker));
    }
    Ok(())
}

fn drop_min_max_state(ptr: *mut u8) {
    unsafe {
        std::ptr::drop_in_place(ptr as *mut MinMaxState);
    }
}

fn state_slot(base: AggStatePtr, offset: usize) -> *mut MinMaxState {
    unsafe { (base as *mut u8).add(offset) as *mut MinMaxState }
}

fn update_min_max_state(
    name: &str,
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
    match spec.kind {
        AggKind::MinState | AggKind::MaxState => {
            update_min_max_state_unsigned(name, offset, state_ptrs, input)
        }
        AggKind::MinStateSigned | AggKind::MaxStateSigned => {
            update_min_max_state_signed(name, offset, state_ptrs, input)
        }
        _ => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("{name} update kind mismatch"),
        )),
    }
}

fn update_min_max_state_unsigned(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
    if state_ptrs.is_empty() {
        return Ok(());
    }
    let AggInputView::Any(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("{name} batch input type mismatch"),
        ));
    };
    let mut staged = new_staging_map(state_ptrs, offset, name)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        let state = staged_state(&mut staged, base, offset, name)?;
        let key_bytes = tracked_key_at(&state.allocator, array, row, name)?;
        add_count_to_map(&mut state.counts, key_bytes, 1, name, &state.allocator)?;
    }
    commit_staged(staged);
    Ok(())
}

fn update_min_max_state_signed(
    name: &str,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
    if state_ptrs.is_empty() {
        return Ok(());
    }
    let (struct_arr, value_arr, op_arr) = signed_parts(name, input)?;
    let mut staged = new_staging_map(state_ptrs, offset, name)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if struct_arr.is_null(row) || value_arr.is_null(row) {
            continue;
        }
        let delta = match signed_delta(name, op_arr, row)? {
            Some(delta) => delta,
            None => continue,
        };
        let state = staged_state(&mut staged, base, offset, name)?;
        let key_bytes = tracked_key_at(&state.allocator, value_arr, row, name)?;
        add_count_to_map(&mut state.counts, key_bytes, delta, name, &state.allocator)?;
    }
    commit_staged(staged);
    Ok(())
}

fn signed_parts<'a>(
    name: &str,
    input: &'a AggInputView<'a>,
) -> Result<
    (&'a StructArray, &'a ArrayRef, &'a Int8Array),
    crate::runtime::fragment::ExecutionFailure,
> {
    let AggInputView::Any(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("{name} batch input type mismatch"),
        ));
    };
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
                "{name} expects struct input"
            ))
        })?;
    if struct_arr.num_columns() != 2 {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!(
                "{name} expects 2 arguments, got {}",
                struct_arr.num_columns()
            ),
        ));
    }
    let value_arr = struct_arr.column(0);
    let op_arr_ref = struct_arr.column(1);
    let op_arr = op_arr_ref
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
                "{name} change_op must be Int8, got {:?}",
                op_arr_ref.data_type()
            ))
        })?;
    Ok((struct_arr, value_arr, op_arr))
}

fn signed_delta(
    name: &str,
    op_arr: &Int8Array,
    row: usize,
) -> Result<Option<i64>, crate::runtime::fragment::ExecutionFailure> {
    if op_arr.is_null(row) {
        return Ok(None);
    }
    match op_arr.value(row) {
        CHANGE_OP_INSERT => Ok(Some(1)),
        CHANGE_OP_DELETE => Ok(Some(-1)),
        other => Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("unknown {name} change_op: {other}"),
        )),
    }
}

fn staged_state<'a>(
    staged: &'a mut AggregateHashMap<usize, MinMaxState>,
    base: AggStatePtr,
    offset: usize,
    _context: &str,
) -> Result<&'a mut MinMaxState, crate::runtime::fragment::ExecutionFailure> {
    let ptr = state_slot(base, offset);
    if !staged.contains_key(&(ptr as usize)) {
        let copy = clone_state(unsafe { &*ptr })?;
        staged.try_reserve(1).map_err(|source| {
            copy.allocator
                .allocation_error("reserve min/max staging state", source)
        })?;
        staged.insert(ptr as usize, copy);
    }
    Ok(staged
        .get_mut(&(ptr as usize))
        .expect("staging state inserted above"))
}

fn new_staging_map(
    state_ptrs: &[AggStatePtr],
    offset: usize,
    context: &str,
) -> Result<AggregateHashMap<usize, MinMaxState>, crate::runtime::fragment::ExecutionFailure> {
    let first = state_ptrs.first().ok_or_else(|| {
        crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
            "{context} staging requires at least one state"
        ))
    })?;
    let state = unsafe { &*state_slot(*first, offset) };
    Ok(aggregate_hash_map(state.allocator.clone()))
}

fn clone_state(
    state: &MinMaxState,
) -> Result<MinMaxState, crate::runtime::fragment::ExecutionFailure> {
    let mut counts = aggregate_hash_map(state.allocator.clone());
    counts.try_reserve(state.counts.len()).map_err(|source| {
        state
            .allocator
            .allocation_error("reserve min/max state clone", source)
    })?;
    for (key, count) in &state.counts {
        counts.insert(aggregate_bytes(state.allocator.clone(), key)?, *count);
    }
    Ok(MinMaxState {
        allocator: state.allocator.clone(),
        counts,
    })
}

#[derive(Debug)]
struct MinMaxKeyFailure {
    aggregate: novarocks_failure_contract::BoundedDetail,
    source: crate::exec::mv::state_codec::StateCodecFailure,
}
impl std::fmt::Display for MinMaxKeyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.aggregate, self.source)
    }
}
impl std::error::Error for MinMaxKeyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for MinMaxKeyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
fn tracked_key_at(
    allocator: &AggregateAllocator,
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<AggregateVec<u8>, crate::runtime::fragment::ExecutionFailure> {
    let codec_failure = |source| {
        crate::runtime::fragment::ExecutionFailure::from_domain(MinMaxKeyFailure {
            aggregate: novarocks_failure_contract::BoundedDetail::truncating(context),
            source,
        })
    };
    let mut len = Some(0usize);
    visit_key_bytes_at(array, row, |bytes| {
        len = len.and_then(|current| current.checked_add(bytes.len()));
    })
    .map_err(&codec_failure)?;
    let len = len.ok_or_else(|| {
        crate::runtime::fragment::ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    let mut output = AggregateVec::new_in(allocator.clone());
    output
        .try_reserve_exact(len)
        .map_err(|source| allocator.allocation_error("reserve min/max canonical key", source))?;
    visit_key_bytes_at(array, row, |bytes| output.extend_from_slice(bytes))
        .map_err(codec_failure)?;
    Ok(output)
}

fn add_count_to_map(
    counts: &mut AggregateHashMap<AggregateVec<u8>, i64>,
    key_bytes: AggregateVec<u8>,
    delta: i64,
    context: &str,
    allocator: &AggregateAllocator,
) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
    let current = *counts.get(&key_bytes).unwrap_or(&0);
    let next = super::failure::add_count(current, delta, context)?;
    if !counts.contains_key(&key_bytes) {
        counts.try_reserve(1).map_err(|source| {
            allocator.allocation_error("reserve min/max multiset entry", source)
        })?;
    }
    counts.insert(key_bytes, next);
    Ok(())
}

fn commit_staged(staged: AggregateHashMap<usize, MinMaxState>) {
    for (ptr, state) in staged {
        unsafe {
            *(ptr as *mut MinMaxState) = state;
        }
    }
}

fn merge_min_max_state(
    name: &str,
    spec: &AggSpec,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
    if state_ptrs.is_empty() {
        return Ok(());
    }
    let AggInputView::Binary(array) = input else {
        return Err(crate::exec::expr::agg::views::aggregate_input_contract(
            format_args!("{name} merge input type mismatch"),
        ));
    };
    let key_type = spec
        .input_arg_type
        .as_ref()
        .map(merge_key_type)
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
                "{name} merge requires original logical input type"
            ))
        })?;
    validate_key_type(name, key_type)?;

    let mut staged = new_staging_map(state_ptrs, offset, name)?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if array.is_null(row) {
            continue;
        }
        let state = staged_state(&mut staged, base, offset, name)?;
        merge_entries_into_state(state, array.value(row), key_type, name)?;
    }

    commit_staged(staged);
    Ok(())
}

fn merge_key_type(data_type: &DataType) -> &DataType {
    if let DataType::Struct(fields) = data_type
        && fields.len() == 2
    {
        return fields[0].data_type();
    }
    data_type
}

fn build_min_max_state_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, crate::runtime::fragment::ExecutionFailure> {
    let key_type = spec
        .input_arg_type
        .as_ref()
        .map(merge_key_type)
        .ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(format_args!(
                "{} build_array requires original logical input type",
                min_max_name_for_kind(&spec.kind)
            ))
        })?;
    validate_key_type(min_max_name_for_kind(&spec.kind), key_type)?;
    let mut builder = BinaryBuilder::new();
    let mut extent = super::failure::BinaryOutput::default();
    for &base in group_states {
        let state = unsafe { &*state_slot(base, offset) };
        let mut entries: Vec<_> = state
            .counts
            .iter()
            .filter_map(|(key_bytes, &count)| {
                (count != 0).then_some(MultisetEntry {
                    key_bytes: key_bytes.to_vec(),
                    count,
                })
            })
            .collect();
        entries.sort_by(|left, right| left.key_bytes.cmp(&right.key_bytes));
        let bytes = encode_multiset(&entries, key_type)?;
        extent.append(bytes.len())?;
        builder.append_value(bytes);
    }
    Ok(Arc::new(builder.finish()))
}

fn merge_entries_into_state(
    state: &mut MinMaxState,
    bytes: &[u8],
    key_type: &DataType,
    name: &str,
) -> crate::runtime::fragment::ExecutionResult<()> {
    visit_multiset_with_key_type(bytes, key_type, |key, count| {
        let key = aggregate_bytes(state.allocator.clone(), key)?;
        add_count_to_map(&mut state.counts, key, count, name, &state.allocator)
    })
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
    use crate::exec::mv::state_codec::{
        MultisetEntry, decode_multiset_with_key_type, encode_multiset,
    };
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    use super::super::super::{AggInputView, AggKind, AggSpec, AggStatePtr};

    fn agg_func(name: &str, input_arg_type: Option<DataType>) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Binary),
                input_arg_type,
            }),
            ..Default::default()
        }
    }

    fn agg_func_with_signature(
        name: &str,
        output_type: DataType,
        intermediate_type: DataType,
        input_arg_type: Option<DataType>,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(output_type),
                input_arg_type,
            }),
            ..Default::default()
        }
    }

    fn build_spec(name: &str, input_type: &DataType) -> AggSpec {
        super::super::super::build_spec_from_type(
            &agg_func(name, Some(input_type.clone())),
            Some(input_type),
            false,
        )
        .unwrap()
    }

    fn build_merge_spec(name: &str, key_type: DataType) -> AggSpec {
        super::super::super::build_spec_from_type(
            &agg_func(name, Some(key_type)),
            Some(&DataType::Binary),
            true,
        )
        .unwrap()
    }

    fn signed_input_type(value_type: DataType) -> DataType {
        DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("value", value_type, true)),
            Arc::new(Field::new("op", DataType::Int8, true)),
        ]))
    }

    struct StateCell {
        spec: AggSpec,
        cell: Box<MaybeUninit<super::MinMaxState>>,
    }

    impl StateCell {
        fn new(spec: AggSpec) -> Self {
            let mut cell = Box::new(MaybeUninit::<super::MinMaxState>::uninit());
            let agg = super::super::super::resolve_by_kind(&spec.kind);
            agg.init_state_with_tracker(
                &spec,
                cell.as_mut_ptr() as *mut u8,
                Some(crate::runtime::mem_tracker::MemTracker::new_root(
                    "min-max-state-test",
                )),
            )
            .unwrap();
            Self { spec, cell }
        }

        fn ptr(&mut self) -> AggStatePtr {
            self.cell.as_mut_ptr() as AggStatePtr
        }

        fn update(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_input_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn try_update(
            &mut self,
            input: ArrayRef,
        ) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_input_view(&self.spec, &input_slot)?;
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &view)
        }

        fn try_update_view(
            &mut self,
            input: super::super::super::AggInputView<'_>,
            rows: usize,
        ) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::update_batch(&self.spec, 0, &state_ptrs, &input)
        }

        fn merge(&mut self, input: ArrayRef) {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_merge_view(&self.spec, &input_slot).unwrap();
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::merge_batch(&self.spec, 0, &state_ptrs, &view).unwrap();
        }

        fn try_merge(
            &mut self,
            input: ArrayRef,
        ) -> Result<(), crate::runtime::fragment::ExecutionFailure> {
            let rows = input.len();
            let input_slot = Some(input);
            let view = super::super::super::build_merge_view(&self.spec, &input_slot)?;
            let ptr = self.ptr();
            let state_ptrs = vec![ptr; rows];
            super::super::super::merge_batch(&self.spec, 0, &state_ptrs, &view)
        }

        fn final_bytes(&mut self) -> Vec<u8> {
            let ptr = self.ptr();
            let out = super::super::super::build_array(&self.spec, 0, &[ptr], false).unwrap();
            let binary = out.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(binary.len(), 1);
            binary.value(0).to_vec()
        }
    }

    impl Drop for StateCell {
        fn drop(&mut self) {
            super::super::super::drop_state(&self.spec, self.cell.as_mut_ptr() as *mut u8);
        }
    }

    fn decode_counts(bytes: &[u8], key_type: &DataType) -> Vec<i64> {
        decode_multiset_with_key_type(bytes, key_type)
            .unwrap()
            .into_iter()
            .map(|entry| entry.count)
            .collect()
    }

    fn decode_entries(bytes: &[u8], key_type: &DataType) -> Vec<MultisetEntry> {
        decode_multiset_with_key_type(bytes, key_type).unwrap()
    }

    fn signed_i64_input(
        values: Vec<Option<i64>>,
        ops: Vec<Option<i8>>,
        struct_valid: Option<Vec<bool>>,
    ) -> ArrayRef {
        let value_arr = Arc::new(Int64Array::from(values)) as ArrayRef;
        let op_arr = Arc::new(Int8Array::from(ops)) as ArrayRef;
        Arc::new(StructArray::new(
            Fields::from(vec![
                Arc::new(Field::new("value", DataType::Int64, true)),
                Arc::new(Field::new("op", DataType::Int8, true)),
            ]),
            vec![value_arr, op_arr],
            struct_valid.map(NullBuffer::from),
        )) as ArrayRef
    }

    #[test]
    fn min_state_collects_multiset_entries() {
        let spec = build_spec("min_state", &DataType::Int64);
        let mut cell = StateCell::new(spec);
        cell.update(Arc::new(Int64Array::from(vec![
            Some(5),
            Some(5),
            Some(3),
            None,
            Some(5),
        ])));

        let entries = decode_multiset_with_key_type(&cell.final_bytes(), &DataType::Int64).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries.iter().map(|e| e.count).collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn min_state_and_max_state_produce_identical_bytes() {
        let input = Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(3)])) as ArrayRef;
        let mut min_cell = StateCell::new(build_spec("min_state", &DataType::Int64));
        let mut max_cell = StateCell::new(build_spec("max_state", &DataType::Int64));

        min_cell.update(input.clone());
        max_cell.update(input);

        assert_eq!(min_cell.final_bytes(), max_cell.final_bytes());
    }

    #[test]
    fn min_state_signed_handles_delete() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("min_state_signed", &input_type));

        cell.update(signed_i64_input(
            vec![Some(5), Some(5)],
            vec![Some(CHANGE_OP_INSERT), Some(CHANGE_OP_DELETE)],
            None,
        ));

        assert!(cell.final_bytes().is_empty());
    }

    #[test]
    fn max_state_signed_same_bytes_as_min_state_signed() {
        let input_type = signed_input_type(DataType::Int64);
        let input = signed_i64_input(
            vec![Some(5), Some(7)],
            vec![Some(CHANGE_OP_INSERT), Some(CHANGE_OP_DELETE)],
            None,
        );
        let mut min_cell = StateCell::new(build_spec("min_state_signed", &input_type));
        let mut max_cell = StateCell::new(build_spec("max_state_signed", &input_type));

        min_cell.update(input.clone());
        max_cell.update(input);

        assert_eq!(min_cell.final_bytes(), max_cell.final_bytes());
    }

    #[test]
    fn min_state_signed_delete_only_preserves_negative_delta() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("min_state_signed", &input_type));

        cell.update(signed_i64_input(
            vec![Some(5)],
            vec![Some(CHANGE_OP_DELETE)],
            None,
        ));

        assert_eq!(
            decode_entries(&cell.final_bytes(), &DataType::Int64),
            vec![MultisetEntry {
                key_bytes: 5i64.to_le_bytes().to_vec(),
                count: -1,
            }]
        );
    }

    #[test]
    fn max_state_signed_delete_only_matches_min_state_signed_bytes() {
        let input_type = signed_input_type(DataType::Int64);
        let input = signed_i64_input(vec![Some(5)], vec![Some(CHANGE_OP_DELETE)], None);
        let mut min_cell = StateCell::new(build_spec("min_state_signed", &input_type));
        let mut max_cell = StateCell::new(build_spec("max_state_signed", &input_type));

        min_cell.update(input.clone());
        max_cell.update(input);

        assert_eq!(min_cell.final_bytes(), max_cell.final_bytes());
        assert_eq!(
            decode_entries(&max_cell.final_bytes(), &DataType::Int64),
            vec![MultisetEntry {
                key_bytes: 5i64.to_le_bytes().to_vec(),
                count: -1,
            }]
        );
    }

    #[test]
    fn min_state_signed_skips_struct_null_value_null_and_null_op_zero() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("min_state_signed", &input_type));

        cell.update(signed_i64_input(
            vec![Some(1), Some(2), None, Some(4)],
            vec![
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_INSERT),
                Some(CHANGE_OP_INSERT),
                None,
            ],
            Some(vec![true, false, true, true]),
        ));

        assert_eq!(
            decode_counts(&cell.final_bytes(), &DataType::Int64),
            vec![1]
        );
    }

    #[test]
    fn min_state_signed_unknown_op_errors() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("min_state_signed", &input_type));

        let err = cell
            .try_update(signed_i64_input(vec![Some(1)], vec![Some(7)], None))
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("unknown min_state_signed change_op")
        );
    }

    #[test]
    fn max_state_runtime_errors_use_max_name() {
        let mut cell = StateCell::new(build_spec("max_state", &DataType::Int64));
        let input = Arc::new(BinaryArray::from(vec![Some(&b"x"[..])])) as ArrayRef;

        let err = cell
            .try_update_view(AggInputView::Any(&input), input.len())
            .unwrap_err();

        assert!(err.to_string().contains("max_state"));
        assert!(!err.to_string().contains("min_state"));
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(err.failure_fact().class(), FailureClass::Invariant);
        let mut source: &dyn std::error::Error = &err;
        loop {
            if let Some(original) =
                source.downcast_ref::<crate::exec::mv::state_codec::StateCodecFailure>()
            {
                assert!(matches!(
                    original,
                    crate::exec::mv::state_codec::StateCodecFailure::CarrierContract(_)
                ));
                break;
            }
            source = source.source().expect("the original key codec author");
        }
    }

    #[test]
    fn max_state_signed_runtime_errors_use_max_signed_name() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("max_state_signed", &input_type));

        let err = cell
            .try_update(signed_i64_input(vec![Some(1)], vec![Some(7)], None))
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("unknown max_state_signed change_op")
        );
        assert!(!err.to_string().contains("min_state_signed"));
    }

    #[test]
    fn min_state_merge_decodes_binary_states_and_drops_canceled_entries() {
        let spec = build_merge_spec("min_state", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let input = Arc::new(BinaryArray::from(vec![
            Some(
                encode_multiset(
                    &[
                        MultisetEntry {
                            key_bytes: 1i64.to_le_bytes().to_vec(),
                            count: 2,
                        },
                        MultisetEntry {
                            key_bytes: 2i64.to_le_bytes().to_vec(),
                            count: 1,
                        },
                    ],
                    &DataType::Int64,
                )
                .unwrap()
                .as_slice(),
            ),
            Some(&[][..]),
            Some(
                encode_multiset(
                    &[MultisetEntry {
                        key_bytes: 1i64.to_le_bytes().to_vec(),
                        count: -2,
                    }],
                    &DataType::Int64,
                )
                .unwrap()
                .as_slice(),
            ),
        ])) as ArrayRef;

        cell.merge(input);

        let entries = decode_multiset_with_key_type(&cell.final_bytes(), &DataType::Int64).unwrap();
        assert_eq!(
            entries,
            vec![MultisetEntry {
                key_bytes: 2i64.to_le_bytes().to_vec(),
                count: 1
            }]
        );
    }

    #[test]
    fn min_state_merge_count_overflow_errors_without_partial_mutation() {
        let spec = build_merge_spec("min_state", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let key = 1i64.to_le_bytes().to_vec();
        let first = encode_multiset(
            &[MultisetEntry {
                key_bytes: key.clone(),
                count: i64::MAX,
            }],
            &DataType::Int64,
        )
        .unwrap();
        let second = encode_multiset(
            &[MultisetEntry {
                key_bytes: key.clone(),
                count: 1,
            }],
            &DataType::Int64,
        )
        .unwrap();

        let err = cell
            .try_merge(Arc::new(BinaryArray::from(vec![
                Some(first.as_slice()),
                Some(second.as_slice()),
            ])))
            .unwrap_err();

        assert!(err.to_string().contains("overflow"));
        assert!(cell.final_bytes().is_empty());
    }

    #[test]
    fn min_state_merge_malformed_after_valid_row_does_not_partially_mutate() {
        let spec = build_merge_spec("min_state", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let valid = encode_multiset(
            &[MultisetEntry {
                key_bytes: 1i64.to_le_bytes().to_vec(),
                count: 1,
            }],
            &DataType::Int64,
        )
        .unwrap();

        let err = cell
            .try_merge(Arc::new(BinaryArray::from(vec![
                Some(valid.as_slice()),
                Some(&[0xff][..]),
            ])))
            .unwrap_err();

        assert!(err.to_string().contains("unsupported version byte"));
        assert!(cell.final_bytes().is_empty());
    }

    #[test]
    fn min_state_update_overflow_errors_without_partial_mutation() {
        let spec = build_merge_spec("min_state", DataType::Int64);
        let mut cell = StateCell::new(spec);
        let key = 1i64.to_le_bytes().to_vec();
        let existing = encode_multiset(
            &[MultisetEntry {
                key_bytes: key,
                count: i64::MAX,
            }],
            &DataType::Int64,
        )
        .unwrap();
        cell.merge(Arc::new(BinaryArray::from(vec![Some(existing.as_slice())])));
        let before = cell.final_bytes();

        let input = Arc::new(Int64Array::from(vec![Some(2), Some(1)])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell.spec, &input_slot).unwrap();
        let ptr = cell.ptr();
        let err = super::super::super::update_batch(&cell.spec, 0, &[ptr, ptr], &view).unwrap_err();

        assert!(err.to_string().contains("overflow"));
        assert_eq!(cell.final_bytes(), before);
    }

    #[test]
    fn min_state_signed_update_error_does_not_partially_mutate_state() {
        let input_type = signed_input_type(DataType::Int64);
        let mut cell = StateCell::new(build_spec("min_state_signed", &input_type));

        let err = cell
            .try_update(signed_i64_input(
                vec![Some(1), Some(2)],
                vec![Some(CHANGE_OP_INSERT), Some(7)],
                None,
            ))
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("unknown min_state_signed change_op")
        );
        assert!(cell.final_bytes().is_empty());
    }

    #[test]
    fn min_state_update_staging_splits_distinct_state_pointers() {
        let spec = build_spec("min_state", &DataType::Int64);
        let mut cell_a = StateCell::new(spec.clone());
        let mut cell_b = StateCell::new(spec);
        let input = Arc::new(Int64Array::from(vec![Some(1), Some(10), Some(1)])) as ArrayRef;
        let input_slot = Some(input);
        let view = super::super::super::build_input_view(&cell_a.spec, &input_slot).unwrap();
        let ptr_a = cell_a.ptr();
        let ptr_b = cell_b.ptr();

        super::super::super::update_batch(&cell_a.spec, 0, &[ptr_a, ptr_b, ptr_a], &view).unwrap();

        assert_eq!(
            decode_entries(&cell_a.final_bytes(), &DataType::Int64),
            vec![MultisetEntry {
                key_bytes: 1i64.to_le_bytes().to_vec(),
                count: 2,
            }]
        );
        assert_eq!(
            decode_entries(&cell_b.final_bytes(), &DataType::Int64),
            vec![MultisetEntry {
                key_bytes: 10i64.to_le_bytes().to_vec(),
                count: 1,
            }]
        );
    }

    #[test]
    fn min_state_bool_key_round_trips_with_key_type() {
        let spec = build_spec("min_state", &DataType::Boolean);
        let mut cell = StateCell::new(spec);

        cell.update(Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ])));

        assert_eq!(
            decode_counts(&cell.final_bytes(), &DataType::Boolean),
            vec![1, 2]
        );
    }

    #[test]
    fn registration_resolves_all_names_to_binary_state() {
        for (name, input_type, expected_kind) in [
            ("min_state", DataType::Int64, AggKind::MinState),
            ("max_state", DataType::Int64, AggKind::MaxState),
            (
                "min_state_signed",
                signed_input_type(DataType::Int64),
                AggKind::MinStateSigned,
            ),
            (
                "max_state_signed",
                signed_input_type(DataType::Int64),
                AggKind::MaxStateSigned,
            ),
        ] {
            let spec = super::super::super::build_spec_from_type(
                &agg_func(name, Some(input_type.clone())),
                Some(&input_type),
                false,
            )
            .unwrap();
            assert!(
                matches!(spec.kind, kind if std::mem::discriminant(&kind) == std::mem::discriminant(&expected_kind))
            );
            assert_eq!(spec.output_type, DataType::Binary);
            assert_eq!(spec.intermediate_type, DataType::Binary);
        }
    }

    #[test]
    fn strict_binary_signature_guard_rejects_utf8_output_for_min_max_state() {
        let func = agg_func_with_signature(
            "min_state",
            DataType::Utf8,
            DataType::Binary,
            Some(DataType::Int64),
        );

        let err = crate::exec::expr::agg::spec::build_spec_from_type(
            &func,
            Some(&DataType::Int64),
            false,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("state combinator output_type must be Binary")
        );
    }

    #[test]
    fn min_state_rejects_unsupported_binary_key() {
        let err = super::super::super::build_spec_from_type(
            &agg_func("min_state", Some(DataType::Binary)),
            Some(&DataType::Binary),
            false,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("min_state unsupported key type Binary")
        );
    }

    #[test]
    fn min_state_utf8_key_round_trips_with_key_type() {
        let spec = build_spec("min_state", &DataType::Utf8);
        let mut cell = StateCell::new(spec);

        cell.update(Arc::new(StringArray::from(vec![
            Some("b"),
            Some("a"),
            Some("b"),
        ])));

        assert_eq!(
            decode_counts(&cell.final_bytes(), &DataType::Utf8),
            vec![1, 2]
        );
    }
}

#[cfg(test)]
mod typed_merge_source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn multiset_merge_map_refusal_is_resource_not_numeric_and_releases_the_temporary_key() {
        let tracker = MemTracker::new_root("overflow invalid encoding cancelled");
        tracker.install_limit_once(8).unwrap();
        let mut state = MinMaxState::new(tracker.clone());
        let bytes = encode_multiset(
            &[MultisetEntry {
                key_bytes: 7i64.to_le_bytes().to_vec(),
                count: 1,
            }],
            &DataType::Int64,
        )
        .unwrap();
        let error = merge_entries_into_state(&mut state, &bytes, &DataType::Int64, "min_state")
            .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        // The container exposes its attempted Layout, but its erased allocator
        // refusal has no original tracker-limit fact. Do not reconstruct one.
        assert_eq!(resource.limit, None);
        assert_eq!(fact.reason().code(), 169);
        let mut source = std::error::Error::source(&error);
        let mut actual_map_refusal = false;
        while let Some(cause) = source {
            actual_map_refusal |= cause
                .is::<novarocks_functions::allocation_failure::AllocationRefusal<
                    hashbrown::TryReserveError,
                >>();
            source = cause.source();
        }
        assert!(actual_map_refusal);
        assert!(resource.requested.unwrap() > 8);
        assert!(state.counts.is_empty());
        assert_eq!(tracker.current(), 0);
        let weak = Arc::downgrade(&tracker);
        drop(state);
        assert_eq!(tracker.current(), 0);
        drop(tracker);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact(), fact);
    }
}
