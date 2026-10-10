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
//! Original non-DISTINCT COUNT computation, shared by selected and legacy shells.
//! Physical root NULL behavior and ordinary signed arithmetic are intentional.
//! These operations do not grant memory or infer an invocation budget.
use arrow_array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, builder::Int64Builder,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum CountObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Clone, Copy, Debug)]
pub enum CountNullRule {
    AggregateRootFast,
    AnalyticRoot,
}
#[derive(Clone, Copy)]
pub struct CountValue<'a> {
    pub array: &'a dyn Array,
    pub row: usize,
}
#[derive(Clone, Copy)]
pub enum CountMergeArray<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

pub const fn initial_state() -> i64 {
    0
}
/// The original aggregate skips its bitmap entirely when null_count is zero.
/// The analytic kernel always asks Array::is_null; neither decodes logical NULL.
pub fn contributes(value: CountValue<'_>, rule: CountNullRule) -> bool {
    match rule {
        CountNullRule::AggregateRootFast if value.array.null_count() == 0 => true,
        _ => !value.array.is_null(value.row),
    }
}
/// Ordinary += preserves the original debug panic and release wrapping.
pub fn add_original(state: &mut i64, contribution: i64) {
    *state += contribution;
}
pub fn add_observed<E>(
    state: &mut i64,
    contribution: i64,
    observe: &mut dyn FnMut(CountObservation) -> Result<(), E>,
) -> Result<(), E> {
    observe(CountObservation::Step)?;
    add_original(state, contribution);
    Ok(())
}
/// # Safety
/// Every base has an initialized, aligned i64 COUNT slot at offset and remains
/// live and exclusively mutable during its occurrence. Aliased group bases are
/// processed sequentially, exactly as the original legacy aggregate does.
pub unsafe fn update_legacy_batch(
    count_all: bool,
    array: Option<&dyn Array>,
    offset: usize,
    bases: &[usize],
) {
    if array.is_none() || count_all || array.is_some_and(|a| a.null_count() == 0) {
        for &base in bases {
            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
            add_original(slot, 1);
        }
    } else {
        let array = array.unwrap();
        for (row, &base) in bases.iter().enumerate() {
            if contributes(CountValue { array, row }, CountNullRule::AggregateRootFast) {
                let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                add_original(slot, 1);
            }
        }
    }
}
/// # Safety
/// Same initialized and exclusive state-slot contract as update_legacy_batch.
/// Each source row used by the original non-null path must exist.
pub unsafe fn merge_legacy_batch(source: CountMergeArray<'_>, offset: usize, bases: &[usize]) {
    macro_rules! apply {
        ($a:expr) => {{
            let a = $a;
            let vals = a.values();
            if a.null_count() == 0 {
                for (row, &base) in bases.iter().enumerate() {
                    let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                    add_original(slot, vals[row] as i64);
                }
            } else {
                for (row, &base) in bases.iter().enumerate() {
                    if !a.is_null(row) {
                        let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                        add_original(slot, vals[row] as i64);
                    }
                }
            }
        }};
    }
    match source {
        CountMergeArray::Int8(a) => apply!(a),
        CountMergeArray::Int16(a) => apply!(a),
        CountMergeArray::Int32(a) => apply!(a),
        CountMergeArray::Int64(a) => apply!(a),
    }
}
pub fn build_state_array_observed<E>(
    states: impl Iterator<Item = Result<i64, E>>,
    observe: &mut dyn FnMut(CountObservation) -> Result<(), E>,
) -> Result<ArrayRef, E> {
    observe(CountObservation::OpaqueBoundary)?;
    let mut builder = Int64Builder::new();
    observe(CountObservation::OpaqueBoundary)?;
    for value in states {
        let value = value?;
        observe(CountObservation::Step)?;
        builder.append_value(value);
        observe(CountObservation::Step)?;
    }
    observe(CountObservation::OpaqueBoundary)?;
    let output = Arc::new(builder.finish()) as ArrayRef;
    observe(CountObservation::OpaqueBoundary)?;
    Ok(output)
}
/// Original analytic count for one partition. The host supplies original frame
/// ranges and already-evaluated source addressing; this core looks up no tree.
/// The original frame iterator zip truncation and ordinary subtraction remain.
pub fn window_partition_observed<'a, E>(
    source: Option<&dyn Fn(usize) -> CountValue<'a>>,
    start: usize,
    end: usize,
    frames: impl Iterator<Item = (usize, usize)>,
    emit: &mut dyn FnMut(i64) -> Result<(), E>,
    observe: &mut dyn FnMut(CountObservation) -> Result<(), E>,
) -> Result<(), E> {
    let mut prefix_non_null: Vec<i64> = Vec::new();
    if let Some(source) = source {
        observe(CountObservation::OpaqueBoundary)?;
        prefix_non_null = vec![0; (end - start) + 1];
        observe(CountObservation::OpaqueBoundary)?;
        for (i, row) in (start..end).enumerate() {
            let contribution = if contributes(source(row), CountNullRule::AnalyticRoot) {
                1
            } else {
                0
            };
            observe(CountObservation::Step)?;
            let mut next = prefix_non_null[i];
            add_original(&mut next, contribution);
            prefix_non_null[i + 1] = next;
            observe(CountObservation::Step)?;
        }
    }
    for (_row, (frame_start, frame_end)) in (start..end).zip(frames) {
        let count = if source.is_none() {
            (frame_end as i64) - (frame_start as i64)
        } else {
            let s = frame_start - start;
            let e = frame_end - start;
            prefix_non_null[e] - prefix_non_null[s]
        };
        observe(CountObservation::Step)?;
        emit(count)?;
        observe(CountObservation::Step)?;
    }
    Ok(())
}
