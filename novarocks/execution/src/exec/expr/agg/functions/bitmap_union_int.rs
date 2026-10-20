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
use std::collections::BTreeSet;
use std::sync::Arc;

#[cfg(test)]
use arrow::array::{
    Array, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, Int64Builder};
use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_types::value::bitmap::BITMAP_TYPE_EMPTY;
#[cfg(test)]
use novarocks_types::value::bitmap::decode_bitmap;
use novarocks_types::value::bitmap::encode_bitmap_aggregate as encode_bitmap;

use super::super::*;
use super::AggregateFunction;
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

pub(super) struct BitmapUnionIntAgg;

type BitmapState = novarocks_functions::bitmap_aggregate_core::BitmapState<AggregateAllocator>;

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match canonical_agg_name(name) {
        "bitmap_union_int" => Some(AggKind::BitmapUnionInt),
        "bitmap_agg" | "bitmap_union" => Some(AggKind::BitmapAgg),
        "bitmap_union_count" => Some(AggKind::BitmapUnionInt),
        _ => None,
    }
}

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_functions::bitmap_aggregate_core::{
    BitmapAggregatePort, BitmapInputFailure, InputFailure, StateMutation,
};
use novarocks_functions::bitmap_value::{BitmapDecodePort, BitmapOutputTally, BitmapValueFailure};

#[derive(Debug)]
enum NativeBitmapFailure {
    Data(BitmapValueFailure),
    Input(BitmapInputFailure),
    Kernel(novarocks_functions::KernelFailure),
}
impl std::fmt::Display for NativeBitmapFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(e) => e.fmt(f),
            Self::Input(e) => e.fmt(f),
            Self::Kernel(e) => e.fmt(f),
        }
    }
}
impl From<NativeBitmapFailure> for ExecutionFailure {
    fn from(source: NativeBitmapFailure) -> Self {
        match source {
            NativeBitmapFailure::Data(source) => Self::from_domain(source),
            NativeBitmapFailure::Input(source) => Self::from_domain(source),
            NativeBitmapFailure::Kernel(source) => source.into(),
        }
    }
}
struct NativeBitmapRows<W> {
    writer: W,
}
impl<W> NativeBitmapRows<W> {
    fn new(writer: W) -> Self {
        Self { writer }
    }
}
impl<W: FnMut(usize, StateMutation) -> Result<(), novarocks_functions::KernelFailure>>
    BitmapDecodePort for NativeBitmapRows<W>
{
    type Error = NativeBitmapFailure;
    fn failure(&mut self, source: BitmapValueFailure) -> Self::Error {
        NativeBitmapFailure::Data(source)
    }
    fn is_data(source: &Self::Error) -> bool {
        matches!(source, NativeBitmapFailure::Data(source) if source.is_data())
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_render(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_tree_insert(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_tree_collection(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_roaring(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_u32_collection(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl<W: FnMut(usize, StateMutation) -> Result<(), novarocks_functions::KernelFailure>>
    BitmapAggregatePort for NativeBitmapRows<W>
{
    fn input_failure(&mut self, source: InputFailure<'_>) -> Self::Error {
        NativeBitmapFailure::Input(source.into_failure())
    }
    fn observe(&mut self, ordinal: usize) -> Result<(), Self::Error> {
        (self.writer)(ordinal, StateMutation::Observe).map_err(NativeBitmapFailure::Kernel)
    }
    fn insert(&mut self, ordinal: usize, value: u64) -> Result<(), Self::Error> {
        (self.writer)(ordinal, StateMutation::Insert(value)).map_err(NativeBitmapFailure::Kernel)
    }
}

unsafe fn get_state<'a>(ptr: *mut u8) -> Option<&'a BitmapState> {
    Some(unsafe { &*ptr.cast::<BitmapState>() })
}

impl AggregateFunction for BitmapUnionIntAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        let kind = kind_from_name(func.name.as_str()).ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                "unsupported bitmap aggregate function: {}",
                func.name
            ))
        })?;
        if !input_is_intermediate {
            let dt = input_type.ok_or_else(|| {
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "bitmap_union_int requires 1 argument",
                )
            })?;
            if !matches!(
                dt,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
            ) {
                return Err(
                    crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                        "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
                        dt
                    )),
                );
            }
        }
        Ok(AggSpec {
            kind: kind.clone(),
            output_type: match kind {
                AggKind::BitmapAgg => DataType::Binary,
                AggKind::BitmapUnionInt => DataType::Int64,
                _ => unreachable!("unexpected bitmap kind"),
            },
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::BitmapAgg | AggKind::BitmapUnionInt => (
                std::mem::size_of::<BitmapState>(),
                std::mem::align_of::<BitmapState>(),
            ),
            other => unreachable!("unexpected kind for bitmap aggregate: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "bitmap_union_int input missing",
            )
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
                "bitmap_union_int intermediate input missing",
            )
        })?;
        let binary = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract(
                "failed to downcast to BinaryArray",
            )
        })?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, _ptr: *mut u8) {
        panic!("allocation-tracked bitmap state requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let tracker = tracker.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "allocation-tracked bitmap state requires a memory tracker",
            )
        })?;
        unsafe {
            ptr.cast::<BitmapState>()
                .write(BitmapState::new(AggregateAllocator::new(tracker)))
        };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { ptr.cast::<BitmapState>().drop_in_place() };
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
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
        let AggInputView::Any(array) = input else {
            return Err(super::aggregate_binding_contract(
                "bitmap_union_int batch input type mismatch",
            ));
        };
        use novarocks_functions::bitmap_aggregate_core::BitmapOperation;
        let operation = match spec.kind {
            AggKind::BitmapUnionInt => BitmapOperation::Count,
            _ => BitmapOperation::BitmapProjection,
        };
        let mut port = NativeBitmapRows::new(|ordinal, mutation| {
            let ptr = unsafe { (state_ptrs[ordinal] as *mut u8).add(offset) };
            let state = unsafe { &mut *ptr.cast::<BitmapState>() };
            match mutation {
                StateMutation::Observe => {
                    state.observe();
                    Ok(())
                }
                StateMutation::Insert(value) => state.insert(value),
            }
        });
        novarocks_functions::bitmap_aggregate_core::update(
            operation,
            array,
            0..state_ptrs.len(),
            &mut port,
        )
        .map_err(ExecutionFailure::from)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Binary(array) = input else {
            return Err(super::aggregate_binding_contract(
                "bitmap_union_int merge input type mismatch",
            ));
        };

        let mut port = NativeBitmapRows::new(|ordinal, mutation| {
            let ptr = unsafe { (state_ptrs[ordinal] as *mut u8).add(offset) };
            let state = unsafe { &mut *ptr.cast::<BitmapState>() };
            match mutation {
                StateMutation::Observe => {
                    state.observe();
                    Ok(())
                }
                StateMutation::Insert(value) => state.insert(value),
            }
        });
        novarocks_functions::bitmap_aggregate_core::merge(array, 0..state_ptrs.len(), &mut port)
            .map_err(ExecutionFailure::from)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        if output_intermediate || matches!(spec.kind, AggKind::BitmapAgg) {
            let mut extent = BitmapOutputTally::default();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                if let Some(state) = unsafe { get_state(ptr) }
                    && state.has_value
                {
                    extent.append(
                        state
                            .aggregate_encoded_size()
                            .map_err(ExecutionFailure::from_domain)?,
                    )?;
                }
            }
        }
        if output_intermediate {
            // For both BitmapAgg and BitmapUnionInt the intermediate stage must
            // emit NULL when no non-null input was observed, so the final
            // merge stage propagates the all-null group as NULL instead of
            // treating an empty BITMAP payload as a real (empty) input.
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                match unsafe { get_state(ptr) } {
                    Some(state) if state.has_value => builder.append_value(
                        encode_bitmap(&state.encoded_values())
                            .map_err(ExecutionFailure::from_domain)?,
                    ),
                    _ => builder.append_null(),
                }
            }
            return Ok(Arc::new(builder.finish()));
        }

        match &spec.kind {
            AggKind::BitmapUnionInt => {
                let mut builder = Int64Builder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    match unsafe { get_state(ptr) } {
                        Some(state) if state.has_value => {
                            let value = state
                                .cardinality()
                                .map_err(ExecutionFailure::from_domain)?
                                .expect("original has_value branch");
                            builder.append_value(value);
                        }
                        // All-null group: SQL semantics say bitmap_union_count
                        // and bitmap_union_int return NULL, not 0.
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            AggKind::BitmapAgg => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    match unsafe { get_state(ptr) } {
                        Some(state) if state.has_value => builder.append_value(
                            encode_bitmap(&state.encoded_values())
                                .map_err(ExecutionFailure::from_domain)?,
                        ),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            other => Err(super::aggregate_binding_contract(format_args!(
                "unexpected bitmap aggregate kind: {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AggregateAllocator, BitmapState, MemTracker};
    use std::collections::BTreeSet;
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array};
    use arrow::datatypes::DataType;

    use super::{
        AggregateFunction, BITMAP_TYPE_EMPTY, BitmapUnionIntAgg, decode_bitmap, encode_bitmap,
    };
    use crate::exec::expr::agg::functions::{AggInputView, AggKind, AggSpec, AggStatePtr};
    use crate::exec::node::aggregate::AggFunction;

    #[test]
    fn bitmap_agg_rejects_largeint_input() {
        let function = AggFunction {
            name: "bitmap_agg".to_string(),
            ..Default::default()
        };

        let error = BitmapUnionIntAgg
            .build_spec_from_type(&function, Some(&DataType::FixedSizeBinary(16)), false)
            .expect_err("LARGEINT must not be accepted as a bitmap aggregate input");

        assert!(
            error.detail().contains("bitmap aggregate expects"),
            "{error}"
        );
    }

    #[test]
    fn actual_bitmap_update_preserves_container_refusal_without_panicking_or_retaining_owner() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let tracker = MemTracker::new_root("bitmap original container refusal");
        tracker.install_limit_once(1).unwrap();
        let weak = Arc::downgrade(&tracker);
        let allocator = AggregateAllocator::new(tracker.clone());
        let mut state = BitmapState::new(allocator.clone());
        let pointer = (&mut state as *mut BitmapState) as AggStatePtr;
        let input: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let error = BitmapUnionIntAgg
            .update_batch(
                &spec_bitmap_union_int(),
                0,
                &[pointer],
                &AggInputView::Any(&input),
            )
            .unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 169);
        assert!(state.encoded_values().is_empty());
        assert_eq!(tracker.current(), 0);
        drop(state);
        drop(allocator);
        drop(tracker);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact(), fact);
    }
    #[test]
    fn bitmap_union_int_encodes_single32() {
        let values = BTreeSet::from([7_u64]);
        let encoded = encode_bitmap(&values).expect("encode");
        assert_eq!(encoded, vec![1_u8, 7, 0, 0, 0]);
        let decoded = decode_bitmap(&encoded).expect("decode");
        assert_eq!(decoded, values);
    }

    #[test]
    fn bitmap_union_int_round_trip_set() {
        let values = BTreeSet::from([1_u64, 300_u64, 1_000_000_u64]);
        let encoded = encode_bitmap(&values).expect("encode");
        let decoded = decode_bitmap(&encoded).expect("decode");
        assert_eq!(decoded, values);
    }

    /// Allocates a state slot on the heap, initializes it, runs `f`, then
    /// drops the state. Returns the output Array produced inside `f`.
    fn with_state<F>(spec: &AggSpec, f: F) -> ArrayRef
    where
        F: FnOnce(&BitmapUnionIntAgg, *mut u8, &AggSpec) -> ArrayRef,
    {
        let agg = BitmapUnionIntAgg;
        let mut backing = MaybeUninit::<super::BitmapState>::uninit();
        let ptr = backing.as_mut_ptr().cast::<u8>();
        let tracker = MemTracker::new_root("bitmap native component state");
        agg.init_state_with_tracker(spec, ptr, Some(tracker.clone()))
            .unwrap();
        let out = f(&agg, ptr, spec);
        agg.drop_state(spec, ptr);
        assert_eq!(tracker.current(), 0);
        out
    }

    fn spec_bitmap_agg() -> AggSpec {
        AggSpec {
            kind: AggKind::BitmapAgg,
            output_type: DataType::Binary,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        }
    }

    fn spec_bitmap_union_int() -> AggSpec {
        AggSpec {
            kind: AggKind::BitmapUnionInt,
            output_type: DataType::Int64,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        }
    }

    #[test]
    fn bitmap_agg_finalize_returns_null_for_empty_group() {
        let spec = spec_bitmap_agg();
        let out = with_state(&spec, |agg, ptr, spec| {
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr.is_null(0), "expected NULL for empty group");
    }

    #[test]
    fn bitmap_agg_finalize_returns_null_for_all_null_input() {
        let spec = spec_bitmap_agg();
        // Input array of length 1, all-null Int32 column.
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Option::<i32>::None]));
        let out = with_state(&spec, |agg, ptr, spec| {
            let view = AggInputView::Any(&input);
            agg.update_batch(spec, 0, &[ptr as AggStatePtr], &view)
                .expect("update_batch");
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr.is_null(0), "all-null group should finalize to NULL");
    }

    #[test]
    fn bitmap_agg_finalize_returns_empty_for_non_null_bitmap_empty_input() {
        // A non-null but logically empty BITMAP input still counts as a
        // non-null observation; result must be empty bitmap, NOT NULL.
        let spec = spec_bitmap_agg();
        // Use the merge path with a BITMAP_TYPE_EMPTY intermediate value.
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[BITMAP_TYPE_EMPTY][..])]));
        let out = with_state(&spec, |agg, ptr, spec| {
            let view = AggInputView::Binary(input.as_any().downcast_ref::<BinaryArray>().unwrap());
            agg.merge_batch(spec, 0, &[ptr as AggStatePtr], &view)
                .expect("merge_batch");
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(
            !arr.is_null(0),
            "non-null empty BITMAP input must yield non-null result"
        );
        assert_eq!(arr.value(0), &[BITMAP_TYPE_EMPTY][..]);
    }

    #[test]
    fn bitmap_agg_finalize_returns_bitmap_for_mixed_input() {
        let spec = spec_bitmap_agg();
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(11), None, Some(22)]));
        let out = with_state(&spec, |agg, ptr, spec| {
            let view = AggInputView::Any(&input);
            agg.update_batch(
                spec,
                0,
                &[ptr as AggStatePtr, ptr as AggStatePtr, ptr as AggStatePtr],
                &view,
            )
            .expect("update_batch");
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(!arr.is_null(0));
        let decoded = decode_bitmap(arr.value(0)).expect("decode");
        assert_eq!(decoded, BTreeSet::from([11_u64, 22_u64]));
    }

    #[test]
    fn bitmap_union_int_finalize_returns_null_for_empty_group() {
        let spec = spec_bitmap_union_int();
        let out = with_state(&spec, |agg, ptr, spec| {
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(
            arr.is_null(0),
            "bitmap_union_int/bitmap_union_count over empty group must be NULL, not 0"
        );
    }

    #[test]
    fn bitmap_union_int_finalize_returns_null_for_all_null_input() {
        let spec = spec_bitmap_union_int();
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Option::<i32>::None, None]));
        let out = with_state(&spec, |agg, ptr, spec| {
            let view = AggInputView::Any(&input);
            agg.update_batch(spec, 0, &[ptr as AggStatePtr, ptr as AggStatePtr], &view)
                .expect("update_batch");
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arr.is_null(0));
    }

    #[test]
    fn bitmap_union_int_finalize_returns_count_for_mixed_input() {
        let spec = spec_bitmap_union_int();
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(11), None, Some(22), Some(11)]));
        let out = with_state(&spec, |agg, ptr, spec| {
            let view = AggInputView::Any(&input);
            agg.update_batch(
                spec,
                0,
                &[
                    ptr as AggStatePtr,
                    ptr as AggStatePtr,
                    ptr as AggStatePtr,
                    ptr as AggStatePtr,
                ],
                &view,
            )
            .expect("update_batch");
            agg.build_array(spec, 0, &[ptr as AggStatePtr], false)
                .expect("build_array")
        });
        let arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 2_i64);
    }

    #[test]
    fn bitmap_agg_intermediate_emits_null_for_empty_group() {
        // Round-trip through intermediate: empty state should serialize to
        // NULL in the partial stage, so the merge stage sees NULL and skips
        // it instead of folding an empty bitmap into the final state.
        let spec = spec_bitmap_agg();
        let out = with_state(&spec, |agg, ptr, spec| {
            agg.build_array(spec, 0, &[ptr as AggStatePtr], true)
                .expect("build_array intermediate")
        });
        let arr = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert!(arr.is_null(0));
    }
}

#[cfg(test)]
#[path = "bitmap_union_int_original_baseline_tests.rs"]
mod original_baseline;
