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
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

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

fn legacy_state_error(error: novarocks_functions::aggregate_scalar::ScalarStateError) -> String {
    match error {
        novarocks_functions::aggregate_scalar::ScalarStateError::Legacy(message) => message,
        _ => unreachable!("the original bitmap allocator authors only its legacy rejection"),
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
    ) -> Result<AggSpec, String> {
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported bitmap aggregate function: {}", func.name))?;
        if !input_is_intermediate {
            let dt =
                input_type.ok_or_else(|| "bitmap_union_int requires 1 argument".to_string())?;
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
                return Err(format!(
                    "bitmap aggregate expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
                    dt
                ));
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
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bitmap_union_int input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bitmap_union_int intermediate input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            ptr.cast::<BitmapState>()
                .write(BitmapState::new(AggregateAllocator::new(
                    process_mem_tracker(),
                )))
        };
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked bitmap state requires a memory tracker".to_string()
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
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("bitmap_union_int batch input type mismatch".to_string());
        };
        use novarocks_functions::bitmap_aggregate_core::{
            BitmapOperation, LegacyRows, StateMutation,
        };
        let operation = match spec.kind {
            AggKind::BitmapUnionInt => BitmapOperation::Count,
            _ => BitmapOperation::BitmapProjection,
        };
        let mut port = LegacyRows::new(|ordinal, mutation| {
            let ptr = unsafe { (state_ptrs[ordinal] as *mut u8).add(offset) };
            let state = unsafe { &mut *ptr.cast::<BitmapState>() };
            match mutation {
                StateMutation::Observe => {
                    state.observe();
                    Ok(())
                }
                StateMutation::Insert(value) => state.insert(value).map_err(legacy_state_error),
            }
        });
        novarocks_functions::bitmap_aggregate_core::update(
            operation,
            array,
            0..state_ptrs.len(),
            &mut port,
        )
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(array) = input else {
            return Err("bitmap_union_int merge input type mismatch".to_string());
        };
        use novarocks_functions::bitmap_aggregate_core::{LegacyRows, StateMutation};
        let mut port = LegacyRows::new(|ordinal, mutation| {
            let ptr = unsafe { (state_ptrs[ordinal] as *mut u8).add(offset) };
            let state = unsafe { &mut *ptr.cast::<BitmapState>() };
            match mutation {
                StateMutation::Observe => {
                    state.observe();
                    Ok(())
                }
                StateMutation::Insert(value) => state.insert(value).map_err(legacy_state_error),
            }
        });
        novarocks_functions::bitmap_aggregate_core::merge(array, 0..state_ptrs.len(), &mut port)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            // For both BitmapAgg and BitmapUnionInt the intermediate stage must
            // emit NULL when no non-null input was observed, so the final
            // merge stage propagates the all-null group as NULL instead of
            // treating an empty BITMAP payload as a real (empty) input.
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                match unsafe { get_state(ptr) } {
                    Some(state) if state.has_value => {
                        builder.append_value(encode_bitmap(&state.encoded_values())?)
                    }
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
                                .map_err(|failure| failure.to_string())?
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
                        Some(state) if state.has_value => {
                            builder.append_value(encode_bitmap(&state.encoded_values())?)
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            other => Err(format!("unexpected bitmap aggregate kind: {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
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

        assert!(error.contains("bitmap aggregate expects"), "{error}");
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
        agg.init_state(spec, ptr);
        let out = f(&agg, ptr, spec);
        agg.drop_state(spec, ptr);
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
