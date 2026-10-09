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

use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::{
    Array, BinaryArray, BinaryBuilder, Int64Builder, LargeStringArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;

use crate::exec::hll::{HllHandle, HllTargetType};
use crate::exec::node::aggregate::AggFunction;
#[cfg(test)]
use crate::exec::sketch_hash::prehash_array_value;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;

#[cfg(test)]
const DEFAULT_LOG_K: u8 = 17;
#[cfg(test)]
const DEFAULT_TARGET_TYPE: HllTargetType = HllTargetType::Hll6;

pub(super) struct DsHllAgg;

struct DsHllState {
    handle: Option<HllHandle>,
    retained_charge: AggregateRetainedCharge,
    allocator: AggregateAllocator,
}

impl DsHllState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            handle: None,
            retained_charge: AggregateRetainedCharge::new(allocator.clone()),
            allocator,
        }
    }

    fn ensure_handle(
        &mut self,
        log_k: u8,
        target: HllTargetType,
    ) -> Result<&mut HllHandle, String> {
        novarocks_functions::builtin::aggregate_ds_hll_state::ensure_handle(
            &mut self.handle,
            &mut self.retained_charge,
            log_k,
            target,
            &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
        )
    }
    fn ensure_handle_from_payload(&mut self, payload: &[u8]) -> Result<&mut HllHandle, String> {
        novarocks_functions::builtin::aggregate_ds_hll_state::ensure_handle_from_payload(
            &mut self.handle,
            &mut self.retained_charge,
            payload,
            &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
        )
    }
    fn update_hash(&mut self, hash: u64) -> Result<(), String> {
        novarocks_functions::builtin::aggregate_ds_hll_state::update_hash(
            &mut self.handle,
            &mut self.retained_charge,
            hash,
            &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
        )
    }
    fn merge_payload(&mut self, payload: &[u8]) -> Result<(), String> {
        novarocks_functions::builtin::aggregate_ds_hll_state::merge_payload(
            &mut self.handle,
            &mut self.retained_charge,
            payload,
            &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
        )
    }
}
impl
    novarocks_functions::builtin::aggregate_ds_hll_state::DsHllRetainedPort<
        novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
    > for AggregateRetainedCharge
{
    type Reservation = super::super::allocation::AggregateTransientReservation;
    fn payload_error_headroom(
        &self,
        _: novarocks_functions::datasketches_hll::HllPayloadPreflight,
    ) -> usize {
        0
    }
    fn reserve(
        &self,
        bytes: usize,
        operation: novarocks_functions::builtin::aggregate_ds_hll_state::DsHllRetainedOperation,
        _: &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
    ) -> Result<Self::Reservation, String> {
        self.reserve_operation(bytes, operation.label())
    }
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        _: &mut novarocks_functions::builtin::aggregate_ds_hll_failure::LegacyDsHllFailure,
    ) -> Result<(), String> {
        self.reconcile_under_reservation(bytes, reservation)
    }
}

unsafe fn get_state<'a>(ptr: *const u8) -> &'a DsHllState {
    unsafe { &*(ptr as *const DsHllState) }
}

unsafe fn get_state_mut<'a>(ptr: *mut u8) -> &'a mut DsHllState {
    unsafe { &mut *(ptr as *mut DsHllState) }
}

impl novarocks_functions::builtin::aggregate_ds_hll_core::DsHllStorage for DsHllState {
    type Allocator = AggregateAllocator;
    fn allocator(&self) -> AggregateAllocator {
        self.allocator.clone()
    }
    fn handle(&self) -> Option<&HllHandle> {
        self.handle.as_ref()
    }
    fn ensure_handle(&mut self, log_k: u8, target: HllTargetType) -> Result<(), String> {
        DsHllState::ensure_handle(self, log_k, target).map(|_| ())
    }
    fn update_hash(&mut self, hash: u64) -> Result<(), String> {
        DsHllState::update_hash(self, hash)
    }
    fn merge_payload(&mut self, payload: &[u8]) -> Result<(), String> {
        DsHllState::merge_payload(self, payload)
    }
}
struct StateAccess<'a> {
    offset: usize,
    pointers: &'a [AggStatePtr],
}
impl novarocks_functions::builtin::aggregate_ds_hll_core::DsHllStateAccess for StateAccess<'_> {
    type State = DsHllState;
    fn len(&self) -> usize {
        self.pointers.len()
    }
    fn with_state<T>(
        &mut self,
        ordinal: usize,
        visit: impl FnOnce(&mut DsHllState) -> Result<T, String>,
    ) -> Result<T, String> {
        let ptr = unsafe { (self.pointers[ordinal] as *mut u8).add(self.offset) };
        visit(unsafe { get_state_mut(ptr) })
    }
}
impl novarocks_functions::builtin::aggregate_ds_hll_core::DsHllStateReadAccess for StateAccess<'_> {
    type State = DsHllState;
    fn len(&self) -> usize {
        self.pointers.len()
    }
    fn state(&self, ordinal: usize) -> &DsHllState {
        let ptr = unsafe { (self.pointers[ordinal] as *const u8).add(self.offset) };
        unsafe { get_state(ptr) }
    }
}

impl AggregateFunction for DsHllAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let Some(_input_type) = input_type else {
            return Err("ds_hll expects input".to_string());
        };

        let fe_output_is_binary = func
            .types
            .as_ref()
            .and_then(|sig| sig.output_type.as_ref())
            .is_some_and(|ty| matches!(ty, DataType::Binary));

        let kind = match canonical_agg_name(func.name.as_str()) {
            "ds_hll_count_distinct_union" => AggKind::DsHllMerge,
            "ds_hll_count_distinct_merge" if !input_is_intermediate && fe_output_is_binary => {
                AggKind::DsHllMerge
            }
            "ds_hll_count_distinct" | "approx_count_distinct_hll_sketch"
                if !input_is_intermediate =>
            {
                AggKind::DsHllHash
            }
            "ds_hll_count_distinct_merge"
            | "ds_hll_count_distinct"
            | "approx_count_distinct_hll_sketch" => AggKind::DsHllCount,
            other => return Err(format!("unsupported ds_hll aggregate function: {}", other)),
        };

        Ok(AggSpec {
            kind: kind.clone(),
            output_type: match kind {
                AggKind::DsHllMerge => DataType::Binary,
                _ => DataType::Int64,
            },
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::DsHllHash | AggKind::DsHllMerge | AggKind::DsHllCount => (
                std::mem::size_of::<DsHllState>(),
                std::mem::align_of::<DsHllState>(),
            ),
            other => unreachable!("unexpected ds_hll agg kind: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "ds_hll input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "ds_hll merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, _ptr: *mut u8) {
        panic!("allocation-tracked ds_hll requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked ds_hll requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(ptr as *mut DsHllState, DsHllState::new(tracker));
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut DsHllState);
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("ds_hll input type mismatch".to_string());
        };
        if novarocks_functions::builtin::aggregate_ds_hll_core::update_struct_if_present(
            array,
            &mut StateAccess {
                offset,
                pointers: state_ptrs,
            },
        )?
        .is_some()
        {
            return Ok(());
        }
        let mode = match &spec.kind {
            AggKind::DsHllHash => {
                novarocks_functions::builtin::aggregate_ds_hll_core::DsHllUpdateMode::Hash
            }
            AggKind::DsHllMerge => {
                novarocks_functions::builtin::aggregate_ds_hll_core::DsHllUpdateMode::Merge
            }
            AggKind::DsHllCount => {
                novarocks_functions::builtin::aggregate_ds_hll_core::DsHllUpdateMode::Count
            }
            other => return Err(format!("unexpected ds_hll aggregate kind: {:?}", other)),
        };
        novarocks_functions::builtin::aggregate_ds_hll_core::update_nonstruct_batch(
            mode,
            array,
            &mut StateAccess {
                offset,
                pointers: state_ptrs,
            },
        )
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("ds_hll merge input type mismatch".to_string());
        };
        novarocks_functions::builtin::aggregate_ds_hll_core::merge_batch(
            array,
            &mut StateAccess {
                offset,
                pointers: state_ptrs,
            },
            "ds_hll_merge",
        )
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };

        novarocks_functions::builtin::aggregate_ds_hll_core::build_array(
            output_type,
            &StateAccess {
                offset,
                pointers: group_states,
            },
        )
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_state_charges_retained_hll_heap_and_releases_on_drop() {
        let tracker = MemTracker::new_root("ds-hll-retained-test");
        {
            let mut state = DsHllState::new(Arc::clone(&tracker));
            state
                .ensure_handle(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)
                .expect("create handle");
            state.update_hash(11).expect("update handle");
            assert_eq!(tracker.current(), state.retained_charge.bytes() as i64);
            assert!(tracker.current() > 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn sparse_to_dense_growth_is_rejected_before_mutating_the_handle() {
        let tracker = MemTracker::new_root("ds-hll-admission-test");
        let mut state = DsHllState::new(Arc::clone(&tracker));
        state
            .ensure_handle(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)
            .expect("create handle");
        for hash in 0..7 {
            state.update_hash(hash).expect("sparse update");
        }
        let before_estimate = state
            .handle
            .as_ref()
            .expect("handle")
            .estimate()
            .expect("estimate");
        tracker
            .install_limit_once(tracker.current())
            .expect("install exact current limit");

        let error = state.update_hash(7).expect_err("dense allocation rejected");
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), state.retained_charge.bytes() as i64);
        assert_eq!(
            state
                .handle
                .as_ref()
                .expect("handle")
                .estimate()
                .expect("estimate"),
            before_estimate
        );
    }
    // Use the sealed catalog selection and the production aggregate adapters.
    // Exact estimates below come from the pinned Java 6.2.0 byte-domain oracle.
    fn prepared_binary_hll_parts(
        partitions: Vec<ArrayRef>,
        replay_dense: bool,
    ) -> (i64, Vec<Vec<u8>>, Vec<u8>) {
        use crate::exec::expr::agg::{
            AggStateArena, build_kernel_set, test_builtin_execution_function_set,
        };
        use crate::exec::node::aggregate::AggTypeSignature;
        use arrow::array::Int64Array;
        use arrow::datatypes::Field;
        use novarocks_functions::AggregateInputBatch;
        let functions = test_builtin_execution_function_set();
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted(
                "ds_hll_count_distinct",
                &[DataType::Binary, DataType::Int64],
            )
            .unwrap();
        assert_eq!(
            selected.argument_types,
            vec![DataType::Binary, DataType::Int64]
        );
        assert_eq!(selected.intermediate_type, DataType::Binary);
        assert_eq!(selected.output_type, DataType::Int64);
        let packed_fields = vec![
            Arc::new(Field::new("value", DataType::Binary, true)),
            Arc::new(Field::new("lgk", DataType::Int64, false)),
        ];
        let packed_type = DataType::Struct(packed_fields.clone().into());
        let function = |merge| AggFunction {
            name: "ds_hll_count_distinct".to_string(),
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Binary),
            }),
            ..Default::default()
        };
        let update = build_kernel_set(
            &functions,
            &[function(false)],
            &[Some(packed_type)],
            &[selected.clone()],
        )
        .unwrap();
        let merge = build_kernel_set(
            &functions,
            &[function(true)],
            &[Some(DataType::Binary)],
            &[selected],
        )
        .unwrap();
        let update = &update.entries[0];
        let merge = &merge.entries[0];
        let tracker = MemTracker::new_root("prepared-binary-hll-reference");
        let mut arena = AggStateArena::new(4096);
        arena.try_set_mem_tracker(tracker.clone()).unwrap();
        let mut partials = Vec::new();
        let mut frames = Vec::new();
        for values in partitions {
            let rows = values.len();
            let packed: ArrayRef = Arc::new(StructArray::new(
                packed_fields.clone().into(),
                vec![values, Arc::new(Int64Array::from(vec![10; rows]))],
                None,
            ));
            let pointer = arena.alloc(update.state.size, update.state_align());
            update
                .init_state_with_tracker(pointer, tracker.clone())
                .unwrap();
            update
                .update_batch(
                    &vec![pointer; rows],
                    AggregateInputBatch::try_new(Some(&packed), rows).unwrap(),
                )
                .unwrap();
            let partial = update.build_array(&[pointer], true).unwrap();
            frames.push(
                partial
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0)
                    .to_vec(),
            );
            partials.push(partial);
            update.drop_state(pointer);
        }
        let pointer = arena.alloc(merge.state.size, merge.state_align());
        merge
            .init_state_with_tracker(pointer, tracker.clone())
            .unwrap();
        for partial in partials
            .iter()
            .chain(if replay_dense { partials.first() } else { None })
        {
            merge
                .merge_batch(
                    &[pointer],
                    AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
                )
                .unwrap();
        }
        let result = merge.build_array(&[pointer], false).unwrap();
        let payload = merge.build_array(&[pointer], true).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let payload = payload
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        merge.drop_state(pointer);
        drop(arena);
        assert_eq!(
            tracker.current(),
            0,
            "retained heap and state blocks release after the tree exits"
        );
        (result, frames, payload)
    }

    fn binary_values(indices: impl IntoIterator<Item = i32>) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for index in indices {
            builder.append_value(format!("value_{index}").as_bytes());
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn ds_hll_prepared_binary_dense_threshold_matches_independent_raw_reference() {
        use datasketches::hll::HllSketch;
        for (count, expected_mode, estimate, integer) in [
            (96, 1, 96.00002264977122, 96),
            (97, 2, 97.00002312660857, 97),
            (100, 2, 100.20239597952975, 100),
        ] {
            let (result, frames, _) =
                prepared_binary_hll_parts(vec![binary_values(1..=count)], false);
            assert_eq!(result, integer);
            assert_eq!(
                frames[0][7] & 3,
                expected_mode,
                "97 unique coupons enter dense mode at lgK10"
            );
            let decoded = HllSketch::deserialize(&frames[0]).unwrap();
            assert!(
                (decoded.estimate() - estimate).abs() < 1e-8,
                "count={count}: {}",
                decoded.estimate()
            );
        }
    }

    #[test]
    fn ds_hll_prepared_binary_partial_merge_and_dense_replay_preserve_the_reference_domain() {
        use datasketches::hll::HllSketch;
        let (result, _, _) = prepared_binary_hll_parts(
            (0..3)
                .map(|part| binary_values((1..=100).filter(move |index| index % 3 == part)))
                .collect(),
            false,
        );
        assert_eq!(
            result, 100,
            "three sparse partials preserve all 100 fixed byte inputs"
        );
        // Each overlapping leaf is dense, so merging loses HIP chronology.
        // The composite estimate is register-derived and replay-idempotent.
        let (result, frames, payload) =
            prepared_binary_hll_parts(vec![binary_values(1..=100), binary_values(1..=100)], true);
        assert_eq!(result, 103);
        assert!(frames.iter().all(|frame| frame[7] & 3 == 2));
        let decoded = HllSketch::deserialize(&payload).unwrap();
        assert!(
            (decoded.estimate() - 102.9590150260783).abs() < 1e-8,
            "{}",
            decoded.estimate()
        );
    }

    #[test]
    fn ds_hll_binary_hash_domain_preserves_zero_invalid_utf8_empty_and_null_bytes() {
        let mut builder = BinaryBuilder::new();
        for bytes in [
            b"".as_slice(),
            &[0],
            &[0xff],
            &[0, 1, 0xff, 0x80],
            b"value_1".as_slice(),
        ] {
            builder.append_value(bytes);
        }
        builder.append_null();
        let values: ArrayRef = Arc::new(builder.finish());
        for (row, expected) in [
            0xd8dfea6585bc9732,
            0xa55b92ce23afa288,
            0xe325594e010c6967,
            0xb37e42d422e8a61a,
            0x6c264bfa909526d5,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                prehash_array_value(&values, row, "reference").unwrap(),
                Some(expected)
            );
        }
        assert_eq!(prehash_array_value(&values, 5, "reference").unwrap(), None);
        assert_eq!(prepared_binary_hll_parts(vec![values], false).0, 5);
    }
}

#[cfg(test)]
#[path = "legacy_ds_hll_aggregate_baseline_tests.rs"]
mod legacy_ds_hll_aggregate_baseline_tests;
