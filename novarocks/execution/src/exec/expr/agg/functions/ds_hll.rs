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

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_functions::builtin::aggregate_ds_hll_core::{DEFAULT_LOG_K, DEFAULT_TARGET_TYPE};
use novarocks_functions::builtin::aggregate_ds_hll_failure::{
    DsHllFailureSink, DsHllInputContext, DsHllInputDetail, DsHllInputRecipe,
};
use novarocks_functions::builtin::{
    aggregate_ds_hll_core as core, aggregate_ds_hll_state as state_core,
};
use novarocks_functions::datasketches_hll_failure::{
    HllFailureRecipe, HllFailureSink, HllInvariantRecipe, HllObservation,
};

#[derive(Debug)]
enum NativeDsHllContract {
    MissingStateAllocator,
    UnexpectedKind(novarocks_failure_contract::BoundedDetail),
}
impl std::fmt::Display for NativeDsHllContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingStateAllocator => {
                f.write_str("DS HLL operation has no exact state allocator")
            }
            Self::UnexpectedKind(kind) => {
                write!(f, "unexpected DS HLL aggregate kind: {}", kind.as_str())
            }
        }
    }
}
impl std::error::Error for NativeDsHllContract {}
impl novarocks_failure_contract::FailureProjection for NativeDsHllContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            match self {
                Self::MissingStateAllocator => ExecutionReason::DsHllStateAllocator,
                Self::UnexpectedKind(_) => ExecutionReason::DsHllAggregateKind,
            }
            .reason(),
        )
    }
}
struct NativeDsHllSink {
    allocator: Option<AggregateAllocator>,
}
impl NativeDsHllSink {
    fn new(allocator: Option<AggregateAllocator>) -> Self {
        Self { allocator }
    }
}
impl HllFailureSink for NativeDsHllSink {
    type Error = ExecutionFailure;
    fn failure(&mut self, source: HllFailureRecipe) -> ExecutionFailure {
        ExecutionFailure::from_domain(source)
    }
    fn invariant(&mut self, source: HllInvariantRecipe) -> ExecutionFailure {
        ExecutionFailure::from_domain(source)
    }
    fn observe(&mut self, _: HllObservation) -> ExecutionResult<()> {
        Ok(())
    }
}
impl DsHllFailureSink for NativeDsHllSink {
    type Temporary = super::super::allocation::AggregateTransientReservation;
    fn kernel(&mut self, cause: novarocks_functions::KernelFailure) -> ExecutionFailure {
        cause.into()
    }
    fn reserve_temporary(&mut self, bytes: usize) -> ExecutionResult<Self::Temporary> {
        self.allocator
            .as_ref()
            .ok_or_else(|| {
                ExecutionFailure::from_domain(NativeDsHllContract::MissingStateAllocator)
            })?
            .reserve_transient(bytes, "reserve DS HLL temporary value")
            .map_err(Into::into)
    }
    fn input(&mut self, recipe: DsHllInputRecipe<'_>) -> ExecutionFailure {
        ExecutionFailure::from_domain(recipe.into_failure())
    }
}

pub(super) struct DsHllAgg;
struct DsHllState {
    // The actual sketch backing is destroyed before its retained charge.
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
    #[cfg(test)]
    fn ensure_handle(
        &mut self,
        log_k: u8,
        target: HllTargetType,
    ) -> ExecutionResult<&mut HllHandle> {
        state_core::ensure_handle(
            &mut self.handle,
            &mut self.retained_charge,
            log_k,
            target,
            &mut NativeDsHllSink::new(Some(self.allocator.clone())),
        )
    }
    #[cfg(test)]
    fn ensure_handle_from_payload(&mut self, payload: &[u8]) -> ExecutionResult<&mut HllHandle> {
        state_core::ensure_handle_from_payload(
            &mut self.handle,
            &mut self.retained_charge,
            payload,
            &mut NativeDsHllSink::new(Some(self.allocator.clone())),
        )
    }
    #[cfg(test)]
    fn update_hash(&mut self, hash: u64) -> ExecutionResult<()> {
        state_core::update_hash(
            &mut self.handle,
            &mut self.retained_charge,
            hash,
            &mut NativeDsHllSink::new(Some(self.allocator.clone())),
        )
    }
    #[cfg(test)]
    fn merge_payload(&mut self, payload: &[u8]) -> ExecutionResult<()> {
        state_core::merge_payload(
            &mut self.handle,
            &mut self.retained_charge,
            payload,
            &mut NativeDsHllSink::new(Some(self.allocator.clone())),
        )
    }
}
impl state_core::DsHllRetainedPort<NativeDsHllSink> for AggregateRetainedCharge {
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
        operation: state_core::DsHllRetainedOperation,
        _: &mut NativeDsHllSink,
    ) -> ExecutionResult<Self::Reservation> {
        self.reserve_operation(bytes, operation.label())
            .map_err(Into::into)
    }
    fn reconcile(
        &mut self,
        bytes: usize,
        reservation: &mut Self::Reservation,
        _: &mut NativeDsHllSink,
    ) -> ExecutionResult<()> {
        self.reconcile_under_reservation(bytes, reservation)
            .map_err(Into::into)
    }
}
unsafe fn get_state<'a>(ptr: *const u8) -> &'a DsHllState {
    unsafe { &*(ptr as *const DsHllState) }
}
unsafe fn get_state_mut<'a>(ptr: *mut u8) -> &'a mut DsHllState {
    unsafe { &mut *(ptr as *mut DsHllState) }
}
impl core::TypedDsHllStorage<NativeDsHllSink> for DsHllState {
    type Allocator = AggregateAllocator;
    fn allocator(&self) -> AggregateAllocator {
        self.allocator.clone()
    }
    fn handle(&self) -> Option<&HllHandle> {
        self.handle.as_ref()
    }
    fn ensure_handle(
        &mut self,
        log_k: u8,
        target: HllTargetType,
        sink: &mut NativeDsHllSink,
    ) -> ExecutionResult<()> {
        state_core::ensure_handle(
            &mut self.handle,
            &mut self.retained_charge,
            log_k,
            target,
            sink,
        )
        .map(|_| ())
    }
    fn update_hash(&mut self, hash: u64, sink: &mut NativeDsHllSink) -> ExecutionResult<()> {
        state_core::update_hash(&mut self.handle, &mut self.retained_charge, hash, sink)
    }
    fn merge_payload(&mut self, payload: &[u8], sink: &mut NativeDsHllSink) -> ExecutionResult<()> {
        state_core::merge_payload(&mut self.handle, &mut self.retained_charge, payload, sink)
    }
}
struct StateAccess<'a> {
    offset: usize,
    pointers: &'a [AggStatePtr],
}
impl StateAccess<'_> {
    fn allocator(&self) -> Option<AggregateAllocator> {
        self.pointers.first().map(|base| {
            unsafe { get_state((*base as *const u8).add(self.offset)) }
                .allocator
                .clone()
        })
    }
}
impl core::TypedDsHllStateAccess<NativeDsHllSink> for StateAccess<'_> {
    type State = DsHllState;
    fn len(&self) -> usize {
        self.pointers.len()
    }
    fn source_row(&self, ordinal: usize) -> usize {
        ordinal
    }
    fn with_state<T>(
        &mut self,
        ordinal: usize,
        visit: impl FnOnce(&mut DsHllState) -> ExecutionResult<T>,
    ) -> ExecutionResult<T> {
        let ptr = unsafe { (self.pointers[ordinal] as *mut u8).add(self.offset) };
        visit(unsafe { get_state_mut(ptr) })
    }
}
impl core::DsHllEmissionStates for StateAccess<'_> {
    fn len(&self) -> usize {
        self.pointers.len()
    }
    fn handle(&self, ordinal: usize) -> Option<&HllHandle> {
        unsafe { get_state((self.pointers[ordinal] as *const u8).add(self.offset)) }
            .handle
            .as_ref()
    }
}
struct NativeHllPayload {
    // Preserve backing-before-charge destruction for each serialized payload.
    bytes: Vec<u8>,
    charge: AggregateRetainedCharge,
}
impl AsRef<[u8]> for NativeHllPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct NativeHllEmission {
    allocator: AggregateAllocator,
}
impl NativeHllEmission {
    fn payload(
        &mut self,
        handle: &HllHandle,
        sink: &mut NativeDsHllSink,
    ) -> ExecutionResult<NativeHllPayload> {
        let preflight = handle.serialization_allocation_preflight_observed(&mut || {
            sink.observe(HllObservation::Step)
        })?;
        let mut charge = AggregateRetainedCharge::new(self.allocator.clone());
        let mut lease = charge.reserve_operation(
            preflight.bounds().additional_headroom_bytes,
            "reserve DS HLL serialization",
        )?;
        let bytes = handle.serialize_under_reservation_with_failure(&preflight, &lease, sink)?;
        charge.reconcile_under_reservation(bytes.capacity(), &mut lease)?;
        Ok(NativeHllPayload { bytes, charge })
    }
}
impl core::DsHllEmissionPort<NativeDsHllSink> for NativeHllEmission {
    type Payload = NativeHllPayload;
    fn empty_payload(&mut self, sink: &mut NativeDsHllSink) -> ExecutionResult<NativeHllPayload> {
        let mut state = DsHllState {
            handle: None,
            retained_charge: AggregateRetainedCharge::new(self.allocator.clone()),
            allocator: self.allocator.clone(),
        };
        state_core::ensure_handle(
            &mut state.handle,
            &mut state.retained_charge,
            DEFAULT_LOG_K,
            DEFAULT_TARGET_TYPE,
            sink,
        )?;
        self.payload(state.handle.as_ref().expect("DS HLL initialized"), sink)
    }
    fn serialize(
        &mut self,
        handle: &HllHandle,
        sink: &mut NativeDsHllSink,
    ) -> ExecutionResult<NativeHllPayload> {
        self.payload(handle, sink)
    }
    fn clone_empty(
        &mut self,
        empty: &NativeHllPayload,
        _: &mut NativeDsHllSink,
    ) -> ExecutionResult<NativeHllPayload> {
        let mut charge = AggregateRetainedCharge::new(self.allocator.clone());
        let mut lease =
            charge.reserve_operation(empty.bytes.len(), "reserve DS HLL empty payload copy")?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(empty.bytes.len())
            .map_err(|source| {
                self.allocator
                    .allocation_error("copy DS HLL empty payload", source)
            })?;
        bytes.extend_from_slice(&empty.bytes);
        charge.reconcile_under_reservation(bytes.capacity(), &mut lease)?;
        Ok(NativeHllPayload { bytes, charge })
    }
}

impl AggregateFunction for DsHllAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<AggSpec> {
        let Some(_input_type) = input_type else {
            return Err(
                crate::exec::expr::agg::functions::aggregate_binding_contract(
                    "ds_hll expects input",
                ),
            );
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
            other => {
                return Err(
                    crate::exec::expr::agg::functions::aggregate_binding_contract(format_args!(
                        "unsupported ds_hll aggregate function: {}",
                        other
                    )),
                );
            }
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
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract("ds_hll input missing")
        })?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> crate::runtime::fragment::ExecutionResult<AggInputView<'a>> {
        let arr = array.as_ref().ok_or_else(|| {
            crate::exec::expr::agg::views::aggregate_input_contract("ds_hll merge input missing")
        })?;
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
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let tracker = tracker.ok_or_else(|| {
            crate::exec::expr::agg::functions::aggregate_binding_contract(
                "allocation-tracked ds_hll requires an aggregate memory tracker",
            )
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
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Any(array) = input else {
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "ds_hll input type mismatch",
            ));
        };
        let mut access = StateAccess {
            offset,
            pointers: state_ptrs,
        };
        let mut sink = NativeDsHllSink::new(access.allocator());
        if core::update_struct_if_present_with_sink(array, &mut access, &mut sink)?.is_some() {
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
            other => {
                return Err(ExecutionFailure::from_domain(
                    NativeDsHllContract::UnexpectedKind(
                        novarocks_failure_contract::BoundedDetail::truncating_display(
                            format_args!("{other:?}"),
                        ),
                    ),
                ));
            }
        };
        core::update_nonstruct_batch_with_sink(mode, array, &mut access, &mut sink)
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        let AggInputView::Any(array) = input else {
            return Err(crate::exec::expr::agg::views::aggregate_input_contract(
                "ds_hll merge input type mismatch",
            ));
        };
        let mut access = StateAccess {
            offset,
            pointers: state_ptrs,
        };
        let mut sink = NativeDsHllSink::new(access.allocator());
        core::merge_batch_with_sink(array, &mut access, DsHllInputContext::Merge, &mut sink)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };

        let access = StateAccess {
            offset,
            pointers: group_states,
        };
        if group_states.is_empty() {
            return match output_type {
                DataType::Binary | DataType::Int64 => {
                    Ok(arrow::array::new_empty_array(output_type))
                }
                other => Err(ExecutionFailure::from_domain(
                    DsHllInputRecipe {
                        context: DsHllInputContext::Merge,
                        detail: DsHllInputDetail::Output(other),
                    }
                    .into_failure(),
                )),
            };
        }
        let allocator = access.allocator().ok_or_else(|| {
            ExecutionFailure::from_domain(NativeDsHllContract::MissingStateAllocator)
        })?;
        let mut sink = NativeDsHllSink::new(Some(allocator.clone()));
        core::build_array_with_sink(
            output_type,
            &access,
            &mut NativeHllEmission { allocator },
            &mut sink,
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
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
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

    #[test]
    fn serialized_payload_keeps_its_backing_charge_and_refuses_copy_before_mutation() {
        use super::core::DsHllEmissionPort;
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        let tracker = MemTracker::new_root("DS HLL serialized payload source");
        let weak = Arc::downgrade(&tracker);
        let allocator = AggregateAllocator::new(tracker.clone());
        let mut sink = NativeDsHllSink::new(Some(allocator.clone()));
        let mut emission = NativeHllEmission { allocator };
        let payload = emission.empty_payload(&mut sink).unwrap();
        let retained = tracker.current();
        assert_eq!(
            retained,
            payload.bytes.capacity() as i64,
            "temporary handle and scratch have exited while payload remains live"
        );
        assert_eq!(payload.charge.bytes(), payload.bytes.capacity());
        let original = payload.bytes.clone();
        tracker.install_limit_once(retained).unwrap();
        let error = match emission.clone_empty(&payload, &mut sink) {
            Err(error) => error,
            Ok(_) => panic!("copy must be admitted before allocating or publishing payload"),
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().domain(), domain::MEMORY.get());
        assert_eq!(fact.reason().code(), 8);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, Some(payload.bytes.len() as u64));
        assert_eq!(resource.limit, Some(retained as u64));
        assert_eq!(
            resource.committed,
            Some(retained as u64 + payload.bytes.len() as u64)
        );
        assert_eq!(payload.bytes, original);
        assert_eq!(tracker.current(), retained);
        drop(payload);
        assert_eq!(tracker.current(), 0);
        drop(emission);
        drop(sink);
        drop(tracker);
        assert!(
            weak.upgrade().is_none(),
            "the retained cause owns no tracker"
        );
        assert_eq!(error.failure_fact(), fact);
    }

    #[test]
    fn malformed_payload_keeps_original_header_source_without_owning_input_or_state() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        use novarocks_functions::datasketches_hll_failure::HllFailureDetail;
        let tracker = MemTracker::new_root("DS HLL malformed source");
        let mut state = DsHllState::new(tracker.clone());
        let payload = Arc::new(vec![1u8, 2, 3]);
        let weak = Arc::downgrade(&payload);
        let error = match state.ensure_handle_from_payload(payload.as_slice()) {
            Err(error) => error,
            Ok(_) => panic!("short payload must be refused before constructing a handle"),
        };
        assert!(state.handle.is_none());
        assert_eq!(state.retained_charge.bytes(), 0);
        assert_eq!(tracker.current(), 0);
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Data);
        assert_eq!(fact.reason().domain(), domain::KERNEL.get());
        assert_eq!(fact.reason().code(), 42);
        let crate::runtime::fragment::ExecutionFailureCause::Domain(source) = error.cause() else {
            panic!("the original HLL domain source must survive the execution boundary");
        };
        let source = source.downcast_ref::<HllFailureRecipe>().unwrap();
        assert!(matches!(
            source.detail,
            HllFailureDetail::Length {
                required: 8,
                actual: 3
            }
        ));
        drop(payload);
        drop(state);
        assert!(weak.upgrade().is_none());
        assert_eq!(tracker.current(), 0);
        assert_eq!(error.failure_fact(), fact);
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
