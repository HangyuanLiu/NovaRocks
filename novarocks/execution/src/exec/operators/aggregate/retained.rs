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

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use novarocks_functions::AggregateInputBatch;

use crate::exec::expr::agg::{AggKernelEntry, AggStatePtr, RetainedMemoryPolicy};
use crate::exec::hash_table::allocation::{AggregateAllocator, AggregateVec};
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason};
#[derive(Debug)]
struct RetainedContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for RetainedContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for RetainedContract {}
impl novarocks_failure_contract::FailureProjection for RetainedContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            ExecutionReason::AggregateRetainedContract.reason(),
        )
    }
}
fn retained_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(RetainedContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
fn retained_arithmetic() -> ExecutionFailure {
    ExecutionFailure::from_domain(
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
    )
}
#[derive(Debug)]
struct RetainedPolicyViolation {
    phase: &'static str,
    retained: usize,
    maximum: usize,
    previous: Option<usize>,
}
impl std::fmt::Display for RetainedPolicyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "aggregate retained policy violated {}: retained={} maximum={} previous={:?}",
            self.phase, self.retained, self.maximum, self.previous
        )
    }
}
impl std::error::Error for RetainedPolicyViolation {}
impl novarocks_failure_contract::FailureProjection for RetainedPolicyViolation {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            ExecutionReason::AggregateRetainedPolicy.reason(),
        )
    }
}
fn policy_violation(
    phase: &'static str,
    retained: usize,
    maximum: usize,
    previous: Option<usize>,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(RetainedPolicyViolation {
        phase,
        retained,
        maximum,
        previous,
    })
}
/// The operation remains primary; an independent accounting failure remains
/// available locally without reclassifying either cause from its text.
#[derive(Debug)]
struct RetainedOperationFailure {
    operation: ExecutionFailure,
    accounting: ExecutionFailure,
}
impl std::fmt::Display for RetainedOperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; aggregate accounting also failed: {}",
            self.operation, self.accounting
        )
    }
}
impl std::error::Error for RetainedOperationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.operation)
    }
}
impl novarocks_failure_contract::FailureProjection for RetainedOperationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.operation)
    }
}
fn with_accounting(
    primary: ExecutionFailure,
    accounting: Result<(), ExecutionFailure>,
) -> ExecutionFailure {
    match accounting {
        Ok(()) => primary,
        Err(accounting) => ExecutionFailure::from_domain(RetainedOperationFailure {
            operation: primary,
            accounting,
        }),
    }
}

const RETAINED_MEMORY_CHECK_ROWS: usize = 64;

pub(super) struct AggregateRetainedMemory {
    tracker: Option<Arc<MemTracker>>,
    bytes: usize,
    failure: Option<ExecutionFailure>,
}

impl AggregateRetainedMemory {
    pub(super) const fn new() -> Self {
        Self {
            tracker: None,
            bytes: 0,
            failure: None,
        }
    }

    pub(super) fn set_tracker(&mut self, tracker: Arc<MemTracker>) -> Result<(), ExecutionFailure> {
        if let Some(current) = self.tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return Ok(());
            }
            return Err(retained_contract(
                "aggregate retained-memory tracker cannot be rebound",
            ));
        }
        if self.bytes != 0 {
            return Err(retained_contract(
                "aggregate retained-memory tracker must be bound before allocation",
            ));
        }
        self.tracker = Some(tracker);
        Ok(())
    }

    pub(super) fn initialize_state(
        &mut self,
        kernel: &AggKernelEntry,
        state: AggStatePtr,
    ) -> Result<(), ExecutionFailure> {
        self.ensure_available()?;
        self.ensure_tracker_bound();
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::FixedZero => {
                kernel.init_state(state)?;
                let retained = kernel.retained_bytes(state);
                if retained == 0 {
                    return Ok(());
                }
                let accounting = self.apply_live_growth(retained);
                kernel.drop_state(state);
                self.release_accounted(retained);
                let error = with_accounting(
                    policy_violation("after fixed-zero initialization", retained, 0, None),
                    accounting,
                );
                self.failure = Some(error.clone());
                Err(error)
            }
            RetainedMemoryPolicy::AllocationTracked => {
                let Some(tracker) = self.tracker.as_ref().map(Arc::clone) else {
                    return Err(retained_contract(
                        "allocation-tracked aggregate requires a bound memory tracker",
                    ));
                };
                kernel.init_state_with_tracker(state, tracker)
            }
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => {
                self.reserve_headroom(max_retained_bytes_per_state)?;
                if let Err(error) = kernel.init_state(state) {
                    self.release_accounted(max_retained_bytes_per_state);
                    return Err(error);
                }
                let retained = kernel.retained_bytes(state);
                if retained <= max_retained_bytes_per_state {
                    self.release_accounted(max_retained_bytes_per_state - retained);
                    return Ok(());
                }

                let excess = retained - max_retained_bytes_per_state;
                let accounting = self.apply_live_growth(excess);
                kernel.drop_state(state);
                self.release_accounted(retained);
                let error = with_accounting(
                    policy_violation(
                        "after bounded initialization",
                        retained,
                        max_retained_bytes_per_state,
                        None,
                    ),
                    accounting,
                );
                self.failure = Some(error.clone());
                Err(error)
            }
        }
    }

    pub(super) fn around<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, ExecutionFailure>,
    ) -> Result<T, ExecutionFailure> {
        self.ensure_available()?;
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => operation(),
            RetainedMemoryPolicy::FixedZero => self.around_fixed_zero(kernel, states, operation),
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => {
                // Output materialization receives shared state references in the
                // typed contract and therefore cannot require mutation
                // headroom. Still verify the declared retained-memory contract
                // around legacy erased implementations.
                self.around_read_only_bounded(
                    kernel,
                    states,
                    max_retained_bytes_per_state,
                    operation,
                )
            }
        }
    }

    pub(super) fn run_bounded_batch(
        &mut self,
        touched: &mut TouchedAggregateStates,
        kernel: &AggKernelEntry,
        state_ptrs: &[AggStatePtr],
        batch: AggregateInputBatch<'_>,
        merge: bool,
    ) -> Result<(), ExecutionFailure> {
        if state_ptrs.len() != batch.row_count() {
            return Err(retained_contract(format_args!(
                "aggregate bounded batch state count mismatch: states={}, rows={}",
                state_ptrs.len(),
                batch.row_count()
            )));
        }
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => {
                self.ensure_available()?;
                return if merge {
                    kernel.merge_batch(state_ptrs, batch)
                } else {
                    kernel.update_batch(state_ptrs, batch)
                };
            }
            RetainedMemoryPolicy::FixedZero => {
                return self.around_mutation(kernel, state_ptrs, || {
                    if merge {
                        kernel.merge_batch(state_ptrs, batch)
                    } else {
                        kernel.update_batch(state_ptrs, batch)
                    }
                });
            }
            RetainedMemoryPolicy::BoundedRetained { .. } => {}
        }
        for start in (0..batch.row_count()).step_by(RETAINED_MEMORY_CHECK_ROWS) {
            let len = RETAINED_MEMORY_CHECK_ROWS.min(batch.row_count() - start);
            let values = batch.array_ref().map(|values| values.slice(start, len));
            let slice = AggregateInputBatch::try_new(values.as_ref(), len)
                .map_err(ExecutionFailure::from_domain)?;
            let state_slice = &state_ptrs[start..start + len];
            let touched = touched.pointers(state_slice)?;
            self.around_mutation(kernel, touched, || {
                if merge {
                    kernel.merge_batch(state_slice, slice)
                } else {
                    kernel.update_batch(state_slice, slice)
                }
            })?;
        }
        Ok(())
    }

    pub(super) fn drop_initialized_state(&mut self, kernel: &AggKernelEntry, state: AggStatePtr) {
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => kernel.drop_state(state),
            RetainedMemoryPolicy::FixedZero => {
                let retained = kernel.retained_bytes(state);
                kernel.drop_state(state);
                if retained != 0 {
                    self.release_accounted(retained);
                }
            }
            RetainedMemoryPolicy::BoundedRetained { .. } => {
                let retained = kernel.retained_bytes(state);
                kernel.drop_state(state);
                self.release_accounted(retained);
            }
        }
    }

    pub(super) fn release_all(&mut self) {
        if let Some(tracker) = self.tracker.as_ref() {
            tracker.release(bytes_to_i64(self.bytes));
        }
        self.bytes = 0;
    }

    fn around_mutation<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, ExecutionFailure>,
    ) -> Result<T, ExecutionFailure> {
        match kernel.retained_memory_policy() {
            RetainedMemoryPolicy::AllocationTracked => operation(),
            RetainedMemoryPolicy::FixedZero => self.around_fixed_zero(kernel, states, operation),
            RetainedMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => self.around_bounded_mutation(
                kernel,
                states,
                max_retained_bytes_per_state,
                operation,
            ),
        }
    }

    fn around_fixed_zero<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        operation: impl FnOnce() -> Result<T, ExecutionFailure>,
    ) -> Result<T, ExecutionFailure> {
        let before = retained_sum(kernel, states)?;
        if before != 0 {
            let error = policy_violation("before fixed-zero operation", before, 0, None);
            self.failure = Some(error.clone());
            return Err(error);
        }
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        if after == 0 {
            return operation_result;
        }
        let accounting_result = self.apply_live_growth(after);
        let violation = with_accounting(
            policy_violation("after fixed-zero operation", after, 0, Some(before)),
            accounting_result,
        );
        let result = combine_operation_and_accounting(operation_result, Err(violation));
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn around_read_only_bounded<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        max_retained_bytes_per_state: usize,
        operation: impl FnOnce() -> Result<T, ExecutionFailure>,
    ) -> Result<T, ExecutionFailure> {
        let before = retained_sum(kernel, states)?;
        let maximum = max_retained_bytes_per_state
            .checked_mul(states.len())
            .ok_or_else(retained_arithmetic)?;
        if before > maximum {
            let error = policy_violation("before read-only operation", before, maximum, None);
            self.failure = Some(error.clone());
            return Err(error);
        }
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        if before == after && after <= maximum {
            return operation_result;
        }
        let accounting_result = self.reconcile_live(before, after);
        let violation = policy_violation("after read-only operation", after, maximum, Some(before));
        let result = combine_operation_and_accounting(
            combine_operation_and_accounting(operation_result, accounting_result),
            Err(violation),
        );
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn around_bounded_mutation<T>(
        &mut self,
        kernel: &AggKernelEntry,
        states: &[AggStatePtr],
        max_retained_bytes_per_state: usize,
        operation: impl FnOnce() -> Result<T, ExecutionFailure>,
    ) -> Result<T, ExecutionFailure> {
        let before = retained_sum(kernel, states)?;
        let maximum = max_retained_bytes_per_state
            .checked_mul(states.len())
            .ok_or_else(retained_arithmetic)?;
        if before > maximum {
            let error = policy_violation("before bounded mutation", before, maximum, None);
            self.failure = Some(error.clone());
            return Err(error);
        }
        let headroom = maximum - before;
        self.reserve_headroom(headroom)?;
        let operation_result = operation();
        let after = retained_sum(kernel, states)?;
        let accounting_result = if after <= maximum {
            self.release_accounted(maximum - after);
            Ok(())
        } else {
            let accounting = self.apply_live_growth(after - maximum);
            Err(with_accounting(
                policy_violation("after bounded mutation", after, maximum, Some(before)),
                accounting,
            ))
        };
        let violated = after > maximum;
        let result = combine_operation_and_accounting(operation_result, accounting_result);
        if violated && let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn reconcile_live(&mut self, before: usize, after: usize) -> Result<(), ExecutionFailure> {
        if after >= before {
            self.apply_live_growth(after - before)
        } else {
            let released = before - after;
            self.release_accounted(released);
            Ok(())
        }
    }

    fn reserve_headroom(&mut self, bytes: usize) -> Result<(), ExecutionFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(retained_arithmetic)?;
        if let Some(tracker) = self.tracker.as_ref()
            && let Err(error) = tracker.consume_and_check_limit_nominal(bytes_to_i64(bytes))
        {
            tracker.release(bytes_to_i64(bytes));
            let error = ExecutionFailure::from_domain(error);
            self.failure = Some(error.clone());
            return Err(error);
        }
        self.bytes = next;
        Ok(())
    }

    fn apply_live_growth(&mut self, growth: usize) -> Result<(), ExecutionFailure> {
        if growth == 0 {
            return Ok(());
        }
        self.bytes = self
            .bytes
            .checked_add(growth)
            .ok_or_else(retained_arithmetic)?;
        if let Some(tracker) = self.tracker.as_ref() {
            // A limit error does not roll accounting back because the state
            // already owns the memory. Operator failure will drop the state
            // and release the complete retained total.
            if let Err(error) = tracker.consume_and_check_limit_nominal(bytes_to_i64(growth)) {
                let error = ExecutionFailure::from_domain(error);
                self.failure = Some(error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    fn release_accounted(&mut self, released: usize) {
        if released == 0 {
            return;
        }
        self.bytes = self.bytes.checked_sub(released).unwrap_or_else(|| {
            panic!(
                "aggregate retained-memory accounting underflow: tracked={}, release={released}",
                self.bytes
            )
        });
        if let Some(tracker) = self.tracker.as_ref() {
            tracker.release(bytes_to_i64(released));
        }
    }

    pub(super) fn ensure_available(&self) -> Result<(), ExecutionFailure> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone()))
    }

    fn ensure_tracker_bound(&mut self) {
        if self.tracker.is_none() {
            self.tracker = Some(MemTracker::new_child(
                "AggregateRetainedHeapUnbounded",
                &process_mem_tracker(),
            ));
        }
    }
}

impl Drop for AggregateRetainedMemory {
    fn drop(&mut self) {
        self.release_all();
    }
}

pub(super) struct TouchedAggregateStates {
    states: Option<AggregateVec<AggStatePtr>>,
    tracker: Option<Arc<MemTracker>>,
    failure: Option<ExecutionFailure>,
}

impl TouchedAggregateStates {
    pub(super) const fn new() -> Self {
        Self {
            states: None,
            tracker: None,
            failure: None,
        }
    }

    pub(super) fn set_tracker(&mut self, tracker: Arc<MemTracker>) -> Result<(), ExecutionFailure> {
        if let Some(current) = self.tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return Ok(());
            }
            return Err(retained_contract(
                "aggregate touched-state tracker cannot be rebound",
            ));
        }
        if self.states.is_some() {
            return Err(retained_contract(
                "aggregate touched-state tracker cannot be rebound",
            ));
        }
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        self.states = Some(AggregateVec::new_in(allocator));
        self.tracker = Some(tracker);
        Ok(())
    }

    pub(super) fn pointers(
        &mut self,
        state_ptrs: &[AggStatePtr],
    ) -> Result<&[AggStatePtr], ExecutionFailure> {
        self.ensure_available()?;
        self.ensure_tracker_bound()?;
        let states = self.states.as_mut().expect("touched-state tracker");
        states.clear();
        if let Err(source) = states.try_reserve(state_ptrs.len()) {
            let error = ExecutionFailure::from(
                states
                    .allocator()
                    .allocation_error("reserve aggregate touched states", source),
            );
            self.failure = Some(error.clone());
            return Err(error);
        }
        states.extend_from_slice(state_ptrs);
        states.sort_unstable();
        states.dedup();
        Ok(states)
    }

    pub(super) fn ensure_available(&self) -> Result<(), ExecutionFailure> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone()))
    }

    fn ensure_tracker_bound(&mut self) -> Result<(), ExecutionFailure> {
        if self.states.is_none() {
            self.set_tracker(MemTracker::new_child(
                "AggregateTouchedStatesUnbounded",
                &process_mem_tracker(),
            ))?;
        }
        Ok(())
    }
}

pub(super) struct AggregateOperatorVectorsMemory {
    failure: Option<ExecutionFailure>,
}

impl AggregateOperatorVectorsMemory {
    pub(super) const fn new() -> Self {
        Self { failure: None }
    }

    pub(super) fn set_tracker(
        &mut self,
        group_states: &mut AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
        tracker: Arc<MemTracker>,
    ) -> Result<(), ExecutionFailure> {
        group_states.bind(MemTracker::new_child(
            "AggregateGroupStatePointers",
            &tracker,
        ))?;
        state_ptrs.bind(MemTracker::new_child(
            "AggregateBatchStatePointers",
            &tracker,
        ))?;
        Ok(())
    }

    pub(super) fn ensure_bound(
        &mut self,
        group_states: &mut AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
    ) -> Result<(), ExecutionFailure> {
        self.ensure_available()?;
        if !group_states.is_bound() || !state_ptrs.is_bound() {
            let tracker =
                MemTracker::new_child("AggregateOperatorVectorsUnbounded", &process_mem_tracker());
            self.set_tracker(group_states, state_ptrs, tracker)?;
        }
        Ok(())
    }

    pub(super) fn reserve_group_states(
        &mut self,
        group_states: &mut AggregateStatePointers,
        _state_ptrs: &AggregateStatePointers,
        additional: usize,
    ) -> Result<(), ExecutionFailure> {
        self.ensure_available()?;
        let values = group_states.values_mut()?;
        if let Err(source) = values.try_reserve(additional) {
            let error = ExecutionFailure::from(
                values
                    .allocator()
                    .allocation_error("reserve aggregate group states", source),
            );
            self.failure = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn reserve_state_ptrs(
        &mut self,
        _group_states: &AggregateStatePointers,
        state_ptrs: &mut AggregateStatePointers,
        additional: usize,
    ) -> Result<(), ExecutionFailure> {
        self.ensure_available()?;
        let values = state_ptrs.values_mut()?;
        if let Err(source) = values.try_reserve(additional) {
            let error = ExecutionFailure::from(
                values
                    .allocator()
                    .allocation_error("reserve aggregate batch state pointers", source),
            );
            self.failure = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn ensure_available(&self) -> Result<(), ExecutionFailure> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone()))
    }
}

pub(super) struct AggregateStatePointers {
    values: Option<AggregateVec<AggStatePtr>>,
}

impl AggregateStatePointers {
    pub(super) const fn new() -> Self {
        Self { values: None }
    }

    fn bind(&mut self, tracker: Arc<MemTracker>) -> Result<(), ExecutionFailure> {
        if self.values.is_some() {
            return Err(retained_contract(
                "aggregate state-pointer vector cannot be rebound",
            ));
        }
        self.values = Some(AggregateVec::new_in(AggregateAllocator::new(tracker)));
        Ok(())
    }

    pub(super) const fn is_bound(&self) -> bool {
        self.values.is_some()
    }

    fn values_mut(&mut self) -> Result<&mut AggregateVec<AggStatePtr>, ExecutionFailure> {
        self.values
            .as_mut()
            .ok_or_else(|| retained_contract("aggregate state-pointer vector tracker is not bound"))
    }
}

impl Deref for AggregateStatePointers {
    type Target = AggregateVec<AggStatePtr>;

    fn deref(&self) -> &Self::Target {
        self.values
            .as_ref()
            .expect("aggregate state-pointer vector tracker")
    }
}

impl DerefMut for AggregateStatePointers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.values
            .as_mut()
            .expect("aggregate state-pointer vector tracker")
    }
}

fn retained_sum(
    kernel: &AggKernelEntry,
    states: &[AggStatePtr],
) -> Result<usize, ExecutionFailure> {
    states.iter().try_fold(0usize, |total, state| {
        total
            .checked_add(kernel.retained_bytes(*state))
            .ok_or_else(retained_arithmetic)
    })
}

fn combine_operation_and_accounting<T>(
    operation: Result<T, ExecutionFailure>,
    accounting: Result<(), ExecutionFailure>,
) -> Result<T, ExecutionFailure> {
    match (operation, accounting) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(accounting)) => {
            Err(ExecutionFailure::from_domain(RetainedOperationFailure {
                operation,
                accounting,
            }))
        }
    }
}

fn bytes_to_i64(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        AggregateOperatorVectorsMemory, AggregateRetainedMemory, AggregateStatePointers,
        TouchedAggregateStates,
    };
    use crate::runtime::mem_tracker::MemTracker;

    #[test]
    fn touched_state_capacity_is_latched_and_released_after_limit_crossing() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(1).expect("install limit");
        {
            let mut touched = TouchedAggregateStates::new();
            touched
                .set_tracker(MemTracker::new_child("touched", &tracker))
                .expect("bind tracker");
            let error = touched
                .pointers(&[3, 1, 3, 2])
                .expect_err("touched state allocation must cross the limit");
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&error).class(),
                novarocks_failure_contract::FailureClass::Resource
            );
            assert_eq!(tracker.current(), 0);
            let retry = touched
                .pointers(&[1])
                .expect_err("crossing must latch future mutations");
            assert_eq!(retry, error);
            assert_eq!(tracker.current(), 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn operator_vector_capacity_is_latched_and_released_after_limit_crossing() {
        let tracker = MemTracker::new_root("query");
        tracker.install_limit_once(1).expect("install limit");
        {
            let mut memory = AggregateOperatorVectorsMemory::new();
            let mut groups = AggregateStatePointers::new();
            let mut pointers = AggregateStatePointers::new();
            memory
                .set_tracker(
                    &mut groups,
                    &mut pointers,
                    MemTracker::new_child("vectors", &tracker),
                )
                .expect("bind tracker");
            let error = memory
                .reserve_group_states(&mut groups, &pointers, 1)
                .expect_err("group vector allocation must cross the limit");
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&error).class(),
                novarocks_failure_contract::FailureClass::Resource
            );
            assert_eq!(tracker.current(), 0);
            let retry = memory
                .reserve_group_states(&mut groups, &pointers, 1)
                .expect_err("crossing must latch future mutations");
            assert_eq!(retry, error);
            assert_eq!(tracker.current(), 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn retained_tracker_requires_binding_before_reservation_and_cannot_rebind() {
        let root = MemTracker::new_root("query");
        let first = MemTracker::new_child("first", &root);
        let second = MemTracker::new_child("second", &root);
        let mut memory = AggregateRetainedMemory::new();
        memory.set_tracker(Arc::clone(&first)).expect("first bind");
        let error = memory
            .set_tracker(second)
            .expect_err("retained tracker rebind must fail");
        assert!(error.detail().contains("cannot be rebound"), "{error}");

        let mut late = AggregateRetainedMemory::new();
        late.reserve_headroom(1).expect("untracked reservation");
        let error = late
            .set_tracker(first)
            .expect_err("late retained tracker bind must fail");
        assert!(error.detail().contains("before allocation"), "{error}");
        late.release_all();
    }
}

#[cfg(test)]
mod typed_retained_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn reservation_and_live_charge_keep_actual_capacity_facts_and_distinct_rollback() {
        for live in [false, true] {
            let tracker = MemTracker::new_root("retained-capacity-source");
            tracker.install_limit_once(8).unwrap();
            let mut memory = AggregateRetainedMemory::new();
            memory.set_tracker(tracker.clone()).unwrap();
            let error = if live {
                memory.apply_live_growth(16)
            } else {
                memory.reserve_headroom(16)
            }
            .unwrap_err();
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Resource);
            let resource = fact.resource().unwrap();
            assert_eq!(
                (resource.requested, resource.limit, resource.committed),
                (Some(16), Some(8), Some(16))
            );
            assert!(
                memory
                    .ensure_available()
                    .unwrap_err()
                    .same_occurrence(&error)
            );
            assert_eq!(tracker.current(), if live { 16 } else { 0 });
            memory.release_all();
            assert_eq!(tracker.current(), 0);
            drop(memory);
            assert_eq!(tracker.current(), 0);
        }
    }
    #[test]
    fn independent_accounting_failure_retains_the_primary_cause_and_both_originals() {
        use crate::runtime::fragment::ExecutionFailureCause;
        use novarocks_functions::KernelFailure;
        for source in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
        ] {
            let operation = ExecutionFailure::from(source);
            let accounting = policy_violation("test actual bound observation", 32, 8, Some(0));
            let error = combine_operation_and_accounting::<()>(
                Err(operation.clone()),
                Err(accounting.clone()),
            )
            .unwrap_err();
            assert_eq!(error.failure_fact(), operation.failure_fact());
            let ExecutionFailureCause::Domain(shared) = error.cause() else {
                panic!("compound source was erased")
            };
            let original = shared.downcast_ref::<RetainedOperationFailure>().unwrap();
            assert!(original.operation.same_occurrence(&operation));
            assert!(original.accounting.same_occurrence(&accounting));
            assert_eq!(
                original.accounting.failure_fact().class(),
                FailureClass::Invariant
            );
        }
        let error = policy_violation("after initialization", 9, 8, None);
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::AggregateRetainedPolicy.reason()
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
}
