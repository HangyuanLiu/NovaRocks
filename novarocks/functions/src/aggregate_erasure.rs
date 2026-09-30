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

//! Framework-owned typed aggregate erasure with host-borrowed state storage.
//! Handles are immutable; slots and batch frames are execution-owned objects.

use crate::aggregate_kernel::{finish_lifecycle, state_retained_bound};
use crate::kernel_control::{compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregateCallContract, AggregateMergeInvocation, AggregateStateMemoryPolicy,
    AggregateUpdateInvocation, KernelEvaluationControl, KernelFailure, PreparedAggregateKernel,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, create_aggregate_state,
    emit_aggregate,
};
use arrow_array::ArrayRef;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{
    alloc::Layout, cell::Cell, fmt, marker::PhantomData, mem::MaybeUninit, ptr::NonNull, sync::Arc,
};

/// A real resolved CPU implementation, not an identity/name to resolve later.
/// Clone shares the same adapter allocation. Wrapping a typed kernel again
/// creates a distinct ownership domain, even if its contract/Arc is equal.
#[derive(Clone, Debug)]
pub struct PreparedAggregateHandle {
    inner: Arc<dyn ErasedAggregateOps>,
}
impl PreparedAggregateHandle {
    pub fn from_typed<K: PreparedAggregateKernel>(
        kernel: Arc<K>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(compile_failure)?;
        state_retained_bound(kernel.as_ref())?;
        let inner = Arc::new(TypedOps {
            contract: kernel.contract().clone(),
            layout: Layout::new::<K::State>(),
            policy: kernel.memory_policy(),
            kernel,
        });
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(compile_failure)?;
        Ok(Self { inner })
    }
    pub fn contract(&self) -> &Arc<AggregateCallContract> {
        self.inner.contract()
    }
    pub fn state_layout(&self) -> Layout {
        self.inner.layout()
    }
    pub fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        self.inner.policy()
    }
    /// The host already owns and authorized this correctly aligned storage.
    /// Its borrow survives until typed state destruction. The slot allocates
    /// no state arena and carries no capacity account, lease or Task context.
    pub fn initialize_in<'storage>(
        &self,
        storage: &'storage mut [MaybeUninit<u8>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateStateSlot<'storage>, KernelFailure> {
        control.checkpoint(0)?;
        let layout = self.state_layout();
        let pointer = NonNull::new(storage.as_mut_ptr().cast::<u8>())
            .expect("Rust slices have non-null backing");
        if storage.len() < layout.size() || !pointer.as_ptr().addr().is_multiple_of(layout.align())
        {
            return Err(invalid(
                "aggregate state storage differs from its actual layout",
            ));
        }
        let owner = self.inner.clone();
        // SAFETY: the entire storage is exclusively borrowed for the returned
        // slot's lifetime, has enough bytes and satisfies actual State alignment.
        // The private generic adapter initializes exactly its State type.
        unsafe {
            owner.initialize(pointer, control)?;
        }
        // No fallible operation may intervene between successful write and
        // installing this unique typed destruction owner.
        Ok(AggregateStateSlot {
            pointer,
            owner,
            _storage: PhantomData,
            _not_sync: PhantomData,
        })
    }
    fn validate_mapping(
        &self,
        states: &[AggregateStateSlot<'_>],
        mapping: &[usize],
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        for index in mapping {
            let slot = states
                .get(*index)
                .ok_or_else(|| invalid("aggregate state mapping is out of range"))?;
            if !Arc::ptr_eq(&self.inner, &slot.owner) {
                return Err(invalid(
                    "aggregate state mapping differs from exact prepared owner",
                ));
            }
            work.step()?;
        }
        work.finish()
    }
    pub fn prepare_update_batch<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateUpdateInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateBatchInvocation<'frame>, KernelFailure> {
        control.checkpoint(0)?;
        if mapping.len() != input.selection().len() {
            return Err(invalid(
                "aggregate update mapping differs from selected row count",
            ));
        }
        self.validate_mapping(states, mapping, control)?;
        self.inner
            .prepare_update(states, mapping, input, control)
            .map(|inner| AggregateBatchInvocation { inner })
    }
    pub fn prepare_merge_batch<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateMergeInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<AggregateBatchInvocation<'frame>, KernelFailure> {
        control.checkpoint(0)?;
        if mapping.len() != input.selection().len() {
            return Err(invalid(
                "aggregate merge mapping differs from selected row count",
            ));
        }
        self.validate_mapping(states, mapping, control)?;
        self.inner
            .prepare_merge(states, mapping, input, control)
            .map(|inner| AggregateBatchInvocation { inner })
    }
    /// State indices preserve host output order and may repeat shared borrows.
    /// Global group/output responsibility remains the host's plan obligation.
    pub fn emit(
        &self,
        states: &[AggregateStateSlot<'_>],
        indices: &[usize],
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        self.validate_mapping(states, indices, control)?;
        self.inner.emit(states, indices, row_capacity, control)
    }
}

/// One initialized typed state, borrowing the host's actual bytes. It is not
/// Clone or Sync; State only promises Send. Moving unique ownership to another
/// execution thread is valid, but sharing &Slot across threads is forbidden.
pub struct AggregateStateSlot<'storage> {
    pointer: NonNull<u8>,
    owner: Arc<dyn ErasedAggregateOps>,
    _storage: PhantomData<&'storage mut [MaybeUninit<u8>]>,
    _not_sync: PhantomData<Cell<()>>,
}
// SAFETY: private generic initialization always creates a Send State. The slot
// uniquely owns that state and the exclusive backing borrow; its immutable
// owner is Send+Sync. No shared state reference survives a unique slot move.
unsafe impl Send for AggregateStateSlot<'_> {}
impl fmt::Debug for AggregateStateSlot<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateStateSlot")
            .field("layout", &self.owner.layout())
            .finish_non_exhaustive()
    }
}
impl AggregateStateSlot<'_> {
    pub fn state_layout(&self) -> Layout {
        self.owner.layout()
    }
    pub fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        self.owner.policy()
    }
    /// Exact O(1) owner-reported heap for reconciliation, including on failure.
    /// This fact does not authorize allocation or prove physical coverage.
    pub fn retained_heap_bytes(&self) -> usize {
        // SAFETY: only the corresponding generic adapter can create this slot;
        // its initialized state is alive and exclusively owned until Drop.
        unsafe { self.owner.retained_heap(self.pointer) }
    }
}
impl Drop for AggregateStateSlot<'_> {
    fn drop(&mut self) {
        // SAFETY: unique initialization and no Clone ensure exactly one Drop.
        // owner remains alive until after typed destruction. Cancellation is
        // never consulted here and cannot skip the state's actual destructor.
        unsafe {
            self.owner.destroy(self.pointer);
        }
    }
}

/// One mutable execution batch frame. A dynamic call runs the whole typed row
/// loop, not a virtual call per row. Each success advances once; any failure
/// latches this frame. The host separately latches the entire operator and
/// must not rebuild a frame to replay an already mutated successful prefix.
pub struct AggregateBatchInvocation<'frame> {
    inner: Box<dyn BatchExecution + 'frame>,
}
impl AggregateBatchInvocation<'_> {
    pub fn run(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        self.inner.run(control)
    }
    pub fn rows_processed(&self) -> usize {
        self.inner.rows_processed()
    }
}

trait BatchExecution {
    fn run(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure>;
    fn rows_processed(&self) -> usize;
}
trait ErasedAggregateOps: Send + Sync + fmt::Debug {
    fn contract(&self) -> &Arc<AggregateCallContract>;
    fn layout(&self) -> Layout;
    fn policy(&self) -> AggregateStateMemoryPolicy;
    unsafe fn initialize(
        &self,
        pointer: NonNull<u8>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure>;
    unsafe fn destroy(&self, pointer: NonNull<u8>);
    unsafe fn retained_heap(&self, pointer: NonNull<u8>) -> usize;
    fn prepare_update<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateUpdateInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn BatchExecution + 'frame>, KernelFailure>;
    fn prepare_merge<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateMergeInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn BatchExecution + 'frame>, KernelFailure>;
    fn emit(
        &self,
        states: &[AggregateStateSlot<'_>],
        indices: &[usize],
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>;
}
#[derive(Debug)]
struct TypedOps<K: PreparedAggregateKernel> {
    kernel: Arc<K>,
    contract: Arc<AggregateCallContract>,
    layout: Layout,
    policy: AggregateStateMemoryPolicy,
}
impl<K: PreparedAggregateKernel> TypedOps<K> {
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        if self.kernel.memory_policy() != self.policy
            || !Arc::ptr_eq(self.kernel.contract(), &self.contract)
        {
            Err(internal(
                "aggregate implementation changed its immutable metadata",
            ))
        } else {
            Ok(())
        }
    }
    fn validate_slot(&self, slot: &AggregateStateSlot<'_>) -> Result<(), KernelFailure> {
        if !std::ptr::addr_eq(slot.owner.as_ref(), self as &dyn ErasedAggregateOps) {
            Err(invalid("aggregate slot differs from exact typed adapter"))
        } else {
            Ok(())
        }
    }
    fn validate_retained(&self, state: &K::State) -> Result<(), KernelFailure> {
        let bound = match self.policy {
            AggregateStateMemoryPolicy::FixedZero => 0,
            AggregateStateMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state,
            } => max_retained_bytes_per_state,
        };
        if self.kernel.retained_bytes(state) > bound {
            Err(internal(
                "aggregate state exceeded its frozen retained bound",
            ))
        } else {
            Ok(())
        }
    }
}
impl<K: PreparedAggregateKernel> ErasedAggregateOps for TypedOps<K> {
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn layout(&self) -> Layout {
        self.layout
    }
    fn policy(&self) -> AggregateStateMemoryPolicy {
        self.policy
    }
    unsafe fn initialize(
        &self,
        pointer: NonNull<u8>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.validate_metadata()?;
        let state = create_aggregate_state(self.kernel.as_ref(), control)?;
        self.validate_retained(&state)?;
        self.validate_metadata()?;
        // SAFETY: the private caller checked the layout of this exact K::State
        // and exclusively borrowed storage. State is moved exactly once.
        unsafe {
            pointer.cast::<K::State>().as_ptr().write(state);
        }
        Ok(())
    }
    unsafe fn destroy(&self, pointer: NonNull<u8>) {
        // SAFETY: this thunk is retained by the unique initialized slot.
        unsafe {
            pointer.cast::<K::State>().as_ptr().drop_in_place();
        }
    }
    unsafe fn retained_heap(&self, pointer: NonNull<u8>) -> usize {
        // SAFETY: private slot ownership proves initialization and exact type.
        self.kernel
            .retained_bytes(unsafe { pointer.cast::<K::State>().as_ref() })
    }
    fn prepare_update<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateUpdateInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn BatchExecution + 'frame>, KernelFailure> {
        self.validate_metadata()?;
        let invocation = AggregateUpdateInvocation::try_new(self.kernel.as_ref(), input, control)?;
        Ok(Box::new(UpdateFrame {
            adapter: self,
            states,
            mapping,
            invocation,
            processed: 0,
            finished: false,
        }))
    }
    fn prepare_merge<'frame, 'storage: 'frame>(
        &'frame self,
        states: &'frame mut [AggregateStateSlot<'storage>],
        mapping: &'frame [usize],
        input: SelectedAggregateMergeInput<'frame, 'frame>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn BatchExecution + 'frame>, KernelFailure> {
        self.validate_metadata()?;
        let invocation = AggregateMergeInvocation::try_new(self.kernel.as_ref(), input, control)?;
        Ok(Box::new(MergeFrame {
            adapter: self,
            states,
            mapping,
            invocation,
            processed: 0,
            finished: false,
        }))
    }
    fn emit(
        &self,
        states: &[AggregateStateSlot<'_>],
        indices: &[usize],
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        self.validate_metadata()?;
        // Each mapping was checked before this private dispatch, and immutable
        // slot borrows keep initialization and exact owner identity unchanged.
        let typed = TypedStateIter::<K> {
            states,
            indices,
            next: 0,
            _kernel: PhantomData,
        };
        let result = emit_aggregate(self.kernel.as_ref(), typed.clone(), row_capacity, control);
        let post = (|| {
            self.validate_metadata()?;
            let mut work = EvaluationCheckpoints::new(control);
            for state in typed {
                self.validate_retained(state)?;
                work.step()?;
            }
            work.finish()
        })();
        finish_lifecycle(result, post)
    }
}

struct UpdateFrame<'frame, 'storage, K: PreparedAggregateKernel> {
    adapter: &'frame TypedOps<K>,
    states: &'frame mut [AggregateStateSlot<'storage>],
    mapping: &'frame [usize],
    invocation: AggregateUpdateInvocation<'frame, K>,
    processed: usize,
    finished: bool,
}
impl<K: PreparedAggregateKernel> BatchExecution for UpdateFrame<'_, '_, K> {
    fn run(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.finished {
            return Err(KernelFailure::InstanceFailed);
        }
        // Consumed on both success and failure: the exact batch is never replayed.
        self.finished = true;
        let result = (|| {
            control.checkpoint(0)?;
            self.adapter.validate_metadata()?;
            while let Some(ordinal) = self.invocation.next_selected_ordinal() {
                let slot = &mut self.states[self.mapping[ordinal]];
                self.adapter.validate_slot(slot)?;
                // SAFETY: the slot is initialized by the same private adapter. Each
                // iteration borrows just one State and expires it before the next,
                // so repeated group mappings create no aliased mutable references.
                let state = unsafe { slot.pointer.cast::<K::State>().as_mut() };
                self.adapter.validate_retained(state)?;
                let result = self.invocation.update_next(state, control);
                finish_lifecycle(result, self.adapter.validate_retained(state))?;
                self.processed += 1;
            }
            control.checkpoint(0)
        })();
        finish_lifecycle(result, self.adapter.validate_metadata())
    }
    fn rows_processed(&self) -> usize {
        self.processed
    }
}
struct MergeFrame<'frame, 'storage, K: PreparedAggregateKernel> {
    adapter: &'frame TypedOps<K>,
    states: &'frame mut [AggregateStateSlot<'storage>],
    mapping: &'frame [usize],
    invocation: AggregateMergeInvocation<'frame, K>,
    processed: usize,
    finished: bool,
}
impl<K: PreparedAggregateKernel> BatchExecution for MergeFrame<'_, '_, K> {
    fn run(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.finished {
            return Err(KernelFailure::InstanceFailed);
        }
        self.finished = true;
        let result = (|| {
            control.checkpoint(0)?;
            self.adapter.validate_metadata()?;
            while let Some(ordinal) = self.invocation.next_selected_ordinal() {
                let slot = &mut self.states[self.mapping[ordinal]];
                self.adapter.validate_slot(slot)?;
                // SAFETY: exact generic adapter identity and unique per-iteration
                // borrow prove State type, alignment, initialization and exclusivity.
                let state = unsafe { slot.pointer.cast::<K::State>().as_mut() };
                self.adapter.validate_retained(state)?;
                let result = self.invocation.merge_next(state, control);
                finish_lifecycle(result, self.adapter.validate_retained(state))?;
                self.processed += 1;
            }
            control.checkpoint(0)
        })();
        finish_lifecycle(result, self.adapter.validate_metadata())
    }
    fn rows_processed(&self) -> usize {
        self.processed
    }
}
struct TypedStateIter<'a, 'storage, K: PreparedAggregateKernel> {
    states: &'a [AggregateStateSlot<'storage>],
    indices: &'a [usize],
    next: usize,
    _kernel: PhantomData<K>,
}
impl<K: PreparedAggregateKernel> Clone for TypedStateIter<'_, '_, K> {
    fn clone(&self) -> Self {
        Self {
            states: self.states,
            indices: self.indices,
            next: self.next,
            _kernel: PhantomData,
        }
    }
}
impl<'a, K: PreparedAggregateKernel> Iterator for TypedStateIter<'a, '_, K> {
    type Item = &'a K::State;
    fn next(&mut self) -> Option<Self::Item> {
        let index = *self.indices.get(self.next)?;
        self.next += 1;
        let slot = &self.states[index];
        // SAFETY: private construction follows complete owner/range validation;
        // shared slot borrow keeps the initialized State alive. Slot is !Sync,
        // so a merely Send State cannot be shared across execution threads.
        Some(unsafe { slot.pointer.cast::<K::State>().as_ref() })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}
impl<K: PreparedAggregateKernel> ExactSizeIterator for TypedStateIter<'_, '_, K> {
    fn len(&self) -> usize {
        self.indices.len() - self.next
    }
}

#[cfg(test)]
mod tests;
