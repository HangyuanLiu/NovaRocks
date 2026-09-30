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

//! Pure typed aggregate preparation and selected update/merge lifecycles.
//! Execution owns group mapping, state arena/type erasure and destruction.

use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, validate_argument_observed};
use crate::{
    AggregateCallContract, AggregateKernelPhase, AggregateOrderKey, AggregateStateMemoryPolicy,
    CallEffectInput, EvaluatedArgument, FunctionBindingError, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCallContract, FunctionEffectOwner,
    FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure, ScopedExpressionEffects,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use arrow_array::ArrayRef;
use novarocks_type_contract::{CallEffects, CompilePhase, FunctionValueType, PureCompileControl};
use std::{alloc::Layout, fmt, sync::Arc};

/// Typed, immutable, exact preparation. The host monomorphizes batch dispatch;
/// this is not a per-row dynamic ABI or a private aggregate state arena.
///
/// State construction and every mutation/emission run inside host-installed
/// memory scopes with prior allocation/temporary-peak authorization. Retained
/// bounds below are facts for that owner, not grants or a second wallet.
/// Lifecycle data failures are non-maskable Operational failures; control,
/// resource and internal failures keep their distinct outer categories.
pub trait PreparedAggregateKernel: Send + Sync + fmt::Debug + 'static {
    type State: Send + 'static;
    type PreparedUpdateBatch<'batch>
    where
        Self: 'batch;
    type PreparedMergeBatch<'batch>
    where
        Self: 'batch;

    fn contract(&self) -> &Arc<AggregateCallContract>;
    fn memory_policy(&self) -> AggregateStateMemoryPolicy;
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure>;
    /// Borrow only actually selected values; never inspect unused batch rows
    /// or advance group state while preparing the input view.
    fn prepare_update<'batch>(
        &'batch self,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, KernelFailure>;
    /// The ordinal is relative to the exact prepared input's Selection.
    /// NULL and encoded carriers are read through that input's value_row map.
    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure>;
    /// As with update preparation, only selected input values are reachable.
    fn prepare_merge<'batch>(
        &'batch self,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'batch>, KernelFailure>;
    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        selected_ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure>;
    fn build_intermediate<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;
    fn build_final<'state, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;
    /// O(1) additional owned heap, including retained growth on error exits.
    /// The host accounts inline State in its arena separately.
    fn retained_bytes(&self, state: &Self::State) -> usize;
}

/// The exact pure owner returns a concrete immutable kernel. No live state or
/// service can be obtained during prepare. Process composition keeps the typed
/// adapter with this resolved object; runtime must not resolve by name again.
pub trait PureAggregateImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    type Kernel: PreparedAggregateKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, KernelFailure>;
}

#[derive(Clone, Debug)]
pub struct AggregatePreparationOptions {
    pub phase: AggregateKernelPhase,
    pub distinct: bool,
    pub order_keys: Arc<[AggregateOrderKey]>,
    pub state_input_type: Option<FunctionValueType>,
}
#[derive(Debug)]
pub struct AggregateSpecialization<K: PreparedAggregateKernel> {
    prepared: Arc<K>,
    effects: ScopedExpressionEffects,
}
impl<K: PreparedAggregateKernel> AggregateSpecialization<K> {
    pub fn prepared(&self) -> &Arc<K> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<K> {
        self.prepared
    }
    pub fn state_layout(&self) -> Layout {
        Layout::new::<K::State>()
    }
}

pub fn specialize_aggregate<O: PureAggregateImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    specialize_aggregate_once(owner, input, selected, None, arguments, options, control)
}
pub fn specialize_frozen_aggregate<O: PureAggregateImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    specialize_aggregate_once(
        owner,
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
fn specialize_aggregate_once<O: PureAggregateImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: AggregatePreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<AggregateSpecialization<O::Kernel>, FunctionSpecializationFailure> {
    let (receipt, effects) = crate::specialization::refine_once_for_specialization(
        owner, input, frozen, arguments, control,
    )?;
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let contract = Arc::new(
        AggregateCallContract::try_new(
            call,
            options.phase,
            options.distinct,
            options.order_keys,
            options.state_input_type,
            control,
        )
        .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_aggregate(input, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "aggregate preparation replaced its exact immutable contract",
        )));
    }
    state_retained_bound(prepared.as_ref()).map_err(FunctionSpecializationFailure::Kernel)?;
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(AggregateSpecialization { prepared, effects })
}

pub(crate) fn state_retained_bound<K: PreparedAggregateKernel>(
    kernel: &K,
) -> Result<usize, KernelFailure> {
    let heap = match kernel.memory_policy() {
        AggregateStateMemoryPolicy::FixedZero => 0,
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state,
        } => max_retained_bytes_per_state,
    };
    size_of::<K::State>()
        .checked_add(heap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(heap)
}
fn validate_state_retained<K: PreparedAggregateKernel>(
    kernel: &K,
    state: &K::State,
) -> Result<(), KernelFailure> {
    if kernel.retained_bytes(state) > state_retained_bound(kernel)? {
        Err(internal(
            "aggregate state exceeded its immutable retained bound",
        ))
    } else {
        Ok(())
    }
}
/// Returns an unpublished typed state only after successful initialization.
/// The host publishes it into its arena afterwards and owns exact Drop, even
/// during cancellation. State-owned partial construction cleanup remains Rust
/// RAII. Global-empty versus grouped-empty state creation is a host decision.
pub fn create_aggregate_state<K: PreparedAggregateKernel>(
    kernel: &K,
    control: &dyn KernelEvaluationControl,
) -> Result<K::State, KernelFailure> {
    control.checkpoint(0)?;
    state_retained_bound(kernel)?;
    let state = kernel.create_state(control)?;
    validate_state_retained(kernel, &state)?;
    control.checkpoint(0)?;
    Ok(state)
}

/// One borrowed selected batch; success advances once, failure latches. The
/// host maps next_selected_ordinal to one short exclusive group-state borrow.
/// Repeated group mappings are legal; no aliased mutable state slice exists.
/// The host must also latch the whole operator after failure: rebuilding this
/// batch cannot authorize replay of an already applied prefix. Concrete State
/// type identity is insufficient: the host retains exact specialization and
/// initialization/Drop identity for every supplied state.
pub struct AggregateUpdateInvocation<'batch, K: PreparedAggregateKernel> {
    kernel: &'batch K,
    input: SelectedAggregateUpdateInput<'batch, 'batch>,
    prepared: Option<K::PreparedUpdateBatch<'batch>>,
    next: usize,
    failed: bool,
}
impl<'batch, K: PreparedAggregateKernel> AggregateUpdateInvocation<'batch, K> {
    pub fn try_new(
        kernel: &'batch K,
        input: SelectedAggregateUpdateInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if !std::ptr::eq(input.contract(), kernel.contract().as_ref()) {
            return Err(invalid(
                "aggregate update input differs from exact prepared contract",
            ));
        }
        let prepared = if input.selection().is_empty() {
            None
        } else {
            Some(kernel.prepare_update(input, control)?)
        };
        control.checkpoint(0)?;
        Ok(Self {
            kernel,
            input,
            prepared,
            next: 0,
            failed: false,
        })
    }
    pub fn next_selected_ordinal(&self) -> Option<usize> {
        (!self.failed && self.next < self.input.selection().len()).then_some(self.next)
    }
    pub fn next_batch_row(&self) -> Option<usize> {
        self.next_selected_ordinal()
            .and_then(|ordinal| self.input.selection().row(ordinal))
    }
    pub fn update_next(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let Some(ordinal) = self.next_selected_ordinal() else {
            return Err(invalid("aggregate update has no remaining selected row"));
        };
        let result = (|| {
            control.checkpoint(0)?;
            validate_state_retained(self.kernel, state)?;
            let result = self.kernel.update_row(
                state,
                self.prepared.as_ref().expect("nonempty checked invocation"),
                ordinal,
                control,
            );
            // Observe retained growth even on error exits. Control/resources
            // remain the primary failure; no post-check grants allocation.
            finish_lifecycle(result, validate_state_retained(self.kernel, state))
        })();
        match result {
            Ok(()) => {
                self.next += 1;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

pub struct AggregateMergeInvocation<'batch, K: PreparedAggregateKernel> {
    kernel: &'batch K,
    input: SelectedAggregateMergeInput<'batch, 'batch>,
    prepared: Option<K::PreparedMergeBatch<'batch>>,
    next: usize,
    failed: bool,
}
impl<'batch, K: PreparedAggregateKernel> AggregateMergeInvocation<'batch, K> {
    pub fn try_new(
        kernel: &'batch K,
        input: SelectedAggregateMergeInput<'batch, 'batch>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if !std::ptr::eq(input.contract(), kernel.contract().as_ref()) {
            return Err(invalid(
                "aggregate merge input differs from exact prepared contract",
            ));
        }
        let prepared = if input.selection().is_empty() {
            None
        } else {
            Some(kernel.prepare_merge(input, control)?)
        };
        control.checkpoint(0)?;
        Ok(Self {
            kernel,
            input,
            prepared,
            next: 0,
            failed: false,
        })
    }
    pub fn next_selected_ordinal(&self) -> Option<usize> {
        (!self.failed && self.next < self.input.selection().len()).then_some(self.next)
    }
    pub fn next_batch_row(&self) -> Option<usize> {
        self.next_selected_ordinal()
            .and_then(|ordinal| self.input.selection().row(ordinal))
    }
    pub fn merge_next(
        &mut self,
        state: &mut K::State,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let Some(ordinal) = self.next_selected_ordinal() else {
            return Err(invalid("aggregate merge has no remaining selected row"));
        };
        let result = (|| {
            control.checkpoint(0)?;
            validate_state_retained(self.kernel, state)?;
            let result = self.kernel.merge_row(
                state,
                self.prepared.as_ref().expect("nonempty checked invocation"),
                ordinal,
                control,
            );
            finish_lifecycle(result, validate_state_retained(self.kernel, state))
        })();
        match result {
            Ok(()) => {
                self.next += 1;
                Ok(())
            }
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
}

pub(crate) fn finish_lifecycle<T>(
    result: Result<T, KernelFailure>,
    post: Result<(), KernelFailure>,
) -> Result<T, KernelFailure> {
    match result {
        Ok(value) => post.map(|()| value),
        Err(
            primary @ (KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted),
        ) => Err(primary),
        Err(primary) => match post {
            Err(violation @ KernelFailure::Internal(_)) => Err(violation),
            _ => Err(primary),
        },
    }
}

fn validate_emission_states<'state, K, I>(
    kernel: &K,
    states: I,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State>,
{
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    for state in states {
        validate_state_retained(kernel, state)?;
        work.step()?;
    }
    work.finish()
}

/// Emit complete supplied group states according to the exact call phase.
/// Iterator Clone must be a bounded borrowed view, never a state/schema copy.
/// The host preserves exact initialized state/kernel identities; sharing a
/// concrete State type does not prove the same specialization or provenance.
/// Row capacity is a host output-shape grant, not peak-byte authorization.
/// No outer scalar Selection can suppress required aggregate setup or errors.
/// Post-checking can itself stop on control; in that case it has not validated
/// every state. The host still reconciles and destroys actual retained backing.
pub fn emit_aggregate<'state, K, I>(
    kernel: &K,
    states: I,
    row_capacity: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure>
where
    K: PreparedAggregateKernel,
    K::State: 'state,
    I: ExactSizeIterator<Item = &'state K::State> + Clone,
{
    control.checkpoint(0)?;
    let rows = states.len();
    if rows > row_capacity {
        return Err(KernelFailure::ResourceExhausted);
    }
    validate_emission_states(kernel, states.clone(), control)?;
    let result = if kernel.contract().phase().produces_final_result() {
        kernel.build_final(states.clone(), control)
    } else {
        kernel.build_intermediate(states.clone(), control)
    };
    // &State may contain interior mutable storage. Both successful and failed
    // emissions are checked, without changing a primary control/resource error.
    let output = finish_lifecycle(result, validate_emission_states(kernel, states, control))?;
    let value_type = if kernel.contract().phase().produces_final_result() {
        kernel.contract().final_type()
    } else {
        kernel.contract().intermediate_type()
    };
    validate_argument_observed(
        EvaluatedArgument::Column(&output),
        Selection::all(rows),
        value_type,
        control,
    )
    .map_err(|error| match error {
        KernelFailure::InvalidProgram(_) => {
            internal("aggregate emission differs from exact result type, rows or NULL contract")
        }
        other => other,
    })?;
    Ok(output)
}

#[cfg(test)]
mod tests;
