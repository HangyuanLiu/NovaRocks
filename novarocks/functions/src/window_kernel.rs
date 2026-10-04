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

//! Pure window preparation and complete-input partition lifecycles.

use crate::aggregate_kernel::finish_lifecycle;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    AggregatePreparationOptions, CallEffectInput, FullPartitionWindowInput, FunctionBindingError,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCallContract, FunctionEffectOwner,
    FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure, PreparedAggregateKernel,
    PureAggregateImplementation, ScopedExpressionEffects, SelectedValues, Selection,
    WindowCallContract, WindowCallOptions, WindowOutputProjection, specialize_aggregate,
    specialize_frozen_aggregate,
};
use novarocks_type_contract::{
    CallEffects, CompilePhase, PureCompileControl, WindowFrameExclusion,
};
use std::{fmt, sync::Arc};

/// Half-open local partition row coordinates. Empty frames are legal; peer
/// groups are nonempty. Bounds do not prove sorting or SQL frame derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowRowRange {
    pub start: usize,
    pub end: usize,
}

#[cfg(test)]
mod aggregate_tests;
#[cfg(test)]
mod tests;

/// Complete partition geometry supplied by the operator's ORDER/frame owner.
/// This view makes no independent comparison or default-frame decision.
/// Contiguous frames express NO OTHERS only. Other exclusions require an
/// actual membership implementation, rather than silently ignoring options.
#[derive(Clone, Copy, Debug)]
pub struct WindowPartitionInput<'input> {
    input: FullPartitionWindowInput<'input, 'input>,
    peers: &'input [WindowRowRange],
    frames: &'input [WindowRowRange],
}
impl<'input> WindowPartitionInput<'input> {
    pub fn try_new(
        input: FullPartitionWindowInput<'input, 'input>,
        peers: &'input [WindowRowRange],
        frames: &'input [WindowRowRange],
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        if input
            .contract()
            .options()
            .frame()
            .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers)
        {
            return Err(invalid("contiguous window frames require NO OTHERS"));
        }
        let rows = input.partition_rows();
        if frames.len() != rows {
            return Err(invalid(
                "window frame table differs from complete partition",
            ));
        }
        let mut work = EvaluationCheckpoints::new(control);
        let mut end = 0;
        for peer in peers {
            if peer.start != end || peer.start >= peer.end || peer.end > rows {
                return Err(invalid(
                    "window peers do not continuously cover the partition",
                ));
            }
            end = peer.end;
            work.step()?;
        }
        if end != rows {
            return Err(invalid("window peers do not cover the complete partition"));
        }
        for frame in frames {
            if frame.start > frame.end || frame.end > rows {
                return Err(invalid("window frame is outside the complete partition"));
            }
            work.step()?;
        }
        work.finish()?;
        Ok(Self {
            input,
            peers,
            frames,
        })
    }
    pub const fn full_input(self) -> FullPartitionWindowInput<'input, 'input> {
        self.input
    }
    pub const fn peers(self) -> &'input [WindowRowRange] {
        self.peers
    }
    pub const fn frames(self) -> &'input [WindowRowRange] {
        self.frames
    }
}

/// Immutable resolved CPU preparation; no state, ExprArena, Task, connector,
/// lookup service or capacity account is stored here. The exact specializer
/// checks frame/NULL capabilities and required constants before publication.
/// Aggregate OVER uses an exact Single aggregate owner plus a window adapter,
/// retaining its aggregate identity and separate function ORDER channels.
pub trait PreparedWindowKernel: Send + Sync + fmt::Debug + 'static {
    fn contract(&self) -> &Arc<WindowCallContract>;
    /// O(1) complete bound of one boxed partition body and owned heap for the
    /// given row count, including bounded growth on error exits. Borrowed
    /// columns/geometry remain under their host owners. This is no MEM grant.
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure>;
    /// Called with complete input even when all output is guarded away. The
    /// owner must complete all required partition setup/evaluation and expose
    /// its required failures here, independently of output demand. Passing
    /// complete columns alone is insufficient: an Aggregate OVER adapter may
    /// not postpone required frame updates/overflow until selected emission.
    /// Arc receiver lets typed instances retain their exact immutable owner
    /// without a self-referential runtime object or raw lifetime conversion.
    fn begin_partition<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, KernelFailure>;
}

/// A mutable, input-borrowed CPU partition. Dispatch is once per selected
/// output batch. Setup and required input failures have no scalar row masking.
/// Actual construction, mutation, builders and Drop run under host MEM scopes;
/// post-call retained checks cannot authorize prior allocations or peaks.
pub trait WindowKernelPartition: Send {
    /// Output demand only. Required partition work/errors already belong to
    /// begin_partition; implementations cannot move them behind this Selection.
    /// This method may build selected results and report its own outer faults.
    fn evaluate<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure>;
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure>;
    /// Exact O(1) owned body/heap fact, including failure exits.
    fn retained_bytes(&self) -> usize;
}

pub trait PureWindowImplementation:
    FunctionBindingResolver + FunctionEffectOwner<Error = FunctionBindingError>
{
    fn prepare_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure>;
}

/// The same exact Aggregate owner provides its dedicated window adapter.
/// The argument is a real immutable typed Single kernel, already refined and
/// prepared once. This entry does not resolve a new Window-kind overload.
pub trait PureAggregateWindowImplementation: PureAggregateImplementation {
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<Self::Kernel>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure>;
}
#[derive(Debug)]
pub struct WindowSpecialization {
    prepared: Arc<dyn PreparedWindowKernel>,
    effects: ScopedExpressionEffects,
}
impl WindowSpecialization {
    pub fn prepared(&self) -> &Arc<dyn PreparedWindowKernel> {
        &self.prepared
    }
    pub const fn effects(&self) -> ScopedExpressionEffects {
        self.effects
    }
    pub fn into_prepared(self) -> Arc<dyn PreparedWindowKernel> {
        self.prepared
    }
}
pub fn specialize_window<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_window_once(owner, input, selected, None, arguments, options, control)
}
pub fn specialize_frozen_window<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_window_once(
        owner,
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
fn specialize_window_once<O: PureWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: WindowCallOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    let (receipt, effects) = crate::specialization::refine_once_for_specialization(
        owner, input, frozen, arguments, control,
    )?;
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let contract = Arc::new(
        WindowCallContract::try_window(call, options, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_window(input, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "window preparation replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(WindowSpecialization { prepared, effects })
}

/// Exact Aggregate Single call options and its independent OVER options.
#[derive(Clone, Debug)]
pub struct AggregateWindowPreparationOptions {
    pub aggregate: AggregatePreparationOptions,
    pub window: WindowCallOptions,
}

pub fn specialize_aggregate_window<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_aggregate_window_once(owner, input, selected, None, arguments, options, control)
}
pub fn specialize_frozen_aggregate_window<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: &CallEffects,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    specialize_aggregate_window_once(
        owner,
        input,
        selected,
        Some(frozen),
        arguments,
        options,
        control,
    )
}
fn specialize_aggregate_window_once<O: PureAggregateWindowImplementation + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    selected: Arc<FunctionBindingSelection>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    options: AggregateWindowPreparationOptions,
    control: &dyn PureCompileControl,
) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
    let AggregateWindowPreparationOptions {
        aggregate: aggregate_options,
        window: window_options,
    } = options;
    // Fail before even refining/preparing an inappropriate aggregate phase.
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    if aggregate_options.phase != crate::AggregateKernelPhase::Single
        || aggregate_options.state_input_type.is_some()
        || !matches!(
            input.argument_uses,
            crate::CallArgumentUses::SelectedChannels(_)
        )
    {
        return Err(FunctionSpecializationFailure::Kernel(invalid(
            "aggregate window specialization requires Single logical input",
        )));
    }
    let specialization = match frozen {
        Some(frozen) => specialize_frozen_aggregate(
            owner,
            input,
            selected,
            frozen,
            arguments,
            aggregate_options,
            control,
        ),
        None => specialize_aggregate(
            owner,
            input,
            selected,
            arguments,
            aggregate_options,
            control,
        ),
    }?;
    let effects = specialization.effects();
    let aggregate = specialization.into_prepared();
    let contract = Arc::new(
        WindowCallContract::try_aggregate(aggregate.contract().clone(), window_options, control)
            .map_err(FunctionSpecializationFailure::Kernel)?,
    );
    let prepared = owner
        .prepare_aggregate_window(aggregate, contract.clone(), control)
        .map_err(FunctionSpecializationFailure::Kernel)?;
    if !Arc::ptr_eq(prepared.contract(), &contract) {
        return Err(FunctionSpecializationFailure::Kernel(internal(
            "aggregate window adapter replaced its exact immutable contract",
        )));
    }
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    Ok(WindowSpecialization { prepared, effects })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartitionStatus {
    Open,
    Failed,
    Finished,
}

/// Execution-owned partition instance. Its complete inputs stay borrowed
/// through typed instance destruction; preparation itself stays immutable.
/// An error latches the partition, so a successful prefix cannot be replayed.
pub struct WindowEvaluationPartition<'input> {
    // Rust drops fields in declaration order: typed partition destruction must
    // complete while its exact immutable owner is still alive.
    instance: Box<dyn WindowKernelPartition + 'input>,
    prepared: Arc<dyn PreparedWindowKernel>,
    contract: Arc<WindowCallContract>,
    input: WindowPartitionInput<'input>,
    retained_upper_bound: usize,
    status: PartitionStatus,
}
impl<'input> WindowEvaluationPartition<'input> {
    pub fn begin(
        prepared: Arc<dyn PreparedWindowKernel>,
        input: WindowPartitionInput<'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        control.checkpoint(0)?;
        let contract = prepared.contract().clone();
        if !std::ptr::eq(contract.as_ref(), input.full_input().contract()) {
            return Err(invalid(
                "window partition differs from exact prepared contract",
            ));
        }
        let retained_upper_bound =
            prepared.partition_retained_upper_bound(input.full_input().partition_rows())?;
        size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let instance = prepared.clone().begin_partition(input, control)?;
        let partition = Self {
            prepared,
            contract,
            input,
            retained_upper_bound,
            instance,
            status: PartitionStatus::Open,
        };
        partition.validate_retained()?;
        partition.validate_metadata()?;
        control.checkpoint(0)?;
        Ok(partition)
    }
    pub fn retained_upper_bound(&self) -> usize {
        size_of::<Self>() + self.retained_upper_bound
    }
    pub fn retained_bytes(&self) -> Result<usize, KernelFailure> {
        self.validate_retained()?;
        Ok(size_of::<Self>() + self.instance.retained_bytes())
    }
    fn validate_retained(&self) -> Result<(), KernelFailure> {
        if self.instance.retained_bytes() > self.retained_upper_bound {
            Err(internal(
                "window partition exceeded its frozen retained bound",
            ))
        } else {
            Ok(())
        }
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(self.prepared.contract(), &self.contract)
            || self
                .prepared
                .partition_retained_upper_bound(self.input.full_input().partition_rows())?
                != self.retained_upper_bound
        {
            Err(internal(
                "window implementation changed its immutable metadata",
            ))
        } else {
            Ok(())
        }
    }
    pub fn evaluate<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure> {
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            let input = self.input.full_input();
            let projection = WindowOutputProjection::try_new(&input, selection, control)?;
            if selection.len() > row_capacity {
                return Err(invalid("window output Selection exceeds host row grant"));
            }
            let output = if selection.is_empty() {
                // The complete-input begin already ran. Skipping output work
                // cannot erase required setup/input failures or finish work.
                SelectedValues::try_new(
                    selection,
                    &self.contract.result_type().data_type,
                    arrow_array::new_empty_array(&self.contract.result_type().data_type),
                    Box::default(),
                )
                .map_err(|_| internal("empty window output violates its exact contract"))?
            } else {
                self.instance.evaluate(selection, row_capacity, control)?
            };
            projection.validate_result(&output, row_capacity, control)?;
            control.checkpoint(0)?;
            Ok(output)
        })();
        let result = finish_lifecycle(result, self.validate_retained());
        let result = finish_lifecycle(result, self.validate_metadata());
        if result.is_err() {
            self.status = PartitionStatus::Failed;
        }
        result
    }
    pub fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.status != PartitionStatus::Open {
            return Err(KernelFailure::InstanceFailed);
        }
        self.status = PartitionStatus::Finished;
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            self.instance.finish(control)?;
            control.checkpoint(0)
        })();
        let result = finish_lifecycle(result, self.validate_retained());
        let result = finish_lifecycle(result, self.validate_metadata());
        if result.is_err() {
            self.status = PartitionStatus::Failed;
        }
        result
    }
}
