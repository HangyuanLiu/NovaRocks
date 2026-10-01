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

use super::*;
use crate::aggregate_kernel::{
    AggregatePreparationOptions, AggregateSpecialization, PureAggregateImplementation,
    specialize_aggregate,
};
use crate::{
    AggregateBindingSelection, AggregateStateFormatIdentity, FunctionArgument,
    FunctionBindingRequest, FunctionEffectOwnerError, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, KernelDiagnostic,
};
use crate::{AggregateKernelPhase, AggregateOrderKey, EvaluatedArgument};
use crate::{
    CallEffectInput, FunctionBindingError, FunctionBindingResolver, FunctionBindingSelection,
    FunctionEffectOwner, ScopedExpressionEffects, Selection,
};
use arrow_array::{Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters,
};
use novarocks_type_contract::{CallEffects, FunctionValueType};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    prepare: AtomicUsize,
    create: AtomicUsize,
    drop: AtomicUsize,
    policy_drift: AtomicUsize,
    kernel_drop: AtomicUsize,
    drop_saw_live_owner: AtomicUsize,
    update: AtomicUsize,
    merge: AtomicUsize,
    batches: AtomicUsize,
    intermediate: AtomicUsize,
    final_emit: AtomicUsize,
}
#[derive(Clone, Debug, Default)]
enum Emit {
    #[default]
    Good,
    WrongRows,
    WrongType,
    Null,
    Grow(Option<KernelFailure>),
    Failure(KernelFailure),
}
#[derive(Clone, Debug)]
struct Behavior {
    bound: usize,
    foreign: bool,
    create_failure: Option<KernelFailure>,
    create_growth: bool,
    create_drift: bool,
    row_drift: bool,
    row_failure: Option<(usize, KernelFailure)>,
    row_growth: bool,
    emit: Emit,
}
impl Default for Behavior {
    fn default() -> Self {
        Self {
            bound: 0,
            foreign: false,
            create_failure: None,
            create_growth: false,
            create_drift: false,
            row_drift: false,
            row_failure: None,
            row_growth: false,
            emit: Emit::Good,
        }
    }
}
#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    positive: bool,
    seen: Mutex<Vec<u32>>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(work <= 256);
        self.seen.lock().unwrap().push(work);
        if !self.positive || work > 0 {
            self.failure.map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive: bool,
    seen: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        self.seen.lock().unwrap().push(work);
        if !self.positive || work > 0 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
struct Owner {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    logical: usize,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
    counts: Arc<Counts>,
    behavior: Behavior,
}
impl Owner {
    fn new(logical: usize, ordered: bool) -> Self {
        let mut types = vec![FunctionValueType::new(DataType::Int64, false); logical];
        if ordered {
            types.push(FunctionValueType::new(DataType::Int32, false));
        }
        let arguments = types
            .into_iter()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        Self {
            id: FunctionId::try_new("fixture/aggregate-lifecycle/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/aggregate-lifecycle/signature")
                    .unwrap(),
                argument_types: arguments
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect(),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Int32,
                    false,
                )),
                aggregate: Some(AggregateBindingSelection {
                    intermediate_type: FunctionValueType::new(DataType::Int64, false),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/aggregate-lifecycle/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            uses: (0..arguments.len())
                .map(|n| Some(ExpressionUseId::new(n as u32 + 1)))
                .collect(),
            arguments,
            logical,
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Aggregate,
                instance_state: FunctionInstanceState::AggregateInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
            counts: Arc::new(Counts::default()),
            behavior: Behavior::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                domain: EvaluationDomainId::new(9),
                demand: EvaluationDemand::Value,
            },
            argument_uses: &self.uses,
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.logical,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn frozen(&self) -> CallEffects {
        CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn options(&self, phase: AggregateKernelPhase) -> AggregatePreparationOptions {
        let update = phase.consumes_logical_arguments();
        AggregatePreparationOptions {
            phase,
            distinct: update,
            order_keys: if update {
                vec![
                    AggregateOrderKey {
                        ascending: false,
                        nulls_first: true
                    };
                    self.arguments.len() - self.logical
                ]
                .into()
            } else {
                Arc::from([])
            },
            state_input_type: (!update).then(|| {
                self.selected
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .intermediate_type
                    .clone()
            }),
        }
    }
    fn specialize(&self, phase: AggregateKernelPhase) -> AggregateSpecialization<Kernel> {
        let input = self.input();
        specialize_aggregate(
            self,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            self.options(phase),
            &CompileControl::default(),
        )
        .unwrap()
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("must not reselect")
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(selected, self.selected.as_ref())
            || request.logical_argument_count != self.logical
            || request.arguments != self.arguments
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || !std::ptr::eq(selected, self.selected.as_ref()) {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.counts.refine.fetch_add(1, Ordering::Relaxed);
        self.validate_selected(input.selected, input.request, control)?;
        if input.function_id != &self.id
            || input.kind != FunctionKind::Aggregate
            || !input.environment.is_empty()
            || !std::ptr::eq(input.parameters, &self.parameters)
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        for _ in input.request.arguments {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(self.frozen())
    }
}
impl PureAggregateImplementation for Owner {
    type Kernel = Kernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Kernel>, KernelFailure> {
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        assert!(std::ptr::eq(contract.call().selected(), input.selected));
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        let contract = if self.behavior.foreign {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        Ok(Arc::new(Kernel {
            contract,
            counts: self.counts.clone(),
            behavior: self.behavior.clone(),
        }))
    }
}
#[derive(Debug)]
struct Kernel {
    contract: Arc<AggregateCallContract>,
    counts: Arc<Counts>,
    behavior: Behavior,
}
#[repr(align(64))]
#[derive(Debug)]
struct State {
    sum: i64,
    last_row: Option<usize>,
    last_order: Option<i32>,
    heap: Mutex<Vec<u8>>,
    counts: Arc<Counts>,
}
impl Drop for State {
    fn drop(&mut self) {
        self.counts.drop.fetch_add(1, Ordering::Relaxed);
        if self.counts.kernel_drop.load(Ordering::Relaxed) == 0 {
            self.counts
                .drop_saw_live_owner
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl Drop for Kernel {
    fn drop(&mut self) {
        self.counts.kernel_drop.fetch_add(1, Ordering::Relaxed);
    }
}
impl Kernel {
    fn row(
        &self,
        state: &mut State,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(256)?;
        if self.behavior.row_drift {
            self.counts.policy_drift.store(1, Ordering::Relaxed);
        }
        if self.behavior.row_growth {
            state.heap.lock().unwrap().push(1);
        }
        if let Some((at, error)) = &self.behavior.row_failure
            && *at == ordinal
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn emit<'s, I>(
        &self,
        states: I,
        final_result: bool,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s State>,
    {
        control.checkpoint(0)?;
        let mut values = Vec::with_capacity(states.len());
        for state in states {
            control.checkpoint(1)?;
            if let Emit::Grow(_) = self.behavior.emit {
                state.heap.lock().unwrap().push(1);
            }
            values.push(state.sum);
        }
        match &self.behavior.emit {
            Emit::Grow(Some(error)) | Emit::Failure(error) => return Err(error.clone()),
            Emit::WrongRows => values.push(0),
            Emit::WrongType => {
                return Ok(Arc::new(arrow_array::BooleanArray::from(vec![
                    true;
                    values.len()
                ])));
            }
            Emit::Null => {
                return Ok(if final_result {
                    Arc::new(Int32Array::from(vec![None; values.len()]))
                } else {
                    Arc::new(Int64Array::from(vec![None; values.len()]))
                });
            }
            _ => {}
        }
        Ok(if final_result {
            Arc::new(Int32Array::from(
                values.iter().map(|v| (*v * 10) as i32).collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Int64Array::from(values))
        })
    }
}
impl PreparedAggregateKernel for Kernel {
    type State = State;
    type PreparedUpdateBatch<'b> = SelectedAggregateUpdateInput<'b, 'b>;
    type PreparedMergeBatch<'b> = SelectedAggregateMergeInput<'b, 'b>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state: self
                .behavior
                .bound
                .saturating_add(self.counts.policy_drift.load(Ordering::Relaxed)),
        }
    }
    fn create_state(&self, control: &dyn KernelEvaluationControl) -> Result<State, KernelFailure> {
        self.counts.create.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let state = State {
            sum: 0,
            last_row: None,
            last_order: None,
            heap: Mutex::new(if self.behavior.create_growth {
                vec![1]
            } else {
                vec![]
            }),
            counts: self.counts.clone(),
        };
        if self.behavior.create_drift {
            self.counts.policy_drift.store(1, Ordering::Relaxed);
        }
        if let Some(error) = &self.behavior.create_failure {
            return Err(error.clone());
        }
        Ok(state)
    }
    fn prepare_update<'b>(
        &'b self,
        input: SelectedAggregateUpdateInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.batches.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
    fn update_row<'b>(
        &self,
        state: &mut State,
        input: &Self::PreparedUpdateBatch<'b>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.counts.update.fetch_add(1, Ordering::Relaxed);
        self.row(state, ordinal, control)?;
        let row = input.selection().row(ordinal).unwrap();
        state.last_row = Some(row);
        for arg in input.logical_arguments() {
            let array = arg.array().as_any().downcast_ref::<Int64Array>().unwrap();
            state.sum += array.value(arg.value_row(ordinal, row));
        }
        for arg in input.order_arguments() {
            let array = arg.array().as_any().downcast_ref::<Int32Array>().unwrap();
            state.last_order = Some(array.value(arg.value_row(ordinal, row)));
        }
        Ok(())
    }
    fn prepare_merge<'b>(
        &'b self,
        input: SelectedAggregateMergeInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.batches.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
    fn merge_row<'b>(
        &self,
        state: &mut State,
        input: &Self::PreparedMergeBatch<'b>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.counts.merge.fetch_add(1, Ordering::Relaxed);
        self.row(state, ordinal, control)?;
        let row = input.selection().row(ordinal).unwrap();
        let arg = input.state();
        state.sum += arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(arg.value_row(ordinal, row));
        state.last_row = Some(row);
        Ok(())
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        State: 's,
        I: ExactSizeIterator<Item = &'s State>,
    {
        self.counts.intermediate.fetch_add(1, Ordering::Relaxed);
        self.emit(states, false, control)
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        State: 's,
        I: ExactSizeIterator<Item = &'s State>,
    {
        self.counts.final_emit.fetch_add(1, Ordering::Relaxed);
        self.emit(states, true, control)
    }
    fn retained_bytes(&self, state: &State) -> usize {
        state.heap.lock().unwrap().capacity()
    }
}
fn operational() -> KernelFailure {
    KernelFailure::Operational(KernelDiagnostic::new("fixture data failure"))
}
fn failures() -> Vec<KernelFailure> {
    vec![
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        operational(),
        internal("fixture fault"),
    ]
}
fn value_array() -> ArrayRef {
    Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))
}

#[repr(align(64))]
struct Storage([MaybeUninit<u8>; 4096]);
impl Storage {
    fn new() -> Self {
        Self([MaybeUninit::new(0xa5); 4096])
    }
}
fn prepared_handle(owner: &Owner, phase: AggregateKernelPhase) -> PreparedAggregateHandle {
    PreparedAggregateHandle::from_typed(
        owner.specialize(phase).into_prepared(),
        &CompileControl::default(),
    )
    .unwrap()
}
fn integers(output: &ArrayRef) -> Vec<i64> {
    if let Some(array) = output.as_any().downcast_ref::<Int64Array>() {
        array.values().to_vec()
    } else {
        output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .map(|v| i64::from(*v))
            .collect()
    }
}

#[test]
fn actual_aligned_layout_short_and_misaligned_storage_are_rejected_before_create() {
    let owner = Owner::new(0, false);
    let handle = prepared_handle(&owner, AggregateKernelPhase::Single);
    let runtime = RuntimeControl::default();
    let mut storage = Storage::new();
    let layout = handle.state_layout();
    assert_eq!(layout, Layout::new::<State>());
    assert_eq!(layout.align(), 64);
    assert!(matches!(
        handle.initialize_in(&mut storage.0[1..1 + layout.size()], &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        handle.initialize_in(&mut storage.0[..layout.size() - 1], &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 0);
    let slot = handle.initialize_in(&mut storage.0, &runtime).unwrap();
    assert_eq!(slot.state_layout(), layout);
    assert_eq!(slot.memory_policy(), handle.memory_policy());
    assert_eq!(slot.retained_heap_bytes(), 0);
    let moved = slot;
    drop(moved);
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    // Only bytes initialized by this fixture, beyond the actual State, are read.
    for byte in &storage.0[layout.size()..] {
        assert_eq!(unsafe { byte.assume_init() }, 0xa5);
    }
    let slot = handle
        .initialize_in(&mut storage.0[..layout.size()], &runtime)
        .unwrap();
    drop(slot);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
}

#[test]
fn cloned_handle_accepts_slots_but_rewrapped_same_kernel_and_foreign_kernel_do_not() {
    let owner = Owner::new(1, false);
    let typed = owner
        .specialize(AggregateKernelPhase::Partial)
        .into_prepared();
    let handle =
        PreparedAggregateHandle::from_typed(typed.clone(), &CompileControl::default()).unwrap();
    let cloned = handle.clone();
    let rewrapped =
        PreparedAggregateHandle::from_typed(typed.clone(), &CompileControl::default()).unwrap();
    let other = Arc::new(Kernel {
        contract: typed.contract.clone(),
        counts: owner.counts.clone(),
        behavior: Behavior::default(),
    });
    let foreign = PreparedAggregateHandle::from_typed(other, &CompileControl::default()).unwrap();
    assert!(Arc::ptr_eq(handle.contract(), rewrapped.contract()));
    assert!(Arc::ptr_eq(handle.contract(), foreign.contract()));
    let mut storage = Storage::new();
    let runtime = RuntimeControl::default();
    let mut slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let input = SelectedAggregateUpdateInput::try_new(
        handle.contract(),
        Selection::all(5),
        &args,
        &[],
        &runtime,
    )
    .unwrap();
    for wrong in [&rewrapped, &foreign] {
        assert!(matches!(
            wrong.prepare_update_batch(&mut slots, &[0; 5], input, &runtime),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            wrong.emit(&slots, &[0], 1, &runtime),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    assert_eq!(owner.counts.batches.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.update.load(Ordering::Relaxed), 0);
    let mut frame = cloned
        .prepare_update_batch(&mut slots, &[0; 5], input, &runtime)
        .unwrap();
    frame.run(&runtime).unwrap();
    assert_eq!(frame.rows_processed(), 5);
    assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
    drop(frame);
    assert_eq!(
        integers(&cloned.emit(&slots, &[0], 1, &runtime).unwrap()),
        [150]
    );
    // The merge ownership check uses an actual merge-phase contract.
    let merge = handle_for_same_owner_merge_probe(&owner, &runtime);
    drop(merge);
}
fn handle_for_same_owner_merge_probe(
    owner: &Owner,
    runtime: &RuntimeControl,
) -> PreparedAggregateHandle {
    let typed = owner
        .specialize(AggregateKernelPhase::Final)
        .into_prepared();
    let handle =
        PreparedAggregateHandle::from_typed(typed.clone(), &CompileControl::default()).unwrap();
    let foreign = PreparedAggregateHandle::from_typed(typed, &CompileControl::default()).unwrap();
    let mut storage = Storage::new();
    let mut slots = [handle.initialize_in(&mut storage.0, runtime).unwrap()];
    let values = value_array();
    let input = SelectedAggregateMergeInput::try_new(
        handle.contract(),
        Selection::all(5),
        EvaluatedArgument::Column(&values),
        runtime,
    )
    .unwrap();
    assert!(matches!(
        foreign.prepare_merge_batch(&mut slots, &[0; 5], input, runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    handle
}

#[test]
fn sparse_repeated_group_mapping_and_emission_order_are_exact_without_aliases() {
    let owner = Owner::new(1, false);
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let rows = [1, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        let handle = prepared_handle(&owner, phase);
        let mut first = Storage::new();
        let mut second = Storage::new();
        let mut slots = [
            handle.initialize_in(&mut first.0, &runtime).unwrap(),
            handle.initialize_in(&mut second.0, &runtime).unwrap(),
        ];
        let map = [0, 0, 1];
        let mut frame = if phase.consumes_logical_arguments() {
            let input = SelectedAggregateUpdateInput::try_new(
                handle.contract(),
                selection,
                &args,
                &[],
                &runtime,
            )
            .unwrap();
            handle
                .prepare_update_batch(&mut slots, &map, input, &runtime)
                .unwrap()
        } else {
            let input = SelectedAggregateMergeInput::try_new(
                handle.contract(),
                selection,
                args[0],
                &runtime,
            )
            .unwrap();
            handle
                .prepare_merge_batch(&mut slots, &map, input, &runtime)
                .unwrap()
        };
        frame.run(&runtime).unwrap();
        assert_eq!(frame.rows_processed(), 3);
        assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
        drop(frame);
        let scale = if phase.produces_final_result() { 10 } else { 1 };
        assert_eq!(
            integers(&handle.emit(&slots, &[1, 0, 1], 3, &runtime).unwrap()),
            [50 * scale, 60 * scale, 50 * scale]
        );
    }
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 4);
}

#[test]
fn all_mapping_errors_including_foreign_suffix_preflight_before_private_work() {
    let owner = Owner::new(1, false);
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        let handle = prepared_handle(&owner, phase);
        let foreign = PreparedAggregateHandle::from_typed(
            owner.specialize(phase).into_prepared(),
            &CompileControl::default(),
        )
        .unwrap();
        let mut first = Storage::new();
        let mut second = Storage::new();
        let mut slots = [
            handle.initialize_in(&mut first.0, &runtime).unwrap(),
            foreign.initialize_in(&mut second.0, &runtime).unwrap(),
        ];
        let before = owner.counts.batches.load(Ordering::Relaxed);
        for mapping in [&[0, 0][..], &[0, 0, 0, 0, 2][..], &[0, 0, 0, 0, 1][..]] {
            let result = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                handle.prepare_update_batch(&mut slots, mapping, input, &runtime)
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                handle.prepare_merge_batch(&mut slots, mapping, input, &runtime)
            };
            assert!(matches!(result, Err(KernelFailure::InvalidProgram(_))));
        }
        assert_eq!(owner.counts.batches.load(Ordering::Relaxed), before);
        for indices in [&[0, 2][..], &[0, 1][..]] {
            assert!(matches!(
                handle.emit(&slots, indices, 2, &runtime),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        assert_eq!(
            integers(&handle.emit(&slots, &[0], 1, &runtime).unwrap()),
            [0]
        );
    }
    assert_eq!(owner.counts.update.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.merge.load(Ordering::Relaxed), 0);
}

#[test]
fn owner_survives_until_unique_typed_state_destruction_and_partial_cleanup_is_exact() {
    let owner = Owner::new(0, false);
    let typed = owner
        .specialize(AggregateKernelPhase::Single)
        .into_prepared();
    let weak = Arc::downgrade(&typed);
    let handle =
        PreparedAggregateHandle::from_typed(typed.clone(), &CompileControl::default()).unwrap();
    let cloned = handle.clone();
    let mut storage = Storage::new();
    let runtime = RuntimeControl::default();
    let slot = handle.initialize_in(&mut storage.0, &runtime).unwrap();
    drop(typed);
    drop(handle);
    drop(cloned);
    assert!(weak.upgrade().is_some());
    assert_eq!(owner.counts.kernel_drop.load(Ordering::Relaxed), 0);
    drop(slot);
    assert!(weak.upgrade().is_none());
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.drop_saw_live_owner.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.kernel_drop.load(Ordering::Relaxed), 1);
    for failure in failures() {
        let mut owner = Owner::new(0, false);
        owner.behavior.create_failure = Some(failure.clone());
        let handle = prepared_handle(&owner, AggregateKernelPhase::Single);
        assert_eq!(
            handle.initialize_in(&mut storage.0, &runtime).unwrap_err(),
            failure
        );
        assert_eq!(owner.counts.create.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    }
    let mut owner = Owner::new(0, false);
    owner.behavior.create_growth = true;
    let handle = prepared_handle(&owner, AggregateKernelPhase::Single);
    assert!(matches!(
        handle.initialize_in(&mut storage.0, &runtime),
        Err(KernelFailure::Internal(_))
    ));
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
}

#[test]
fn typed_failures_after_success_prefix_consume_frame_and_cleanup_all_slots_once() {
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let rows = [1, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let map = [0, 0, 1];
    for failure in failures() {
        for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
            let mut owner = Owner::new(1, false);
            owner.behavior.row_failure = Some((1, failure.clone()));
            let handle = prepared_handle(&owner, phase);
            let mut first = Storage::new();
            let mut second = Storage::new();
            let mut slots = [
                handle.initialize_in(&mut first.0, &runtime).unwrap(),
                handle.initialize_in(&mut second.0, &runtime).unwrap(),
            ];
            let mut frame = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    selection,
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_update_batch(&mut slots, &map, input, &runtime)
                    .unwrap()
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    selection,
                    args[0],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_merge_batch(&mut slots, &map, input, &runtime)
                    .unwrap()
            };
            assert_eq!(frame.run(&runtime), Err(failure.clone()));
            assert_eq!(frame.rows_processed(), 1);
            assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
            assert_eq!(frame.rows_processed(), 1);
            drop(frame);
            let scale = if phase.produces_final_result() { 10 } else { 1 };
            assert_eq!(
                integers(&handle.emit(&slots, &[0, 1], 2, &runtime).unwrap()),
                [20 * scale, 0]
            );
            assert_eq!(
                owner.counts.update.load(Ordering::Relaxed)
                    + owner.counts.merge.load(Ordering::Relaxed),
                2
            );
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
        }
    }
}

#[test]
fn retained_error_growth_and_emission_growth_faults_drop_slots_without_masking_controls() {
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for failure in [
        None,
        Some(operational()),
        Some(KernelFailure::Cancelled),
        Some(KernelFailure::DeadlineExceeded),
        Some(KernelFailure::ResourceExhausted),
    ] {
        for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
            let mut owner = Owner::new(1, false);
            owner.behavior.row_growth = true;
            owner.behavior.row_failure = failure.clone().map(|f| (0, f));
            let handle = prepared_handle(&owner, phase);
            let mut storage = Storage::new();
            let mut slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            let mut frame = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_update_batch(&mut slots, &[0; 5], input, &runtime)
                    .unwrap()
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_merge_batch(&mut slots, &[0; 5], input, &runtime)
                    .unwrap()
            };
            let result = frame.run(&runtime);
            assert_eq!(frame.rows_processed(), 0);
            assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
            drop(frame);
            if let Some(
                error @ (KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted),
            ) = &failure
            {
                assert_eq!(result, Err(error.clone()));
            } else {
                assert!(matches!(result, Err(KernelFailure::Internal(_))));
            }
            assert!(slots[0].retained_heap_bytes() > 0);
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = Emit::Grow(failure.clone());
            let handle = prepared_handle(&owner, phase);
            let slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            let result = handle.emit(&slots, &[0], 1, &runtime);
            if let Some(
                error @ (KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted),
            ) = &failure
            {
                assert_eq!(result.unwrap_err(), *error);
            } else {
                assert!(matches!(result, Err(KernelFailure::Internal(_))));
            }
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn emission_capacity_and_carrier_faults_are_checked_on_actual_erased_results() {
    let runtime = RuntimeControl::default();
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        for emit in [Emit::WrongRows, Emit::WrongType, Emit::Null] {
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = emit;
            let handle = prepared_handle(&owner, phase);
            let mut storage = Storage::new();
            let slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            assert_eq!(
                handle.emit(&slots, &[0], 0, &runtime).unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert_eq!(
                owner.counts.intermediate.load(Ordering::Relaxed)
                    + owner.counts.final_emit.load(Ordering::Relaxed),
                0
            );
            assert!(matches!(
                handle.emit(&slots, &[0], 1, &runtime),
                Err(KernelFailure::Internal(_))
            ));
        }
        for failure in failures() {
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = Emit::Failure(failure.clone());
            let handle = prepared_handle(&owner, phase);
            let mut storage = Storage::new();
            let slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            assert_eq!(handle.emit(&slots, &[0], 1, &runtime).unwrap_err(), failure);
        }
    }
}

#[test]
fn slot_is_send_but_not_sync_or_clone_by_compile_time_ambiguity() {
    fn require_send<T: Send>() {}
    require_send::<AggregateStateSlot<'static>>();
    trait AmbiguousIfSync<A> {
        fn probe() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
    let _ = <AggregateStateSlot<'static> as AmbiguousIfSync<_>>::probe;
    trait AmbiguousIfClone<A> {
        fn probe() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <AggregateStateSlot<'static> as AmbiguousIfClone<_>>::probe;
}

#[test]
fn controls_preflight_large_mapping_before_dispatch_and_consume_interrupted_frames() {
    let runtime = RuntimeControl::default();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1; 300]));
    let args = [EvaluatedArgument::Column(&array)];
    let map = vec![0; 300];
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
            let owner = Owner::new(1, false);
            let handle = prepared_handle(&owner, phase);
            let mut storage = Storage::new();
            let mut slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            for positive in [false, true] {
                let control = RuntimeControl {
                    failure: Some(failure.clone()),
                    positive,
                    ..RuntimeControl::default()
                };
                let result = if phase.consumes_logical_arguments() {
                    let input = SelectedAggregateUpdateInput::try_new(
                        handle.contract(),
                        Selection::all(300),
                        &args,
                        &[],
                        &runtime,
                    )
                    .unwrap();
                    handle.prepare_update_batch(&mut slots, &map, input, &control)
                } else {
                    let input = SelectedAggregateMergeInput::try_new(
                        handle.contract(),
                        Selection::all(300),
                        args[0],
                        &runtime,
                    )
                    .unwrap();
                    handle.prepare_merge_batch(&mut slots, &map, input, &control)
                };
                assert_eq!(result.err(), Some(failure.clone()));
                assert_eq!(owner.counts.batches.load(Ordering::Relaxed), 0);
                assert_eq!(
                    owner.counts.update.load(Ordering::Relaxed)
                        + owner.counts.merge.load(Ordering::Relaxed),
                    0
                );
                if positive {
                    assert!(control.seen.lock().unwrap().contains(&256));
                }
                assert_eq!(
                    handle.emit(&slots, &map, 300, &control).unwrap_err(),
                    failure
                );
                assert_eq!(
                    owner.counts.intermediate.load(Ordering::Relaxed)
                        + owner.counts.final_emit.load(Ordering::Relaxed),
                    0
                );
            }
            for positive in [false, true] {
                let control = RuntimeControl {
                    failure: Some(failure.clone()),
                    positive,
                    ..RuntimeControl::default()
                };
                let mut frame = if phase.consumes_logical_arguments() {
                    let input = SelectedAggregateUpdateInput::try_new(
                        handle.contract(),
                        Selection::all(300),
                        &args,
                        &[],
                        &runtime,
                    )
                    .unwrap();
                    handle
                        .prepare_update_batch(&mut slots, &map, input, &runtime)
                        .unwrap()
                } else {
                    let input = SelectedAggregateMergeInput::try_new(
                        handle.contract(),
                        Selection::all(300),
                        args[0],
                        &runtime,
                    )
                    .unwrap();
                    handle
                        .prepare_merge_batch(&mut slots, &map, input, &runtime)
                        .unwrap()
                };
                assert_eq!(frame.run(&control), Err(failure.clone()));
                assert_eq!(frame.rows_processed(), 0);
                assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
                drop(frame);
                if positive {
                    assert!(control.seen.lock().unwrap().contains(&256));
                }
            }
            assert_eq!(
                integers(&handle.emit(&slots, &[0], 1, &runtime).unwrap()),
                [0]
            );
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

struct RowControl {
    counts: Arc<Counts>,
    failure: KernelFailure,
}
impl KernelEvaluationControl for RowControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        if work > 0
            && self.counts.update.load(Ordering::Relaxed)
                + self.counts.merge.load(Ordering::Relaxed)
                == 2
        {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
#[test]
fn positive_control_after_first_success_preserves_prefix_and_whole_frame_drop() {
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
            let owner = Owner::new(1, false);
            let handle = prepared_handle(&owner, phase);
            let mut first = Storage::new();
            let mut second = Storage::new();
            let mut slots = [
                handle.initialize_in(&mut first.0, &runtime).unwrap(),
                handle.initialize_in(&mut second.0, &runtime).unwrap(),
            ];
            let mapping = [0, 0, 1, 1, 1];
            let mut frame = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_update_batch(&mut slots, &mapping, input, &runtime)
                    .unwrap()
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_merge_batch(&mut slots, &mapping, input, &runtime)
                    .unwrap()
            };
            assert_eq!(
                frame.run(&RowControl {
                    counts: owner.counts.clone(),
                    failure: failure.clone()
                }),
                Err(failure.clone())
            );
            assert_eq!(frame.rows_processed(), 1);
            assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
            drop(frame);
            let scale = if phase.produces_final_result() { 10 } else { 1 };
            assert_eq!(
                integers(&handle.emit(&slots, &[0, 1], 2, &runtime).unwrap()),
                [10 * scale, 0]
            );
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
        }
    }
}

struct FinishControl {
    failure: KernelFailure,
    zeroes: AtomicUsize,
}
impl KernelEvaluationControl for FinishControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        if work == 0 && self.zeroes.fetch_add(1, Ordering::Relaxed) == 2 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
#[test]
fn handle_compile_and_initialization_control_failures_never_publish_invalid_slot() {
    let runtime = RuntimeControl::default();
    let mut storage = Storage::new();
    for (compile, failure) in [
        (CompileControlError::Cancelled, KernelFailure::Cancelled),
        (
            CompileControlError::DeadlineExceeded,
            KernelFailure::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            KernelFailure::ResourceExhausted,
        ),
    ] {
        let owner = Owner::new(0, false);
        let typed = owner
            .specialize(AggregateKernelPhase::Single)
            .into_prepared();
        assert!(
            matches!(PreparedAggregateHandle::from_typed(typed.clone(),&CompileControl{failure:Some(compile),..CompileControl::default()}),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
        let handle =
            PreparedAggregateHandle::from_typed(typed, &CompileControl::default()).unwrap();
        assert_eq!(
            handle
                .initialize_in(
                    &mut storage.0,
                    &RuntimeControl {
                        failure: Some(failure.clone()),
                        ..RuntimeControl::default()
                    }
                )
                .unwrap_err(),
            failure
        );
        assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
        // initialize entry, create entry, then the final create checkpoint fails after typed initialization.
        let control = FinishControl {
            failure: failure.clone(),
            zeroes: AtomicUsize::new(0),
        };
        assert_eq!(
            handle.initialize_in(&mut storage.0, &control).unwrap_err(),
            failure
        );
        assert_eq!(owner.counts.create.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        let slot = handle.initialize_in(&mut storage.0, &runtime).unwrap();
        drop(slot);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
    }
    let owner = Owner::new(0, false);
    let prepared = owner
        .specialize(AggregateKernelPhase::Single)
        .into_prepared();
    let overflow = Arc::new(Kernel {
        contract: prepared.contract.clone(),
        counts: owner.counts.clone(),
        behavior: Behavior {
            bound: usize::MAX,
            ..Behavior::default()
        },
    });
    assert!(matches!(
        PreparedAggregateHandle::from_typed(overflow, &CompileControl::default()),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
}

#[test]
fn empty_frames_skip_private_preparation_are_one_shot_and_phase_mismatch_rejects() {
    let owner = Owner::new(1, false);
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let rows = [];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        let handle = prepared_handle(&owner, phase);
        let mut storage = Storage::new();
        let mut slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
        let before = owner.counts.batches.load(Ordering::Relaxed);
        let mut frame = if phase.consumes_logical_arguments() {
            let input = SelectedAggregateUpdateInput::try_new(
                handle.contract(),
                selection,
                &args,
                &[],
                &runtime,
            )
            .unwrap();
            handle
                .prepare_update_batch(&mut slots, &[], input, &runtime)
                .unwrap()
        } else {
            let input = SelectedAggregateMergeInput::try_new(
                handle.contract(),
                selection,
                args[0],
                &runtime,
            )
            .unwrap();
            handle
                .prepare_merge_batch(&mut slots, &[], input, &runtime)
                .unwrap()
        };
        frame.run(&runtime).unwrap();
        assert_eq!(frame.rows_processed(), 0);
        assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
        drop(frame);
        assert_eq!(owner.counts.batches.load(Ordering::Relaxed), before);
        if phase.consumes_logical_arguments() {
            assert!(matches!(
                SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    selection,
                    args[0],
                    &runtime
                ),
                Err(KernelFailure::InvalidProgram(_))
            ));
        } else {
            assert!(matches!(
                SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    selection,
                    &args,
                    &[],
                    &runtime
                ),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
    }
    assert_eq!(
        owner.counts.update.load(Ordering::Relaxed) + owner.counts.merge.load(Ordering::Relaxed),
        0
    );
}

#[test]
fn immutable_policy_drift_after_create_or_mutation_is_internal_with_primary_control_preserved() {
    let runtime = RuntimeControl::default();
    let mut storage = Storage::new();
    let mut owner = Owner::new(0, false);
    owner.behavior.create_drift = true;
    let handle = prepared_handle(&owner, AggregateKernelPhase::Single);
    assert!(matches!(
        handle.initialize_in(&mut storage.0, &runtime),
        Err(KernelFailure::Internal(_))
    ));
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        for failure in [
            None,
            Some(operational()),
            Some(KernelFailure::Cancelled),
            Some(KernelFailure::DeadlineExceeded),
            Some(KernelFailure::ResourceExhausted),
        ] {
            let mut owner = Owner::new(1, false);
            owner.behavior.row_drift = true;
            owner.behavior.row_failure = failure.clone().map(|error| (0, error));
            let handle = prepared_handle(&owner, phase);
            let mut slots = [handle.initialize_in(&mut storage.0, &runtime).unwrap()];
            let mut frame = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_update_batch(&mut slots, &[0; 5], input, &runtime)
                    .unwrap()
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    handle.contract(),
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                handle
                    .prepare_merge_batch(&mut slots, &[0; 5], input, &runtime)
                    .unwrap()
            };
            let result = frame.run(&runtime);
            if let Some(
                error @ (KernelFailure::Cancelled
                | KernelFailure::DeadlineExceeded
                | KernelFailure::ResourceExhausted),
            ) = failure
            {
                assert_eq!(result, Err(error));
            } else {
                assert!(matches!(result, Err(KernelFailure::Internal(_))));
            }
            assert_eq!(frame.run(&runtime), Err(KernelFailure::InstanceFailed));
            drop(frame);
            assert_eq!(owner.counts.policy_drift.load(Ordering::Relaxed), 1);
            drop(slots);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[repr(align(64))]
struct ZeroState;
static ZERO_CREATE: AtomicUsize = AtomicUsize::new(0);
static ZERO_DROP: AtomicUsize = AtomicUsize::new(0);
impl Drop for ZeroState {
    fn drop(&mut self) {
        ZERO_DROP.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Debug)]
struct ZeroKernel {
    contract: Arc<AggregateCallContract>,
}
impl PreparedAggregateKernel for ZeroKernel {
    type State = ZeroState;
    type PreparedUpdateBatch<'b> = SelectedAggregateUpdateInput<'b, 'b>;
    type PreparedMergeBatch<'b> = SelectedAggregateMergeInput<'b, 'b>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ZeroState, KernelFailure> {
        control.checkpoint(1)?;
        ZERO_CREATE.fetch_add(1, Ordering::Relaxed);
        Ok(ZeroState)
    }
    fn prepare_update<'b>(
        &'b self,
        input: SelectedAggregateUpdateInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        Ok(input)
    }
    fn update_row<'b>(
        &self,
        _: &mut ZeroState,
        _: &Self::PreparedUpdateBatch<'b>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(1)
    }
    fn prepare_merge<'b>(
        &'b self,
        input: SelectedAggregateMergeInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        Ok(input)
    }
    fn merge_row<'b>(
        &self,
        _: &mut ZeroState,
        _: &Self::PreparedMergeBatch<'b>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(1)
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        ZeroState: 's,
        I: ExactSizeIterator<Item = &'s ZeroState>,
    {
        control.checkpoint(1)?;
        Ok(Arc::new(Int64Array::from(vec![0; states.len()])))
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        ZeroState: 's,
        I: ExactSizeIterator<Item = &'s ZeroState>,
    {
        control.checkpoint(1)?;
        Ok(Arc::new(Int32Array::from(vec![0; states.len()])))
    }
    fn retained_bytes(&self, _: &ZeroState) -> usize {
        0
    }
}
#[test]
fn zero_sized_high_alignment_state_requires_actual_alignment_and_drops_exactly_once() {
    ZERO_CREATE.store(0, Ordering::Relaxed);
    ZERO_DROP.store(0, Ordering::Relaxed);
    let owner = Owner::new(0, false);
    let contract = owner
        .specialize(AggregateKernelPhase::Single)
        .prepared()
        .contract
        .clone();
    let handle = PreparedAggregateHandle::from_typed(
        Arc::new(ZeroKernel { contract }),
        &CompileControl::default(),
    )
    .unwrap();
    assert_eq!(handle.state_layout().size(), 0);
    assert_eq!(handle.state_layout().align(), 64);
    let mut storage = Storage::new();
    let runtime = RuntimeControl::default();
    assert!(matches!(
        handle.initialize_in(&mut storage.0[1..1], &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(ZERO_CREATE.load(Ordering::Relaxed), 0);
    let slot = handle.initialize_in(&mut storage.0[..0], &runtime).unwrap();
    assert_eq!(slot.retained_heap_bytes(), 0);
    drop(slot);
    assert_eq!(ZERO_CREATE.load(Ordering::Relaxed), 1);
    assert_eq!(ZERO_DROP.load(Ordering::Relaxed), 1);
}
