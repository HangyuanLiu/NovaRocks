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

//! Actual full-data map_agg consumer witnesses; root runs after owner wiring.
use super::*;
use crate::aggregate_scalar::AggScalarValue as V;
use arrow_array::StringArray;
use arrow_schema::{Field, Fields};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("map_agg never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if *stop == at {
                return Err(cause.clone());
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}
fn kernel(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
) -> MapKernel {
    let catalog = super::super::catalogue::map_agg_private_catalog_for_test();
    let args = [
        FunctionArgument::Value {
            value_type: value,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: key,
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(431),
        domain: EvaluationDomainId::new(29),
        demand: EvaluationDemand::Value,
    };
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(434),
        domain: EvaluationDomainId::new(30),
        demand: EvaluationDemand::Value,
    };
    let state_type = selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let uses = [
        Some(ExpressionUseId::new(432)),
        Some(ExpressionUseId::new(433)),
    ];
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context,
            state_input_type: &state_type,
        }
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                function_id: &bound.function_id,
                kind: FunctionKind::Aggregate,
                selected: &selected,
                request,
                argument_uses,
                context,
                parameters: &parameters,
                environment: &[],
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Domain(context.domain),
            },
            selected.clone(),
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context),
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: (!phase.consumes_logical_arguments())
                        .then_some(state_type.clone()),
                },
            },
            &Compile,
        )
        .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("aggregate handle")
    };
    MapKernel {
        contract: handle.contract().clone(),
    }
}

fn update_selected(
    kernel: &MapKernel,
    state: &mut MapState,
    host: Arc<Host>,
    values: &ArrayRef,
    rates: &ArrayRef,
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let args = [
        EvaluatedArgument::Column(values),
        EvaluatedArgument::Column(rates),
    ];
    let setup = Control::default();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &setup)
            .unwrap();
    let prepared = kernel.prepare_update_evaluation(input, mapping, Some(host), &setup)?;
    for ordinal in 0..selection.len() {
        kernel.update_row_evaluation(state, &prepared, ordinal, control)?;
    }
    Ok(())
}

fn source(array: &ArrayRef) -> FunctionValueType {
    FunctionValueType::new(array.data_type().clone(), true)
}
fn make_kernel(keys: &ArrayRef, values: &ArrayRef, phase: AggregateKernelPhase) -> MapKernel {
    kernel("map_agg", source(keys), source(values), phase)
}
fn emit(
    kernel: &MapKernel,
    state: &MapState,
    host: Arc<Host>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, EvaluationFailure> {
    let allocator: Arc<dyn AggregateStateAllocator> = host;
    let indices = [9];
    let context =
        AggregateEmissionContext::from_host(&kernel.contract, &indices, 32, Some(&allocator));
    if kernel.contract.phase().produces_final_result() {
        kernel.build_final_evaluation_with_context(std::iter::once(state), &context, control)
    } else {
        kernel.build_intermediate_evaluation_with_context(std::iter::once(state), &context, control)
    }
}
fn merge_selected(
    kernel: &MapKernel,
    state: &mut MapState,
    host: Arc<Host>,
    array: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let setup = Control::default();
    let argument = EvaluatedArgument::Column(array);
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(array.len()),
        argument,
        &setup,
    )
    .unwrap();
    let mapping = vec![9; array.len()];
    let prepared = kernel.prepare_merge_evaluation(input, &mapping, Some(host), &setup)?;
    for ordinal in 0..array.len() {
        kernel.merge_row_evaluation(state, &prepared, ordinal, control)?;
    }
    Ok(())
}
fn values() -> (ArrayRef, ArrayRef) {
    (
        Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("a"),
        ])),
        Arc::new(arrow_array::Int64Array::from(vec![
            Some(10),
            Some(80),
            None,
            Some(99),
        ])),
    )
}
#[test]
fn map_owner_four_phases_exact_metadata_first_wins_and_drop() {
    let (keys, values) = values();
    let single = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let partial = make_kernel(&keys, &values, AggregateKernelPhase::Partial);
    let host = Arc::new(Host::default());
    let mut direct = single
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let mut first = partial
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    for (kernel, state) in [(&single, &mut direct), (&partial, &mut first)] {
        update_selected(
            kernel,
            state,
            host.clone(),
            &keys,
            &values,
            Selection::all(4),
            &[9; 4],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(state.core.entries.len(), 2);
    }
    let expected = emit(&single, &direct, host.clone(), &Control::default()).unwrap();
    let intermediate = emit(&partial, &first, host.clone(), &Control::default()).unwrap();
    for phase in [
        AggregateKernelPhase::Final,
        AggregateKernelPhase::Intermediate,
    ] {
        let kernel = make_kernel(&keys, &values, phase);
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        merge_selected(
            &kernel,
            &mut state,
            host.clone(),
            &intermediate,
            &Control::default(),
        )
        .unwrap();
        let actual = emit(&kernel, &state, host.clone(), &Control::default()).unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
        assert_eq!(actual.data_type(), &kernel.contract.final_type().data_type);
    }
    assert_eq!(
        single.retained_bytes(&direct) + partial.retained_bytes(&first),
        host.ledger.lock().unwrap().bytes
    );
    drop(first);
    drop(direct);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_sparse_constant_keys_have_distinct_actual_addresses() {
    let keys = Arc::new(StringArray::from(vec!["fixed"])) as ArrayRef;
    let values = Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = [
        EvaluatedArgument::Scalar(&keys),
        EvaluatedArgument::Column(&values),
    ];
    let control = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        selection,
        &arguments,
        &[],
        &control,
    )
    .unwrap();
    let prepared = kernel
        .prepare_update_evaluation(input, &[9, 9], Some(host.clone()), &control)
        .unwrap();
    for ordinal in 0..2 {
        kernel
            .update_row_evaluation(&mut state, &prepared, ordinal, &control)
            .unwrap();
    }
    assert!(matches!(
        state.core.entries[0].1,
        Some(scalar::TrackedAggScalarValue::Int64(2))
    ));
    drop(prepared);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_null_key_skips_value_but_duplicate_key_keeps_original_data_frontier() {
    let keys = Arc::new(StringArray::from(vec![None, Some("a"), Some("a")])) as ArrayRef;
    let values = Arc::new(arrow_array::UInt32Array::from(vec![Some(8), None, Some(3)])) as ArrayRef;
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::try_sparse(3, &[0, 1]).unwrap(),
        &[9, 9],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(state.core.entries.len(), 1);
    let rows = [2];
    let failure = update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::try_sparse(3, &rows).unwrap(),
        &[9],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("whole original Data")
    };
    assert_eq!(
        data.message(),
        "update aggregate state: unsupported tracked scalar type: UInt32"
    );
    assert_eq!(data.input_rows(), &[2]);
    assert_eq!(data.state_indices(), &[9]);
    assert!(std::ptr::eq(
        data.aggregate_contract(),
        kernel.contract.as_ref()
    ));
    assert!(state.failed && state.core.entries.is_empty());
    assert!(matches!(
        update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &keys,
            &values,
            Selection::all(3),
            &[9; 3],
            &Control::default()
        ),
        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    drop(data);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_final_lazy_unsupported_output_has_actual_emission_domain() {
    let keys = arrow_array::new_null_array(&DataType::UInt32, 1);
    let values = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &Control::default(),
    )
    .unwrap();
    let failure = emit(&kernel, &state, host.clone(), &Control::default()).unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("whole original output Data")
    };
    assert_eq!(
        data.message(),
        "build aggregate final output: unsupported scalar output type: UInt32"
    );
    assert_eq!(data.state_indices(), &[9]);
    assert_eq!(data.input_rows(), &[0]);
    assert_eq!(data.emission_row_capacity(), Some(32));
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Final);
    drop(data);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
fn long_bad() -> (ArrayRef, ArrayRef) {
    let child = Arc::new(Field::new("x".repeat(700), DataType::Int64, true));
    let values = Arc::new(
        arrow_array::FixedSizeListArray::try_new(
            child,
            1,
            Arc::new(arrow_array::Int64Array::from(vec![1])),
            None,
        )
        .unwrap(),
    ) as ArrayRef;
    (Arc::new(StringArray::from(vec!["a"])) as ArrayRef, values)
}
#[test]
fn map_owner_long_original_actual_carrier_debug_is_not_clipped() {
    let (keys, values) = long_bad();
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let failure = update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("whole full original diagnostic")
    };
    let expected = format!(
        "update aggregate state: unsupported tracked scalar type: {:?}",
        values.data_type()
    );
    assert!(expected.len() > 512);
    assert_eq!(data.message(), expected);
    let clone = data.clone();
    drop(data);
    drop(state);
    assert!(host.ledger.lock().unwrap().bytes > 0);
    drop(clone);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_each_update_callback_and_real_allocation_keeps_all_seven_causes() {
    let text = "é中🙂".repeat(100);
    let keys = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
    let values = Arc::new(arrow_array::Int64Array::from(vec![8])) as ArrayRef;
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let reference = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(reference.clone()), &Control::default())
        .unwrap();
    let start = reference.ledger.lock().unwrap().attempts;
    let control = Control::default();
    update_selected(
        &kernel,
        &mut state,
        reference.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &control,
    )
    .unwrap();
    let allocations = reference.ledger.lock().unwrap().attempts - start;
    let callbacks = control.trace.lock().unwrap().len();
    assert!(control.trace.lock().unwrap().contains(&256));
    drop(state);
    assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
    for cause in causes() {
        for callback in 0..callbacks {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((callback, cause.clone())),
            };
            let failure = update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &keys,
                &values,
                Selection::all(1),
                &[9],
                &control,
            )
            .unwrap_err();
            assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
            assert_eq!(control.trace.lock().unwrap().len(), callback + 1);
            assert!(state.failed && state.core.entries.is_empty());
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
        for offset in 0..allocations {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            arm_refusal(&host, offset, cause.clone());
            let failure = update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &keys,
                &values,
                Selection::all(1),
                &[9],
                &Control::default(),
            )
            .unwrap_err();
            assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn map_owner_real_diagnostic_allocation_causes_and_missing_host_are_not_data() {
    let (keys, values) = long_bad();
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    assert!(matches!(
        kernel.create_state(&Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let start = host.ledger.lock().unwrap().attempts;
    let failure = update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &Control::default(),
    )
    .unwrap_err();
    let attempts = host.ledger.lock().unwrap().attempts - start;
    assert!(matches!(failure, EvaluationFailure::InvocationData(_)));
    drop(failure);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for cause in causes() {
        for offset in 0..attempts {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            arm_refusal(&host, offset, cause.clone());
            let failure = update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &keys,
                &values,
                Selection::all(1),
                &[9],
                &Control::default(),
            )
            .unwrap_err();
            assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn map_owner_each_merge_and_emit_callback_has_no_tail_and_drop() {
    let text = "中🙂".repeat(70);
    let keys = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
    let values = Arc::new(arrow_array::Int64Array::from(vec![8])) as ArrayRef;
    let partial = make_kernel(&keys, &values, AggregateKernelPhase::Partial);
    let final_kernel = make_kernel(&keys, &values, AggregateKernelPhase::Final);
    let host = Arc::new(Host::default());
    let mut state = partial
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &partial,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &Control::default(),
    )
    .unwrap();
    let incoming = emit(&partial, &state, host.clone(), &Control::default()).unwrap();
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for emission in [false, true] {
        let reference = Arc::new(Host::default());
        let mut state = final_kernel
            .create_state_with_allocator(Some(reference.clone()), &Control::default())
            .unwrap();
        let trace = Control::default();
        if emission {
            merge_selected(
                &final_kernel,
                &mut state,
                reference.clone(),
                &incoming,
                &Control::default(),
            )
            .unwrap();
            emit(&final_kernel, &state, reference.clone(), &trace).unwrap();
        } else {
            merge_selected(
                &final_kernel,
                &mut state,
                reference.clone(),
                &incoming,
                &trace,
            )
            .unwrap();
        }
        let checkpoints = trace.trace.lock().unwrap().len();
        assert!(checkpoints > 0);
        drop(state);
        assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
        for cause in causes() {
            for callback in 0..checkpoints {
                let host = Arc::new(Host::default());
                let mut state = final_kernel
                    .create_state_with_allocator(Some(host.clone()), &Control::default())
                    .unwrap();
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((callback, cause.clone())),
                };
                let failure = if emission {
                    merge_selected(
                        &final_kernel,
                        &mut state,
                        host.clone(),
                        &incoming,
                        &Control::default(),
                    )
                    .unwrap();
                    emit(&final_kernel, &state, host.clone(), &control).map(|_| ())
                } else {
                    merge_selected(&final_kernel, &mut state, host.clone(), &incoming, &control)
                }
                .unwrap_err();
                assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
                assert_eq!(control.trace.lock().unwrap().len(), callback + 1);
                if !emission {
                    assert!(state.failed && state.core.entries.is_empty());
                }
                drop(state);
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
#[test]
fn map_owner_data_construction_checkpoints_do_not_add_optional_footer() {
    let (keys, values) = long_bad();
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let reference = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(reference.clone()), &Control::default())
        .unwrap();
    let trace = Control::default();
    let failure = update_selected(
        &kernel,
        &mut state,
        reference.clone(),
        &keys,
        &values,
        Selection::all(1),
        &[9],
        &trace,
    )
    .unwrap_err();
    let checkpoints = trace.trace.lock().unwrap().len();
    assert!(matches!(failure, EvaluationFailure::InvocationData(_)));
    drop(failure);
    drop(state);
    assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
    for cause in causes() {
        for callback in 0..checkpoints {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((callback, cause.clone())),
            };
            let failure = update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &keys,
                &values,
                Selection::all(1),
                &[9],
                &control,
            )
            .unwrap_err();
            assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
            assert_eq!(control.trace.lock().unwrap().len(), callback + 1);
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
    // The first callback beyond the actual failure prefix is unreachable.
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let control = Control {
        trace: Mutex::new(vec![]),
        refusal: Some((checkpoints, KernelFailure::Cancelled)),
    };
    assert!(matches!(
        update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &keys,
            &values,
            Selection::all(1),
            &[9],
            &control
        ),
        Err(EvaluationFailure::InvocationData(_))
    ));
    assert_eq!(control.trace.lock().unwrap().len(), checkpoints);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_actual_pool_ordinal_is_broadcast_without_consuming_other_values() {
    let keys = Arc::new(StringArray::from(vec!["unused", "fixed", "unused-tail"])) as ArrayRef;
    let values = Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let key_type = source(&keys);
    let policy = ConstantPolicy {
        max_rows: 32,
        max_array_nodes: 512,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4 * 1024 * 1024,
        max_library_validation_work: 128 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    };
    let pool = ConstantPool::try_new(
        Arc::new(key_type.try_to_field("original-key").unwrap()),
        key_type,
        keys.to_data(),
        policy,
        CompilePhase::Validate,
        &Compile,
    )
    .unwrap();
    let key = pool.value(1).unwrap();
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let arguments = [
        EvaluatedArgument::Constant(&key),
        EvaluatedArgument::Column(&values),
    ];
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let control = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        selection,
        &arguments,
        &[],
        &control,
    )
    .unwrap();
    let prepared = kernel
        .prepare_update_evaluation(input, &[9, 9], Some(host.clone()), &control)
        .unwrap();
    for ordinal in 0..2 {
        kernel
            .update_row_evaluation(&mut state, &prepared, ordinal, &control)
            .unwrap();
    }
    let scalar::TrackedAggScalarValue::Utf8(key) = &state.core.entries[0].0 else {
        panic!("original key scalar")
    };
    assert_eq!(key.as_slice(), b"fixed");
    assert!(matches!(
        state.core.entries[0].1,
        Some(scalar::TrackedAggScalarValue::Int64(2))
    ));
    drop(prepared);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_constructor_actual_metadata_refusal_keeps_every_cause() {
    let (keys, values) = values();
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    for cause in causes() {
        let host = Arc::new(Host::default());
        arm_refusal(&host, 0, cause.clone());
        let result = kernel.create_state_with_allocator(Some(host.clone()), &Control::default());
        assert!(matches!(result, Err(actual) if actual == cause));
        assert_eq!(host.ledger.lock().unwrap().attempts, 1);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[derive(Default)]
struct CompilationTrace {
    checks: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompilationTrace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut checks = self.checks.lock().unwrap();
        let at = checks.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "no compile tail after first cause");
        }
        checks.push((phase, units));
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[test]
fn map_owner_actual_local_phase_clone_has_original_identity_and_compile_causes() {
    let (keys, values) = values();
    let single = make_kernel(&keys, &values, AggregateKernelPhase::Single);
    let (partial, final_stage) =
        AggregateCallContract::local_stages(&single.contract, &Compile).unwrap();
    for contract in [partial, final_stage] {
        let reference = CompilationTrace::default();
        let cloned = single
            .clone_for_local_phase(contract.clone(), &reference)
            .unwrap();
        assert!(Arc::ptr_eq(cloned.contract(), &contract));
        assert!(Arc::ptr_eq(
            cloned.contract().call(),
            single.contract.call()
        ));
        let checkpoints = reference.checks.lock().unwrap().len();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for callback in 0..checkpoints {
                let control = CompilationTrace {
                    checks: Mutex::new(vec![]),
                    refusal: Some((callback, cause)),
                };
                let failure = single
                    .clone_for_local_phase(contract.clone(), &control)
                    .unwrap_err();
                assert_eq!(failure, compile_failure(cause));
                assert_eq!(control.checks.lock().unwrap().len(), callback + 1);
            }
        }
    }
}
#[test]
fn map_owner_merge_null_root_skips_bad_value_and_nonnull_row_reports_full_data() {
    let keys = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
    let values = Arc::new(arrow_array::UInt32Array::from(vec![1, 2])) as ArrayRef;
    let kernel = make_kernel(&keys, &values, AggregateKernelPhase::Final);
    let DataType::Map(field, ordered) = &kernel.contract.intermediate_type().data_type else {
        panic!("original Map state")
    };
    let DataType::Struct(fields) = field.data_type() else {
        panic!("original entries")
    };
    let entries = arrow_array::StructArray::new(fields.clone(), vec![keys, values], None);
    let input = Arc::new(
        arrow_array::MapArray::try_new(
            field.clone(),
            arrow_buffer::OffsetBuffer::new(vec![0i32, 1, 2].into()),
            entries,
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
            *ordered,
        )
        .unwrap(),
    ) as ArrayRef;
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let failure = merge_selected(
        &kernel,
        &mut state,
        host.clone(),
        &input,
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("whole merge Data")
    };
    assert_eq!(
        data.message(),
        "merge aggregate state: unsupported tracked scalar type: UInt32"
    );
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
    assert_eq!(data.input_rows(), &[0, 1]);
    assert_eq!(data.state_indices(), &[9, 9]);
    assert!(state.failed && state.core.entries.is_empty());
    drop(data);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn map_owner_real_merge_and_emission_allocation_frontiers_keep_seven_causes() {
    let (keys, values) = values();
    let partial = make_kernel(&keys, &values, AggregateKernelPhase::Partial);
    let final_kernel = make_kernel(&keys, &values, AggregateKernelPhase::Final);
    let host = Arc::new(Host::default());
    let mut state = partial
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &partial,
        &mut state,
        host.clone(),
        &keys,
        &values,
        Selection::all(4),
        &[9; 4],
        &Control::default(),
    )
    .unwrap();
    let input = emit(&partial, &state, host.clone(), &Control::default()).unwrap();
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for emission in [false, true] {
        let reference = Arc::new(Host::default());
        let mut state = final_kernel
            .create_state_with_allocator(Some(reference.clone()), &Control::default())
            .unwrap();
        if emission {
            merge_selected(
                &final_kernel,
                &mut state,
                reference.clone(),
                &input,
                &Control::default(),
            )
            .unwrap();
        }
        let before = reference.ledger.lock().unwrap().attempts;
        if emission {
            emit(
                &final_kernel,
                &state,
                reference.clone(),
                &Control::default(),
            )
            .unwrap();
        } else {
            merge_selected(
                &final_kernel,
                &mut state,
                reference.clone(),
                &input,
                &Control::default(),
            )
            .unwrap();
        }
        let attempts = reference.ledger.lock().unwrap().attempts - before;
        drop(state);
        assert_eq!(reference.ledger.lock().unwrap().bytes, 0);
        for cause in causes() {
            for offset in 0..attempts {
                let host = Arc::new(Host::default());
                let mut state = final_kernel
                    .create_state_with_allocator(Some(host.clone()), &Control::default())
                    .unwrap();
                if emission {
                    merge_selected(
                        &final_kernel,
                        &mut state,
                        host.clone(),
                        &input,
                        &Control::default(),
                    )
                    .unwrap();
                }
                arm_refusal(&host, offset, cause.clone());
                let failure = if emission {
                    emit(&final_kernel, &state, host.clone(), &Control::default()).map(|_| ())
                } else {
                    merge_selected(
                        &final_kernel,
                        &mut state,
                        host.clone(),
                        &input,
                        &Control::default(),
                    )
                }
                .unwrap_err();
                assert_eq!(failure, EvaluationFailure::Kernel(cause.clone()));
                drop(state);
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}

// This proves the exact nominal core capability independently of the legacy
// registered admission drift. It does not assert native v1 availability.
#[test]
fn map_owner_nominal_largeint_four_phases_keep_exact_child_metadata() {
    for nullable in [false, true] {
        let source_type = FunctionValueType {
            logical_type: novarocks_type_contract::ValueLogicalType::LargeInt,
            ..FunctionValueType::new(DataType::FixedSizeBinary(16), nullable)
        };
        let input: ArrayRef = crate::largeint::array_from_i128(&[
            Some(i128::MIN),
            Some(i128::MAX),
            Some(i128::MIN),
            if nullable { None } else { Some(0) },
        ])
        .unwrap();
        let single = kernel(
            "map_agg",
            source_type.clone(),
            source_type.clone(),
            AggregateKernelPhase::Single,
        );
        let partial = kernel(
            "map_agg",
            source_type.clone(),
            source_type.clone(),
            AggregateKernelPhase::Partial,
        );
        let host = Arc::new(Host::default());
        let control = Control::default();
        let mut direct = single
            .create_state_with_allocator(Some(host.clone()), &control)
            .unwrap();
        let mut first = partial
            .create_state_with_allocator(Some(host.clone()), &control)
            .unwrap();
        for (kernel, state) in [(&single, &mut direct), (&partial, &mut first)] {
            update_selected(
                kernel,
                state,
                host.clone(),
                &input,
                &input,
                Selection::all(4),
                &[9; 4],
                &control,
            )
            .unwrap();
        }
        let expected = emit(&single, &direct, host.clone(), &control).unwrap();
        let intermediate = emit(&partial, &first, host.clone(), &control).unwrap();
        let DataType::Map(entries, _) = expected.data_type() else {
            panic!("exact Map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("exact entries");
        };
        for field in fields {
            assert_eq!(
                field.metadata().get("nr_logical_type").map(String::as_str),
                Some("largeint")
            );
        }
        for phase in [
            AggregateKernelPhase::Final,
            AggregateKernelPhase::Intermediate,
        ] {
            let kernel = kernel("map_agg", source_type.clone(), source_type.clone(), phase);
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &control)
                .unwrap();
            merge_selected(&kernel, &mut state, host.clone(), &intermediate, &control).unwrap();
            assert_eq!(
                emit(&kernel, &state, host.clone(), &control)
                    .unwrap()
                    .to_data(),
                expected.to_data()
            );
        }
        drop(first);
        drop(direct);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
