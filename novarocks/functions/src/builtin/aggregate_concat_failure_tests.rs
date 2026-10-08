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

//! Actual selected CONCAT phases and owned allocation/control refusals.
use super::*;

use arrow_array::StringArray;

use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
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
        panic!("GROUP_CONCAT never waits")
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
fn assert_metadata_only(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    let metadata = ledger
        .metadata
        .expect("successful constructor allocated metadata");
    assert_eq!(ledger.live.as_slice(), &[metadata]);
    assert_eq!(ledger.bytes, metadata.1.size());
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
fn kernel_with_source(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
    source: Option<novarocks_type_contract::AggregateStateInterpretation>,
) -> Result<ConcatKernel, FunctionSpecializationFailure> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
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
    let parameters = SemanticParameters::try_new([
        (
            SemanticParameterId::new(91),
            SemanticParameterValue::GroupConcatLegacy(false),
        ),
        (
            SemanticParameterId::new(23),
            SemanticParameterValue::GroupConcatMaxLen(1024),
        ),
    ])
    .unwrap();
    let environment = [
        SemanticParameterRef {
            id: SemanticParameterId::new(91),
            expected_key: SemanticParameterKey::GroupConcatLegacy,
        },
        SemanticParameterRef {
            id: SemanticParameterId::new(23),
            expected_key: SemanticParameterKey::GroupConcatMaxLen,
        },
    ];
    let prepared = catalog.prepare_fresh_selected(
        CallEffectInput {
            function_id: &bound.function_id,
            kind: FunctionKind::Aggregate,
            selected: &selected,
            request,
            argument_uses,
            context,
            parameters: &parameters,
            environment: &environment,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(context.domain),
        },
        selected.clone(),
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context),
            options: AggregatePreparationOptions {
                state_interpretation: source.clone().map(Arc::new),
                phase,
                distinct: phase.consumes_logical_arguments()
                    && source.as_ref().is_some_and(|source| source.distinct),
                order_keys: Arc::from([]),
                state_input_type: (!phase.consumes_logical_arguments())
                    .then_some(state_type.clone()),
            },
        },
        &Compile,
    )?;
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("aggregate handle")
    };
    Ok(ConcatKernel {
        contract: handle.contract().clone(),
        ascending: Box::new([]),
        nulls_first: Box::new([]),
        max_len: 1024,
    })
}
fn kernel(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
) -> ConcatKernel {
    kernel_with_source(
        name,
        value,
        key,
        phase,
        Some(novarocks_type_contract::AggregateStateInterpretation {
            distinct: false,
            order_keys: Box::new([]),
        }),
    )
    .unwrap()
}
fn text(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}
fn text_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, false)
}
fn new_state(kernel: &ConcatKernel, host: Arc<Host>) -> GroupConcatState<HostAggregateAllocator> {
    kernel
        .create_state_with_allocator(Some(host), &Control::default())
        .unwrap()
}
fn update(
    kernel: &ConcatKernel,
    state: &mut GroupConcatState<HostAggregateAllocator>,
    values: &ArrayRef,
    keys: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let args = [
        EvaluatedArgument::Column(values),
        EvaluatedArgument::Column(keys),
    ];
    let setup = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        Selection::all(1),
        &args,
        &[],
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_update(input, &setup).unwrap();
    kernel.update_row(state, &input, 0, control)
}
fn merge(
    kernel: &ConcatKernel,
    state: &mut GroupConcatState<HostAggregateAllocator>,
    values: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let setup = Control::default();
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(1),
        EvaluatedArgument::Column(values),
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_merge(input, &setup).unwrap();
    kernel.merge_row(state, &input, 0, control)
}
fn assert_released(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
fn assert_failed(kernel: &ConcatKernel, state: &GroupConcatState<HostAggregateAllocator>) {
    assert!(state.failed);
    assert!(state.rows.is_empty());
    assert_eq!(
        kernel.retained_bytes(state),
        state.allocator.metadata_bytes()
    );
    assert!(matches!(
        kernel.build_final(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn concat_plain_actual_owner_all_four_phases_preserve_raw_rows_and_memory() {
    for name in ["group_concat", "string_agg"] {
        let host = Arc::new(Host::default());
        let single = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        let mut direct = new_state(&single, host.clone());
        for (value, sep) in [("a", "/"), ("b", "|"), ("c", ",")] {
            update(
                &single,
                &mut direct,
                &text(value),
                &text(sep),
                &Control::default(),
            )
            .unwrap();
        }
        assert_eq!(
            single.retained_bytes(&direct),
            host.ledger.lock().unwrap().bytes
        );
        let expected = single
            .build_final(std::iter::once(&direct), &Control::default())
            .unwrap();
        let partial = kernel(
            name,
            text_type(),
            text_type(),
            AggregateKernelPhase::Partial,
        );
        let wire = partial
            .build_intermediate(std::iter::once(&direct), &Control::default())
            .unwrap();
        let middle = kernel(
            name,
            text_type(),
            text_type(),
            AggregateKernelPhase::Intermediate,
        );
        let mut accumulated = new_state(&middle, host.clone());
        merge(&middle, &mut accumulated, &wire, &Control::default()).unwrap();
        let wire2 = middle
            .build_intermediate(std::iter::once(&accumulated), &Control::default())
            .unwrap();
        assert_eq!(wire.to_data(), wire2.to_data());
        let final_kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Final);
        let mut final_state = new_state(&final_kernel, host.clone());
        merge(&final_kernel, &mut final_state, &wire2, &Control::default()).unwrap();
        let actual = final_kernel
            .build_final(std::iter::once(&final_state), &Control::default())
            .unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
        assert_eq!(
            actual
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a/b|c"
        );
        drop((direct, accumulated, final_state));
        assert_released(&host);
    }
}
#[test]
fn concat_every_owned_update_allocation_refusal_preserves_cause_and_releases_graph() {
    for name in ["group_concat", "string_agg"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        let probe = Arc::new(Host::default());
        let mut state = new_state(&kernel, probe.clone());
        let first = probe.ledger.lock().unwrap().attempts;
        update(
            &kernel,
            &mut state,
            &text("value"),
            &text("separator"),
            &Control::default(),
        )
        .unwrap();
        let allocations = probe.ledger.lock().unwrap().attempts - first;
        assert!(allocations >= 4);
        drop(state);
        assert_released(&probe);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    update(
                        &kernel,
                        &mut state,
                        &text("value"),
                        &text("separator"),
                        &Control::default()
                    ),
                    Err(cause)
                );
                assert_failed(&kernel, &state);
                assert_metadata_only(&host);
                let attempts = host.ledger.lock().unwrap().attempts;
                assert_eq!(
                    update(
                        &kernel,
                        &mut state,
                        &text("retry"),
                        &text(","),
                        &Control::default()
                    ),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn concat_every_update_checkpoint_keeps_primary_typed_cause_and_failed_latch() {
    for name in ["group_concat", "string_agg"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        let host = Arc::new(Host::default());
        let mut probe = new_state(&kernel, host.clone());
        let success = Control::default();
        update(
            &kernel,
            &mut probe,
            &text("value"),
            &text("separator"),
            &success,
        )
        .unwrap();
        let checkpoints = success.trace.lock().unwrap().len();
        drop(probe);
        assert_released(&host);
        for stop in 0..checkpoints {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    update(
                        &kernel,
                        &mut state,
                        &text("value"),
                        &text("separator"),
                        &control
                    ),
                    Err(cause)
                );
                assert_failed(&kernel, &state);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn concat_core_explicit_order_distinct_policy_survives_raw_state_reemission() {
    // This tests the shared algorithm's explicit policy. It does not assert
    // that the runtime currently authors ordered merge interpretation facts.
    let host = Arc::new(Host::default());
    let alloc = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut direct = GroupConcatState::new(alloc.clone());
    let mut work = ScalarWork::new(None);
    let ty =
        core::build_default_intermediate_type(&[DataType::Utf8, DataType::Utf8, DataType::Int64]);
    let columns = [
        text("x"),
        text("/"),
        Arc::new(arrow_array::Int64Array::from(vec![2])) as ArrayRef,
    ];
    let early = [
        text("x"),
        text("|"),
        Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
    ];
    let later = [
        text("y"),
        text("-"),
        Arc::new(arrow_array::Int64Array::from(vec![3])) as ArrayRef,
    ];
    let layout = GroupConcatLayout::infer(3, 1).unwrap();
    for columns in [&columns, &early, &later] {
        let rows = columns.iter().map(|column| (column, 0)).collect::<Vec<_>>();
        direct.update_row(&rows, layout, &mut work).unwrap();
    }
    let wire = core::build_intermediate_array(&ty, std::iter::once(&direct), &mut work).unwrap();
    let mut accumulated = GroupConcatState::new(alloc.clone());
    core::GroupConcatMerge::new(&wire, 1, &mut work)
        .unwrap()
        .merge_row(&mut accumulated, 0, &ty, &mut work)
        .unwrap();
    let wire2 =
        core::build_intermediate_array(&ty, std::iter::once(&accumulated), &mut work).unwrap();
    let mut final_state = GroupConcatState::new(alloc.clone());
    core::GroupConcatMerge::new(&wire2, 1, &mut work)
        .unwrap()
        .merge_row(&mut final_state, 0, &ty, &mut work)
        .unwrap();
    let output = core::build_final_array(
        &ty,
        std::iter::once(&final_state),
        true,
        &[true],
        &[false],
        1024,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "x/y"
    );
    drop((direct, accumulated, final_state, alloc));
    assert_released(&host);
}

#[test]
fn concat_all_phases_require_explicit_state_interpretation_even_plain_final() {
    for name in ["group_concat", "string_agg"] {
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let error =
                kernel_with_source(name, text_type(), text_type(), phase, None).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("group_concat requires its original state interpretation facts"),
                "{error}"
            );
        }
    }
}
#[test]
fn concat_distinct_final_consumes_producer_receipt_with_empty_merge_flags() {
    for name in ["group_concat", "string_agg"] {
        let receipt = || {
            Some(novarocks_type_contract::AggregateStateInterpretation {
                distinct: true,
                order_keys: Box::new([]),
            })
        };
        let partial = kernel_with_source(
            name,
            text_type(),
            text_type(),
            AggregateKernelPhase::Partial,
            receipt(),
        )
        .unwrap();
        let middle = kernel_with_source(
            name,
            text_type(),
            text_type(),
            AggregateKernelPhase::Intermediate,
            receipt(),
        )
        .unwrap();
        let final_kernel = kernel_with_source(
            name,
            text_type(),
            text_type(),
            AggregateKernelPhase::Final,
            receipt(),
        )
        .unwrap();
        assert!(!middle.contract.distinct());
        assert!(!final_kernel.contract.distinct());
        assert!(final_kernel.contract.order_keys().is_empty());
        assert!(
            final_kernel
                .contract
                .state_interpretation()
                .unwrap()
                .distinct
        );
        let host = Arc::new(Host::default());
        let mut left = new_state(&partial, host.clone());
        let mut right = new_state(&partial, host.clone());
        for (value, sep) in [("a", ","), ("b", "/")] {
            update(
                &partial,
                &mut left,
                &text(value),
                &text(sep),
                &Control::default(),
            )
            .unwrap();
        }
        update(
            &partial,
            &mut right,
            &text("a"),
            &text("|"),
            &Control::default(),
        )
        .unwrap();
        let l = partial
            .build_intermediate(std::iter::once(&left), &Control::default())
            .unwrap();
        let r = partial
            .build_intermediate(std::iter::once(&right), &Control::default())
            .unwrap();
        let mut joined = new_state(&middle, host.clone());
        for wire in [&l, &r] {
            merge(&middle, &mut joined, wire, &Control::default()).unwrap();
        }
        let wire = middle
            .build_intermediate(std::iter::once(&joined), &Control::default())
            .unwrap();
        let mut result = new_state(&final_kernel, host.clone());
        merge(&final_kernel, &mut result, &wire, &Control::default()).unwrap();
        let output = final_kernel
            .build_final(std::iter::once(&result), &Control::default())
            .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "b/a"
        );
        drop((left, right, joined, result));
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
