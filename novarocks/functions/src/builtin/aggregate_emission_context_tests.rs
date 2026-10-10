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

//! Emission provenance and real diagnostic construction, not percentile mathematics.
use super::*;
use crate::aggregate_invocation_backing::HostDiagnostic;
use crate::kernel_input::EvaluationCheckpoints;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct EmissionProbe {
    contract: Arc<AggregateCallContract>,
    published: Arc<AtomicBool>,
}
impl EmissionProbe {
    fn emit<I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = ()>,
    {
        assert_eq!(states.len(), context.state_indices().len());
        assert!(Arc::ptr_eq(&self.contract, context.contract()));
        let host = context
            .allocator()
            .ok_or_else(|| invalid("emission diagnostic requires actual host allocator"))?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(host))?;
        let mut work = EvaluationCheckpoints::new(control);
        let diagnostic =
            HostDiagnostic::prepare(&allocator, &mut work, |out| out.write_str(EMISSION_MESSAGE))?;
        let data = InvocationData::prepare_emission(&allocator, context, diagnostic, &mut work)?;
        self.published.store(true, Ordering::SeqCst);
        Err(data.into())
    }
}
const EMISSION_MESSAGE: &str = concat!(
    "original emission data: ",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
);
impl PreparedAggregateKernel for EmissionProbe {
    type State = ();
    type PreparedUpdateBatch<'a> = ();
    type PreparedMergeBatch<'a> = ();
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn create_state(&self, _: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn has_invocation_data(&self) -> bool {
        true
    }
    fn requires_emission_context(&self) -> bool {
        true
    }
    fn prepare_update<'a>(
        &'a self,
        _: SelectedAggregateUpdateInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn update_row<'a>(
        &self,
        _: &mut (),
        _: &(),
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn merge_row<'a>(
        &self,
        _: &mut (),
        _: &(),
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn build_intermediate<'a, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a ()>,
    {
        panic!("context-free intermediate builder must not run")
    }
    fn build_final<'a, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a ()>,
    {
        panic!("context-free final builder must not run")
    }
    fn build_intermediate_evaluation_with_context<'a, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'a ()>,
    {
        self.emit(states.copied(), context, control)
    }
    fn build_final_evaluation_with_context<'a, I>(
        &self,
        states: I,
        context: &AggregateEmissionContext<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, EvaluationFailure>
    where
        I: ExactSizeIterator<Item = &'a ()>,
    {
        self.emit(states.copied(), context, control)
    }
    fn retained_bytes(&self, _: &()) -> usize {
        0
    }
}
struct NoDataFooter<'a> {
    published: &'a AtomicBool,
    inner: Control,
}
impl KernelEvaluationControl for NoDataFooter<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(
            !self.published.load(Ordering::SeqCst),
            "callback after published invocation Data"
        );
        self.inner.checkpoint(units)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("emission never waits")
    }
}
fn probe(
    phase: AggregateKernelPhase,
    host: Arc<Host>,
) -> (AggregateStateColumn, Arc<EmissionProbe>) {
    let original = kernel("ndv", FunctionValueType::new(DataType::Utf8, true), phase);
    let probe = Arc::new(EmissionProbe {
        contract: original.contract,
        published: Arc::new(AtomicBool::new(false)),
    });
    let handle = PreparedAggregateHandle::from_typed(probe.clone(), &Compile).unwrap();
    let mut column =
        AggregateStateColumn::try_new(handle, host, std::num::NonZeroUsize::new(2).unwrap())
            .unwrap();
    for _ in 0..3 {
        column.push(&Control::default()).unwrap();
    }
    (column, probe)
}
#[test]
fn emission_actual_column_receipt_phase_identity_order_capacity_and_last_drop() {
    for (phase, expected) in [
        (
            AggregateKernelPhase::Single,
            AggregateInvocationPhase::Final,
        ),
        (
            AggregateKernelPhase::Partial,
            AggregateInvocationPhase::Intermediate,
        ),
    ] {
        for indices in [vec![2, 0, 2], vec![]] {
            let host = Arc::new(Host::default());
            let (column, kernel) = probe(phase, host.clone());
            let control = NoDataFooter {
                published: &kernel.published,
                inner: Control::default(),
            };
            let Err(EvaluationFailure::InvocationData(data)) =
                column.emit_evaluation(&indices, 7, &control)
            else {
                panic!("full Data")
            };
            assert_eq!(data.message(), EMISSION_MESSAGE);
            assert!(data.message().len() > 512);
            assert!(std::ptr::eq(
                data.aggregate_contract(),
                column.handle().contract().as_ref()
            ));
            assert_eq!(data.aggregate_phase(), expected);
            assert_eq!(data.batch_rows(), indices.len());
            assert_eq!(data.emission_row_capacity(), Some(7));
            assert_eq!(data.input_rows(), (0..indices.len()).collect::<Vec<_>>());
            assert_eq!(data.state_indices(), indices);
            let attempts = host.ledger.lock().unwrap().attempts;
            let loan = data.clone();
            assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
            drop(column);
            drop(kernel);
            drop(data);
            assert_eq!(host.ledger.lock().unwrap().bytes, loan.retained_bytes());
            drop(loan);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn emission_contextless_typed_entry_refuses_only_new_required_protocol() {
    let host = Arc::new(Host::default());
    let (_column, prepared) = probe(AggregateKernelPhase::Single, host);
    let state = [()];
    let result = emit_aggregate_evaluation(prepared.as_ref(), state.iter(), 1, &Control::default());
    assert_eq!(
        result.unwrap_err(),
        EvaluationFailure::Kernel(invalid(
            "aggregate emission requires its actual host context"
        ))
    );
    assert!(!prepared.published.load(Ordering::SeqCst));
    let old = kernel(
        "ndv",
        FunctionValueType::new(DataType::Utf8, true),
        AggregateKernelPhase::Single,
    );
    let state = old
        .create_state_with_allocator(Some(Arc::new(Host::default())), &Control::default())
        .unwrap();
    let original = Control::default();
    let contextual = Control::default();
    let out = emit_aggregate_evaluation(&old, [&state].into_iter(), 1, &original).unwrap();
    let indices = [0];
    let context = AggregateEmissionContext::from_host(old.contract(), &indices, 1, None);
    let other = crate::aggregate_kernel::emit_aggregate_evaluation_in(
        &old,
        [&state].into_iter(),
        1,
        Some(&context),
        &contextual,
    )
    .unwrap();
    assert_eq!(out.to_data(), other.to_data());
    assert_eq!(
        *original.trace.lock().unwrap(),
        *contextual.trace.lock().unwrap()
    );
}
#[test]
fn emission_real_host_each_allocation_refusal_is_separate_and_rolls_back() {
    let host = Arc::new(Host::default());
    let (column, kernel) = probe(AggregateKernelPhase::Single, host.clone());
    let before = host.ledger.lock().unwrap().attempts;
    let data = column
        .emit_evaluation(&[2, 0, 2], 4, &Control::default())
        .unwrap_err();
    let count = host.ledger.lock().unwrap().attempts - before;
    assert!(count > 0);
    drop(data);
    drop(column);
    drop(kernel);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for at in 0..count {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let (column, kernel) = probe(AggregateKernelPhase::Single, host.clone());
            let live = host.ledger.lock().unwrap().bytes;
            arm_refusal(&host, at, cause.clone());
            assert_eq!(
                column
                    .emit_evaluation(&[2, 0, 2], 4, &Control::default())
                    .unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert!(!kernel.published.load(Ordering::SeqCst));
            assert_eq!(host.ledger.lock().unwrap().bytes, live);
            drop(column);
            drop(kernel);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn emission_every_observed_checkpoint_retains_all_seven_causes_without_tail() {
    let host = Arc::new(Host::default());
    let (column, kernel) = probe(AggregateKernelPhase::Single, host);
    let control = Control::default();
    let data = column.emit_evaluation(&[2, 0, 2], 4, &control).unwrap_err();
    let count = control.trace.lock().unwrap().len();
    drop(data);
    drop(column);
    drop(kernel);
    for at in 0..count {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let (column, kernel) = probe(AggregateKernelPhase::Single, host.clone());
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                column.emit_evaluation(&[2, 0, 2], 4, &control).unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert!(!kernel.published.load(Ordering::SeqCst));
            drop(column);
            drop(kernel);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}

#[test]
fn emission_missing_host_and_original_capacity_guard_precede_diagnostic_work() {
    let host = Arc::new(Host::default());
    let (column, prepared) = probe(AggregateKernelPhase::Single, host.clone());
    let attempts = host.ledger.lock().unwrap().attempts;
    assert_eq!(
        column
            .emit_evaluation(&[2, 0, 2], 2, &Control::default())
            .unwrap_err(),
        EvaluationFailure::Kernel(KernelFailure::ResourceExhausted)
    );
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
    assert!(!prepared.published.load(Ordering::SeqCst));
    let handle = PreparedAggregateHandle::from_typed(prepared.clone(), &Compile).unwrap();
    let mut storage = [std::mem::MaybeUninit::<u8>::uninit(); 0];
    let slot = handle
        .initialize_in(&mut storage, &Control::default())
        .unwrap();
    let slots = [slot];
    assert_eq!(
        handle
            .emit_evaluation(&slots, &[0], 1, &Control::default())
            .unwrap_err(),
        EvaluationFailure::Kernel(invalid(
            "emission diagnostic requires actual host allocator"
        ))
    );
    assert!(!prepared.published.load(Ordering::SeqCst));
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
}
