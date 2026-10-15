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

//! Common transformation state machine over the low-level step contract.
//!
//! The adapter owns accepted input and preparation, not an upstream edge. A
//! custom credited/asynchronous operator may implement StepOperator directly.

use super::{
    input::{AcceptedInput, InputPreparationFailure, PreparedInput, RawInput},
    step::{AdmissionNext, InputResult, NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::fragment::ExecutionResult;

pub trait StepOperator: Send {
    fn try_push(&mut self, ctx: &mut StepContext<'_>, input: RawInput) -> InputResult;
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult>;
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult>;
}

/// Preparation state, including any scratch/plan, belongs to this receiver.
/// An operation that waits returns its original accepted responsibility.
pub enum PreparationStep {
    Ready(PreparedInput),
    Pending {
        input: AcceptedInput,
        next: NextStep,
    },
}

pub trait InputPreparation: Send {
    /// None means the exact raw input may be accepted. This does not authorize
    /// every later allocation performed by preparation or calculation.
    fn admission(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>>;
    fn accept(&mut self, input: RawInput) -> AcceptedInput;
    fn prepare(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: AcceptedInput,
    ) -> Result<PreparationStep, InputPreparationFailure>;
}

pub trait TransformKernel: Send {
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult>;
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult>;
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult>;
}

/// One receiver slot. Multi-prefix kernels retain their own row continuation;
/// they signal NeedInput only after that responsibility has completed.
pub struct Transform<K, P> {
    kernel: K,
    preparation: P,
    accepted: Option<AcceptedInput>,
    needs_input: bool,
    finishing: bool,
    finish_started: bool,
}
impl<K: TransformKernel, P: InputPreparation> Transform<K, P> {
    pub fn new(kernel: K, preparation: P) -> Self {
        Self {
            kernel,
            preparation,
            accepted: None,
            needs_input: true,
            finishing: false,
            finish_started: false,
        }
    }
    fn record(&mut self, mut step: StepResult) -> StepResult {
        if self.finishing && matches!(step.next, NextStep::NeedInput) {
            step.next = NextStep::Runnable;
        }
        self.needs_input = !self.finishing && matches!(step.next, NextStep::NeedInput);
        step
    }
}
impl<K: TransformKernel, P: InputPreparation> StepOperator for Transform<K, P> {
    fn try_push(&mut self, ctx: &mut StepContext<'_>, input: RawInput) -> InputResult {
        if let Err(failure) = ctx.checkpoint() {
            return InputResult::Rejected { input, failure };
        }
        if !self.needs_input || self.finishing {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        match self.preparation.admission(ctx, &input) {
            Err(failure) => InputResult::Rejected { input, failure },
            Ok(Some(next)) => InputResult::Deferred { input, next },
            Ok(None) => {
                self.accepted = Some(self.preparation.accept(input));
                self.needs_input = false;
                InputResult::Accepted(Ok(StepResult::new(None, NextStep::Runnable, 0)))
            }
        }
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        ctx.checkpoint()?;
        let step = if let Some(input) = self.accepted.take() {
            match self.preparation.prepare(ctx, input) {
                Ok(PreparationStep::Ready(input)) => self.kernel.apply(ctx, input)?,
                Ok(PreparationStep::Pending { input, next }) => {
                    self.accepted = Some(input);
                    StepResult::new(None, next, 0)
                }
                Err(InputPreparationFailure { input, failure }) => {
                    self.accepted = Some(input);
                    return Err(failure);
                }
            }
        } else if self.finishing && !self.finish_started {
            self.finish_started = true;
            self.kernel.finish(ctx)?
        } else {
            self.kernel.drive(ctx)?
        };
        Ok(self.record(step))
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.finishing = true;
        self.needs_input = false;
        self.drive(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema},
        exec::pipeline::step_context::TurnWork,
        runtime::runtime_state::RuntimeErrorState,
    };
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field},
    };
    use novarocks_types::SlotId;
    use std::{
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn chunk() -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        Chunk::try_new_with_columns(
            schema,
            vec![Arc::new(Int32Array::from(vec![7, 11])) as ArrayRef],
        )
        .unwrap()
    }
    #[derive(Default)]
    struct Counts {
        accepted: AtomicUsize,
        prepared: AtomicUsize,
        applied: AtomicUsize,
        finished: AtomicUsize,
    }
    struct Prepare {
        counts: Arc<Counts>,
        refuse: bool,
        pause: bool,
        fail: bool,
    }
    impl InputPreparation for Prepare {
        fn admission(
            &mut self,
            _: &mut StepContext<'_>,
            _: &RawInput,
        ) -> ExecutionResult<Option<AdmissionNext>> {
            Ok(self.refuse.then_some(AdmissionNext::Yield))
        }
        fn accept(&mut self, input: RawInput) -> AcceptedInput {
            self.counts.accepted.fetch_add(1, Ordering::SeqCst);
            input.accept(None)
        }
        fn prepare(
            &mut self,
            _: &mut StepContext<'_>,
            input: AcceptedInput,
        ) -> Result<PreparationStep, InputPreparationFailure> {
            if std::mem::take(&mut self.pause) {
                return Ok(PreparationStep::Pending {
                    input,
                    next: NextStep::Yield,
                });
            }
            self.counts.prepared.fetch_add(1, Ordering::SeqCst);
            input
                .prepare(|chunk| {
                    if self.fail {
                        Err(novarocks_functions::KernelFailure::ResourceExhausted.into())
                    } else {
                        Ok(chunk.clone())
                    }
                })
                .map(PreparationStep::Ready)
        }
    }
    struct Kernel {
        counts: Arc<Counts>,
    }
    impl TransformKernel for Kernel {
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.counts.applied.fetch_add(1, Ordering::SeqCst);
            let (chunk, permit) = input.into_parts();
            assert!(permit.is_none());
            Ok(StepResult::new(Some(chunk), NextStep::NeedInput, 2))
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.counts.finished.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(None, NextStep::Runnable, 0))
        }
    }
    fn transform(
        counts: &Arc<Counts>,
        refuse: bool,
        pause: bool,
        fail: bool,
    ) -> Transform<Kernel, Prepare> {
        Transform::new(
            Kernel {
                counts: counts.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse,
                pause,
                fail,
            },
        )
    }
    #[test]
    fn refusal_returns_the_original_without_preparation_or_acceptance() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, true, false, false);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut ctx = StepContext::new(&control, &mut work);
        let original = chunk();
        let address = original.columns()[0].as_ref() as *const _;
        let mut input = RawInput::new(original, None);
        for _ in 0..3 {
            let InputResult::Deferred {
                input: returned, ..
            } = operator.try_push(&mut ctx, input)
            else {
                panic!("input was accepted")
            };
            assert!(std::ptr::addr_eq(
                returned.original().columns()[0].as_ref() as *const _,
                address
            ));
            input = returned;
        }
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn preparation_yield_resumes_one_accepted_input_and_finishes_once() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, false, true, false);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut ctx = StepContext::new(&control, &mut work);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(Ok(_))
        ));
        assert!(matches!(
            operator.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Deferred {
                next: AdmissionNext::ResumeExisting,
                ..
            }
        ));
        let output = operator.finish(&mut ctx).unwrap();
        assert!(matches!(output.next, NextStep::Runnable));
        let values = output
            .output
            .unwrap()
            .column_by_slot_id(SlotId::new(1))
            .unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[7, 11]
        );
        operator.finish(&mut ctx).unwrap();
        operator.finish(&mut ctx).unwrap();
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(counts.applied.load(Ordering::SeqCst), 1);
        assert_eq!(counts.finished.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn acceptance_and_preparation_keep_one_accounting_responsibility() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("receiver");
        let raw = RawInput::new(chunk(), None);
        assert_eq!(tracker.current(), 0);
        let accepted = raw.accept(Some(&tracker));
        let held = tracker.current();
        assert!(held > 0);
        let prepared = match accepted.prepare(|original| Ok(original.clone())) {
            Ok(prepared) => prepared,
            Err(_) => panic!("preparation failed"),
        };
        assert_eq!(tracker.current(), held);
        drop(prepared);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn preparation_failure_keeps_accepted_responsibility_in_the_receiver() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, false, false, true);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut ctx = StepContext::new(&control, &mut work);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(_)
        ));
        assert!(operator.drive(&mut ctx).is_err());
        assert!(operator.accepted.is_some());
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.applied.load(Ordering::SeqCst), 0);
    }
}
