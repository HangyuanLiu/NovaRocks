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

//! Short-lived driver capabilities. Operator-private construction, profiles and
//! containers are installed on the operator, never indexed through this view.

use crate::runtime::{
    fragment::{ExecutionFailure, ExecutionFailureContext, ExecutionResult, PipelineOperation},
    runtime_state::RuntimeErrorState,
};
use novarocks_functions::KernelFailure;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;
use std::num::NonZeroU64;

pub(crate) const DEFAULT_TURN_WORK_UNITS: u32 = 64;

/// One driver turn's shared work allowance. Nested calls cannot refill it.
pub struct TurnWork {
    generation: NonZeroU64,
    budget: ConnectorPollBudget,
}
impl TurnWork {
    pub(crate) fn new(generation: NonZeroU64, units: u32) -> Self {
        let budget = ConnectorPollBudget::new();
        budget.refill(u64::from(units));
        Self { generation, budget }
    }
    pub(crate) fn poll_budget(&self) -> ConnectorPollBudget {
        self.budget.clone()
    }
    pub(crate) fn begin_turn(&mut self, units: u32) -> ExecutionResult<()> {
        self.generation = self
            .generation
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
                    "driver turn generation exhausted",
                ))
            })?;
        self.budget.refill(u64::from(units));
        Ok(())
    }

    pub(crate) fn yield_count(&self) -> u64 {
        self.budget.exhaustions()
    }

    pub fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    pub fn remaining(&self) -> u32 {
        u32::try_from(self.budget.remaining()).expect("driver work allowance has u32 width")
    }
    /// A refusal consumes no work. The caller yields, retaining its cursor.
    pub fn try_consume(&mut self, units: u32) -> bool {
        self.budget.try_take(u64::from(units))
    }
}

pub struct StepKernelControl<'a> {
    control: &'a RuntimeErrorState,
}
impl novarocks_functions::KernelEvaluationControl for StepKernelControl<'_> {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.control.is_stopped() {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, duration: std::time::Duration) -> Result<(), KernelFailure> {
        if self.control.wait_until_stopped(duration) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
}

pub struct StepContext<'a> {
    control: &'a RuntimeErrorState,
    work: &'a mut TurnWork,
    site: Option<ExecutionFailureContext>,
}
impl<'a> StepContext<'a> {
    pub(crate) fn new(control: &'a RuntimeErrorState, work: &'a mut TurnWork) -> Self {
        Self {
            control,
            work,
            site: None,
        }
    }
    /// Current-call metadata is a short reborrow, never an indexed capability
    /// table or a second allowance. Nested work keeps this exact turn's budget.
    pub(crate) fn operator_step(
        &mut self,
        operator_ordinal: usize,
        operation: PipelineOperation,
    ) -> StepContext<'_> {
        StepContext {
            control: self.control,
            work: &mut *self.work,
            site: Some(ExecutionFailureContext {
                operator_ordinal,
                operation,
            }),
        }
    }
    /// Preserve the semantic phase while a shape performs work through drive.
    /// The ordinal, allowance and reporting owner remain the same short borrow.
    pub(crate) fn perform<T>(
        &mut self,
        operation: PipelineOperation,
        action: impl FnOnce(&mut StepContext<'_>) -> ExecutionResult<T>,
    ) -> ExecutionResult<T> {
        let mut scoped = StepContext {
            control: self.control,
            work: &mut *self.work,
            site: self
                .site
                .map(|site| ExecutionFailureContext { operation, ..site }),
        };
        action(&mut scoped).map_err(|failure| scoped.locate(failure))
    }
    pub(crate) fn locate(&self, failure: ExecutionFailure) -> ExecutionFailure {
        match self.site {
            Some(site) => failure.at_operator(site.operator_ordinal, site.operation),
            None => failure,
        }
    }
    /// Submit a known cause before synchronous or asynchronous cleanup starts.
    /// The bound control/reporting owner decides acceptance; the kernel only
    /// submits the cause.
    pub fn report_failure(&self, failure: ExecutionFailure) {
        self.control.set_error(self.locate(failure));
    }
    pub(crate) fn report_cleanup(&self, failure: &ExecutionFailure) {
        self.control.report_cleanup(failure);
    }
    pub(crate) fn report_unresolved_exit(&self, failure: &ExecutionFailure) {
        self.control.report_unresolved_exit(failure);
    }
    pub(crate) fn observe_exit(
        &self,
        observation: super::operator::ExitObservation,
    ) -> super::operator::ExitProgress {
        observation.report_to(self.control)
    }
    pub(crate) fn failure_port(&self) -> &RuntimeErrorState {
        self.control
    }
    pub fn kernel_control(&self) -> StepKernelControl<'_> {
        StepKernelControl {
            control: self.control,
        }
    }
    pub fn work(&mut self) -> &mut TurnWork {
        self.work
    }
    /// Observing another owner's stop does not invent a user cancellation.
    pub fn checkpoint(&self) -> ExecutionResult<()> {
        match self.control.error() {
            Some(original) => Err(original),
            None => Ok(()),
        }
    }
}

/// Test callers get a real turn and the complete native step result. Quota and
/// cross-step custody tests retain their own explicit TurnWork instead.
#[cfg(test)]
pub(super) fn with_test_step<T>(
    state: &crate::runtime::runtime_state::RuntimeState,
    action: impl FnOnce(&mut StepContext<'_>) -> T,
) -> T {
    let control = state.error_state();
    let mut turn = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
    action(&mut StepContext::new(&control, &mut turn))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failure_location_and_work_allowance_share_the_same_short_step() {
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::new(9).unwrap(), 3);
        let mut context = StepContext::new(&control, &mut work);
        {
            let mut step = context.operator_step(7, PipelineOperation::Pull);
            assert!(step.work().try_consume(2));
            step.report_failure(KernelFailure::ResourceExhausted.into());
        }
        assert_eq!(context.work().remaining(), 1);
        assert_eq!(context.work().generation().get(), 9);
        let error = control.error().unwrap();
        assert_eq!(
            context.checkpoint().unwrap_err(),
            error,
            "an observed original Resource failure does not become user cancellation"
        );
        assert_eq!(
            error.context(),
            Some(ExecutionFailureContext {
                operator_ordinal: 7,
                operation: PipelineOperation::Pull,
            })
        );
        assert!(matches!(
            error.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Kernel(
                KernelFailure::ResourceExhausted
            )
        ));
    }

    #[test]
    fn repeated_and_nested_calls_share_one_turn_allowance() {
        let mut work = TurnWork::new(NonZeroU64::new(9).unwrap(), 3);
        assert!(work.try_consume(2));
        assert!(!work.try_consume(2));
        assert_eq!(work.remaining(), 1);
        assert!(work.try_consume(1));
        assert!(!work.try_consume(1));
        assert_eq!(work.generation().get(), 9);
    }
}
