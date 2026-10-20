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

//! A consuming shape cannot produce an output payload. Admission, accepted
//! preparation, continuation and finalization use the common receiver machine.

use super::{
    AdmissionNext, ControlStep, NextStep, PreparedInput, RawInput, StepContext, StepResult,
    TransformKernel, TransformStart,
};
use crate::runtime::fragment::ExecutionResult;

#[derive(Debug)]
pub struct SinkStep {
    pub next: NextStep,
    pub committed_rows: u64,
}
impl SinkStep {
    pub fn new(next: NextStep, committed_rows: u64) -> Self {
        Self {
            next,
            committed_rows,
        }
    }
    fn into_step(self) -> StepResult {
        StepResult::new(None, self.next, self.committed_rows)
    }
}

pub trait SinkKernel: Send {
    fn poll_control(&mut self, _ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        Ok(ControlStep::Active)
    }
    fn control_observable(
        &self,
    ) -> Option<std::sync::Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        None
    }
    fn start(&self) -> TransformStart {
        TransformStart::NeedInput
    }
    fn admission(
        &self,
        _ctx: &mut StepContext<'_>,
        _input: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        Ok(None)
    }
    fn consume(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep>;
    fn poll(&mut self, _ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        Ok(SinkStep::new(NextStep::NeedInput, 0))
    }
    fn finish(&mut self, _ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
    #[cfg(test)]
    fn poll_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<SinkStep> {
        super::super::step_context::with_test_step(state, |ctx| self.poll(ctx))
    }
    #[cfg(test)]
    fn consume_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
        chunk: crate::exec::chunk::Chunk,
    ) -> ExecutionResult<SinkStep> {
        let control = state.error_state();
        let mut turn =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut turn);
        self.consume(&mut ctx, PreparedInput::for_test(chunk))
    }
    #[cfg(test)]
    fn finish_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<SinkStep> {
        let control = state.error_state();
        let mut turn =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut turn);
        SinkKernel::finish(self, &mut ctx)
    }
}
impl<K: SinkKernel> TransformKernel for K {
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        SinkKernel::poll_control(self, ctx)
    }
    fn control_observable(
        &self,
    ) -> Option<std::sync::Arc<crate::exec::pipeline::schedule::observer::Observable>> {
        SinkKernel::control_observable(self)
    }
    fn start(&self) -> TransformStart {
        SinkKernel::start(self)
    }
    fn admission(
        &self,
        ctx: &mut StepContext<'_>,
        input: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        SinkKernel::admission(self, ctx, input)
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.consume(ctx, input).map(SinkStep::into_step)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.poll(ctx).map(SinkStep::into_step)
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        SinkKernel::finish(self, ctx).map(SinkStep::into_step)
    }
}
