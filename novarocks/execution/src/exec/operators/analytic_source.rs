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
//! Source side of analytic-window execution.
//!
//! Responsibilities:
//! - Pulls finalized windowed rows from shared analytic state and emits them downstream.
//! - Respects sink completion and shared error propagation before producing output chunks.
//!
//! Key exported interfaces:
//! - Types: `AnalyticSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use crate::exec::operators::analytic_shared::AnalyticSharedState;
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceRead};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};

/// Factory that builds analytic source operators which emit finalized window results from shared analytic state.
pub struct AnalyticSourceFactory {
    name: String,
    state: AnalyticSharedState,
}

impl AnalyticSourceFactory {
    pub(crate) fn new(state: AnalyticSharedState) -> Self {
        Self {
            name: "AnalyticSource".to_string(),
            state,
        }
    }
}

impl OperatorFactory for AnalyticSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(AnalyticSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AnalyticSourceOperator {
    name: String,
    state: AnalyticSharedState,
}

impl OperatorLifecycle for AnalyticSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }
}

impl SourceKernel for AnalyticSourceOperator {
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        let observable = self.state.observable();
        let observed_generation = observable.generation();
        let (output, next) = match self.state.next_output() {
            SourceRead::Chunk(chunk) => (Some(chunk), NextStep::Runnable),
            SourceRead::Finished => (None, NextStep::Finished),
            SourceRead::Pending => (
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable,
                    observed_generation,
                    deadline: None,
                }),
            ),
        };
        Ok(StepResult::new(output, next, 0))
    }
}
