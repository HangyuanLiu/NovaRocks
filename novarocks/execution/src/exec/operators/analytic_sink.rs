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
//! Sink side of analytic-window execution.
//!
//! Responsibilities:
//! - Consumes input chunks and appends partition/order-aware data into shared analytic state.
//! - Marks sink completion so analytic source operators can emit finalized window outputs.
//!
//! Key exported interfaces:
//! - Types: `AnalyticSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use crate::exec::operators::analytic_shared::{AnalyticInputMemory, AnalyticSharedState};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{SinkKernel, SinkStep, Transform},
    step::NextStep,
    step_context::StepContext,
};
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;

/// Factory that builds analytic sink operators which materialize partition/order inputs into shared analytic state.
pub struct AnalyticSinkFactory {
    name: String,
    state: AnalyticSharedState,
}

impl AnalyticSinkFactory {
    pub(crate) fn new(state: AnalyticSharedState) -> Self {
        Self {
            name: "AnalyticSink".to_string(),
            state,
        }
    }
}

impl OperatorFactory for AnalyticSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(AnalyticSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            memory: None,
            construction: None,
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct AnalyticSinkOperator {
    name: String,
    state: AnalyticSharedState,
    memory: Option<AnalyticInputMemory>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}

impl OperatorLifecycle for AnalyticSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.memory = Some(self.state.input_memory(state));
        Ok(())
    }
}
impl AnalyticSinkOperator {
    fn memory(&self) -> ExecutionResult<&AnalyticInputMemory> {
        self.memory.as_ref().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "analytic input runtime binding is missing",
                ),
            )
            .into()
        })
    }
}
impl InputEncodingPolicy for AnalyticSinkOperator {}
impl SinkKernel for AnalyticSinkOperator {
    fn consume(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.state.push_input(self.memory()?, chunk)?;
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        let factory = self
            .construction
            .as_deref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        self.state.finish_created(self.memory()?, factory, ctx)?;
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
