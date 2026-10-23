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
//! UNION ALL source operator.
//!
//! Responsibilities:
//! - Pulls buffered chunks from shared UNION ALL state and emits merged output stream.
//! - Terminates when all sink branches are finished and queues are drained.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceRead, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};

use super::union_all_shared::UnionAllSharedState;

/// Factory for UNION ALL source operators that emit merged branch chunk streams.
pub struct UnionAllSourceFactory {
    name: String,
    state: UnionAllSharedState,
}

impl UnionAllSourceFactory {
    pub(crate) fn new(state: UnionAllSharedState, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("UnionAllSource (id={node_id})")
        } else {
            "UnionAllSource".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for UnionAllSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // UNION ALL fan-in owns a single-consumer shared queue: only the
        // pipeline-local driver 0 drains it.
        Box::new(Source::new(UnionAllSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            active: driver_id == 0,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct UnionAllSourceOperator {
    name: String,
    state: UnionAllSharedState,
    active: bool,
}

impl OperatorLifecycle for UnionAllSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn cancel(&mut self) {
        if self.active {
            self.state.close();
        }
    }
    fn close(&mut self) -> ExecutionResult<()> {
        if self.active {
            self.state.close();
        }
        Ok(())
    }
}

impl SourceKernel for UnionAllSourceOperator {
    fn start(&self) -> SourceStart {
        if self.active {
            SourceStart::Ready
        } else {
            SourceStart::Finished
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        let observable = self.state.observable();
        let observed_generation = observable.generation();
        let (output, next) = match self.state.next_chunk()? {
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
