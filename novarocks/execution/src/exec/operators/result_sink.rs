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
//! Result sink for buffering query output rows.
//!
//! Responsibilities:
//! - Collects output chunks into a shared result handle consumed by fetch paths.
//! - Tracks completion and terminal error state for client-side result retrieval.
//!
//! Key exported interfaces:
//! - Types: `ResultSinkHandle`, `ResultSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::sync::{Arc, Mutex};

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{SinkKernel, SinkStep, Transform},
    step::NextStep,
    step_context::StepContext,
};

#[derive(Clone)]
/// Shared handle that stores buffered result chunks and terminal status for client fetch.
#[allow(
    dead_code,
    reason = "Result-sink types remain available for embedded execution integration tests."
)]
pub struct ResultSinkHandle {
    inner: Arc<Mutex<Vec<Chunk>>>,
}

#[allow(
    dead_code,
    reason = "Result-sink handle helpers remain available for embedded execution integration tests."
)]
impl ResultSinkHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn take_chunks(&self) -> Vec<Chunk> {
        let mut guard = self.inner.lock().expect("result_sink lock");
        guard.drain(..).collect()
    }
}

impl Default for ResultSinkHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for result sinks that append output chunks to query result buffers.
#[allow(
    dead_code,
    reason = "Result-sink factory remains available for embedded execution integration tests."
)]
pub struct ResultSinkFactory {
    name: String,
    handle: ResultSinkHandle,
}

#[allow(
    dead_code,
    reason = "Result-sink factory remains available for embedded execution integration tests."
)]
impl ResultSinkFactory {
    pub fn new(handle: ResultSinkHandle) -> Self {
        Self::new_with_plan_node_id(handle, None)
    }

    pub fn new_with_plan_node_id(handle: ResultSinkHandle, plan_node_id: Option<i32>) -> Self {
        let plan_node_id = match plan_node_id {
            Some(id) if id >= 0 => id,
            _ => -1,
        };
        let name = format!("RESULT_SINK (plan_node_id={plan_node_id})");
        Self { name, handle }
    }
}

impl OperatorFactory for ResultSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(ResultSinkOperator {
            name: self.name.clone(),
            handle: self.handle.clone(),
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

#[allow(
    dead_code,
    reason = "Result-sink operator remains available for embedded execution integration tests."
)]
struct ResultSinkOperator {
    name: String,
    handle: ResultSinkHandle,
}

impl OperatorLifecycle for ResultSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
}
impl InputEncodingPolicy for ResultSinkOperator {}
impl SinkKernel for ResultSinkOperator {
    fn consume(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.handle
            .inner
            .lock()
            .expect("result_sink lock")
            .push(chunk);
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
}
