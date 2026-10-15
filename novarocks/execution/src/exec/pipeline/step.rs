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

//! Data progress is explicit and independent of Task failure or physical exit.

use super::{input::RawInput, operator::DriverBlockDeadline, schedule::observer::Observable};
use crate::{
    exec::chunk::Chunk,
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReason {
    Input,
    Output,
    Dependency,
    Finish,
}

/// The worker samples generation before inspecting the condition, then
/// subscribes/rechecks it before parking. Scheduler callbacks never call an op.
#[derive(Clone)]
pub struct WaitFact {
    pub reason: WaitReason,
    pub observable: Arc<Observable>,
    pub observed_generation: u64,
    pub deadline: Option<DriverBlockDeadline>,
}

impl std::fmt::Debug for WaitFact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitFact")
            .field("reason", &self.reason)
            .field("observed_generation", &self.observed_generation)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub enum AdmissionNext {
    Wait(WaitFact),
    Yield,
    ResumeExisting,
}

pub enum InputResult {
    Deferred {
        input: RawInput,
        next: AdmissionNext,
    },
    Rejected {
        input: RawInput,
        failure: ExecutionFailure,
    },
    Accepted(ExecutionResult<StepResult>),
}

#[derive(Clone, Debug)]
pub enum NextStep {
    NeedInput,
    Runnable,
    Yield,
    Wait(WaitFact),
    Finished,
}

/// The moved output is the only output delivered by this call. Further rows
/// remain in the operator continuation and require another output-ready step.
pub struct StepResult {
    pub output: Option<Chunk>,
    pub next: NextStep,
    pub committed_rows: u64,
}
impl StepResult {
    pub fn new(output: Option<Chunk>, next: NextStep, committed_rows: u64) -> Self {
        Self {
            output,
            next,
            committed_rows,
        }
    }
}
