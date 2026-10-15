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

//! Linear input custody. An edge never owns receiver materialization state.

use crate::runtime::mem_tracker::MemTracker;
use crate::{
    exec::chunk::Chunk,
    runtime::fragment::{ExecutionFailure, ExecutionResult, io::RootInputPermit},
};
use std::{fmt, sync::Arc};

/// Field order is intentional: input storage exits before its original permit.
pub struct RawInput {
    chunk: Chunk,
    permit: Option<RootInputPermit>,
}
impl RawInput {
    pub(super) fn new(chunk: Chunk, permit: Option<RootInputPermit>) -> Self {
        Self { chunk, permit }
    }
    pub fn rows(&self) -> usize {
        self.chunk.len()
    }
    pub(super) fn original(&self) -> &Chunk {
        &self.chunk
    }
    pub(super) fn permit(&self) -> Option<&RootInputPermit> {
        self.permit.as_ref()
    }
    /// Only the receiver's acceptance path can transfer this responsibility.
    pub(super) fn accept(mut self, tracker: Option<&Arc<MemTracker>>) -> AcceptedInput {
        if self.permit.is_none() {
            if let Some(tracker) = tracker {
                self.chunk.transfer_to(tracker);
            }
        }
        AcceptedInput {
            raw: self,
            tracker: tracker.cloned(),
        }
    }
}
impl fmt::Debug for RawInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawInput")
            .field("rows", &self.rows())
            .field("precovered", &self.permit.is_some())
            .finish()
    }
}

pub struct AcceptedInput {
    raw: RawInput,
    tracker: Option<Arc<MemTracker>>,
}
impl AcceptedInput {
    /// Preparation runs under the receiver's binding. On failure, the accepted
    /// input is still returned to that receiver's cleanup, never to the edge.
    pub(super) fn prepare(
        self,
        prepare: impl FnOnce(&Chunk) -> ExecutionResult<Chunk>,
    ) -> Result<PreparedInput, InputPreparationFailure> {
        match prepare(&self.raw.chunk) {
            Ok(mut chunk) => {
                if let Some(tracker) = self.tracker.as_ref() {
                    chunk.transfer_to(tracker);
                }
                let RawInput {
                    chunk: original,
                    permit,
                } = self.raw;
                drop(original);
                Ok(PreparedInput { chunk, permit })
            }
            Err(failure) => Err(InputPreparationFailure {
                input: self,
                failure,
            }),
        }
    }
    /// An original-input consumer keeps its own finite carrier validation and
    /// preparation policy. This does not add a driver hydration pass.
    pub(super) fn prepare_original(self) -> PreparedInput {
        let RawInput { chunk, permit } = self.raw;
        PreparedInput { chunk, permit }
    }
}

pub struct InputPreparationFailure {
    pub input: AcceptedInput,
    pub failure: ExecutionFailure,
}

pub struct PreparedInput {
    chunk: Chunk,
    permit: Option<RootInputPermit>,
}
impl PreparedInput {
    pub fn chunk(&self) -> &Chunk {
        &self.chunk
    }
    pub(crate) fn into_parts(self) -> (Chunk, Option<RootInputPermit>) {
        (self.chunk, self.permit)
    }
}
