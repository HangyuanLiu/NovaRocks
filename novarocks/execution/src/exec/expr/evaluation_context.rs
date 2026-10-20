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
//! Per-call evaluation loans. Immutable arena facts never own runtime authority.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkFieldSchema};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::FunctionValueType;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;

// The loan stays on its caller's execution thread. Requiring Send/Sync here
// would preclude a later per-step thread stock lease without any consumer
// actually transferring this borrowed evaluator across threads.
pub(crate) trait ExprEvaluationControl {
    fn checkpoint(&self) -> ExecutionResult<()>;
    fn wait(&self, duration: std::time::Duration) -> ExecutionResult<()>;
}

/// Recursive evaluation preserves the producer's exact typed failure. Facts
/// alone do not grant a storage creator or Task publication capability.
pub trait ExprEvaluation {
    fn arena(&self) -> &ExprArena;
    fn eval(&self, id: ExprId, chunk: &Chunk) -> ExecutionResult<ArrayRef> {
        super::evaluate(self, id, chunk)
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> ExecutionResult<Chunk>;
    fn node(&self, id: ExprId) -> Option<&ExprNode> {
        self.arena().node(id)
    }
    fn data_type(&self, id: ExprId) -> Option<&DataType> {
        self.arena().data_type(id)
    }
    fn value_type(&self, id: ExprId) -> Option<&FunctionValueType> {
        self.arena().value_type(id)
    }
    fn field_schema(&self, id: ExprId) -> Option<&ChunkFieldSchema> {
        self.arena().field_schema(id)
    }
    fn decimal_overflow_policy(&self, id: ExprId) -> Option<DecimalOverflowPolicy> {
        self.arena().decimal_overflow_policy(id)
    }
    fn allow_throw_exception(&self) -> bool {
        self.arena().allow_throw_exception()
    }
    fn query_global_dict(
        &self,
        slot: SlotId,
    ) -> Option<&Arc<std::collections::HashMap<i32, Vec<u8>>>> {
        self.arena().query_global_dict(slot)
    }
    fn session_time_zone(&self) -> Option<&str> {
        self.arena().session_time_zone()
    }
    fn require_runtime_binding(&self) -> ExecutionResult<()> {
        self.arena().runtime_error_binding().map(|_| ())
    }
    fn check_runtime_error(&self) -> ExecutionResult<()> {
        self.arena().check_runtime_error()
    }
    fn wait_interruptibly(&self, duration: std::time::Duration) -> ExecutionResult<()> {
        self.arena().wait_interruptibly(duration)
    }
}
impl ExprEvaluation for ExprArena {
    fn arena(&self) -> &ExprArena {
        self
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> ExecutionResult<Chunk> {
        if chunk.chunk_schema().is_typed() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        Ok(chunk.slice(offset, length))
    }
}
impl ExprEvaluation for Arc<ExprArena> {
    fn arena(&self) -> &ExprArena {
        self.as_ref()
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> ExecutionResult<Chunk> {
        self.as_ref().slice_input(chunk, offset, length)
    }
}

/// One invocation borrows its owner's construction scope and control. The
/// loan is never attached to an arena, catalogue, input or global registry.
pub(crate) struct ExprEvaluationContext<'a> {
    arena: &'a ExprArena,
    owner: &'a dyn InputConstructionOwner,
    control: &'a dyn ExprEvaluationControl,
}
impl<'a> ExprEvaluationContext<'a> {
    pub fn new(
        arena: &'a ExprArena,
        owner: &'a dyn InputConstructionOwner,
        control: &'a dyn ExprEvaluationControl,
    ) -> Self {
        Self {
            arena,
            owner,
            control,
        }
    }
    pub fn eval(&self, id: ExprId, chunk: &Chunk) -> ExecutionResult<ArrayRef> {
        <Self as ExprEvaluation>::eval(self, id, chunk)
    }
    pub fn reborrow<'b>(&'b self, arena: &'b ExprArena) -> ExprEvaluationContext<'b> {
        ExprEvaluationContext::new(arena, self.owner, self.control)
    }
}
impl ExprEvaluation for ExprEvaluationContext<'_> {
    fn eval(&self, id: ExprId, chunk: &Chunk) -> ExecutionResult<ArrayRef> {
        self.control.checkpoint()?;
        let value = super::evaluate(self, id, chunk)?;
        self.control.checkpoint()?;
        Ok(value)
    }
    fn arena(&self) -> &ExprArena {
        self.arena
    }
    fn slice_input(&self, chunk: &Chunk, offset: usize, length: usize) -> ExecutionResult<Chunk> {
        self.control.checkpoint()?;
        let result = chunk.slice_with_creation_observed::<ExecutionFailure, _>(
            offset,
            length,
            self.owner.storage_lifetime(),
            |step| {
                self.control.checkpoint()?;
                self.owner.observe(0, &[], step)?;
                Ok(())
            },
        )?;
        self.control.checkpoint()?;
        Ok(result)
    }
    fn require_runtime_binding(&self) -> ExecutionResult<()> {
        self.control.checkpoint()
    }
    fn check_runtime_error(&self) -> ExecutionResult<()> {
        self.control.checkpoint()
    }
    fn wait_interruptibly(&self, duration: std::time::Duration) -> ExecutionResult<()> {
        self.control.wait(duration)
    }
}
